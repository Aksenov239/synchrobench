/*
 * Copyright (c) 2009 Stanford University, unless otherwise specified.
 * All rights reserved.
 *
 * This software was developed by the Pervasive Parallelism Laboratory of
 * Stanford University, California, USA.
 *
 * Permission to use, copy, modify, and distribute this software in source
 * or binary form for any purpose with or without fee is hereby granted,
 * provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *    3. Neither the name of Stanford University nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/* SnapTree - (c) 2009 Stanford University - PPL */

// SnapTreeMap

package trees.lockbased;

import contention.abstractions.AbstractCompositionalIntSet;
import contention.abstractions.CompositionalMap;

import java.util.*;

/**
 * A concurrent relaxed balance AVL tree, based on the algorithm of Bronson,
 * Casper, Chafi, and Olukotun, "A Practical Concurrent Binary Search Tree"
 * published in PPoPP'10. To simplify the locking protocols rebalancing work is
 * performed in pieces, and some removed keys are be retained as routing nodes
 * in the tree.
 * 
 * <p>
 * Compared to {@link trees.lockbased.stanfordutils.SnapTreeMap}, this implementation does not provide any
 * structural sharing with copy on write. As a result, it must support iteration
 * on the mutating structure, so nodes track both the number of shrinks (which
 * invalidate queries and traverals) and grows (which invalidate traversals).
 *
 * @author Nathan Bronson
 */
public class LockBasedStanfordTreeSet extends AbstractCompositionalIntSet {
	// public class OptTreeMap<K,V> extends AbstractMap<K,V> implements
	// ConcurrentMap<K,V> {

	/**
	 * This is a special value that indicates the presence of a null value, to
	 * differentiate from the absence of a value.
	 */
	static final Object SpecialNull = new Object();

	/**
	 * This is a special value that indicates that an optimistic read failed.
	 */
	static final Object SpecialRetry = new Object();

	/** The number of spins before yielding. */
	static final int SpinCount = Integer.parseInt(System.getProperty("spin",
			"100"));

	/** The number of yields before blocking. */
	static final int YieldCount = Integer.parseInt(System.getProperty("yield",
			"0"));

	static final int OVLBitsBeforeOverflow = Integer.parseInt(System
			.getProperty("shrinkbits", "8"));

	// we encode directions as characters
	static final char Left = 'L';
	static final char Right = 'R';

	// return type for extreme searches
	static final int ReturnKey = 0;
	static final int ReturnEntry = 1;
	static final int ReturnNode = 2;

	/**
	 * An <tt>OVL</tt> is a version number and lock used for optimistic
	 * concurrent control of some program invariant. If {@link #isChanging} then
	 * the protected invariant is changing. If two reads of an OVL are performed
	 * that both see the same non-changing value, the reader may conclude that
	 * no changes to the protected invariant occurred between the two reads. The
	 * special value UnlinkedOVL is not changing, and is guaranteed to not
	 * result from a normal sequence of beginChange and endChange operations.
	 * <p>
	 * For convenience <tt>endChange(ovl) == endChange(beginChange(ovl))</tt>.
	 */
	private static final long UnlinkedOVL = 1L;
	private static final long OVLGrowLockMask = 2L;
	private static final long OVLShrinkLockMask = 4L;
	private static final int OVLGrowCountShift = 3;
	private static final long OVLGrowCountMask = ((1L << OVLBitsBeforeOverflow) - 1) << OVLGrowCountShift;
	private static final long OVLShrinkCountShift = OVLGrowCountShift
			+ OVLBitsBeforeOverflow;

	private static long beginGrow(final long ovl) {
		assert (!isChangingOrUnlinked(ovl));
		return ovl | OVLGrowLockMask;
	}

	private static long endGrow(final long ovl) {
		assert (!isChangingOrUnlinked(ovl));

		// Overflows will just go into the shrink lock count, which is fine.
		return ovl + (1L << OVLGrowCountShift);
	}

	private static long beginShrink(final long ovl) {
		assert (!isChangingOrUnlinked(ovl));
		return ovl | OVLShrinkLockMask;
	}

	private static long endShrink(final long ovl) {
		assert (!isChangingOrUnlinked(ovl));

		// increment overflows directly
		return ovl + (1L << OVLShrinkCountShift);
	}

	private static boolean isChanging(final long ovl) {
		return (ovl & (OVLShrinkLockMask | OVLGrowLockMask)) != 0;
	}

	private static boolean isUnlinked(final long ovl) {
		return ovl == UnlinkedOVL;
	}

	private static boolean isShrinkingOrUnlinked(final long ovl) {
		return (ovl & (OVLShrinkLockMask | UnlinkedOVL)) != 0;
	}

	private static boolean isChangingOrUnlinked(final long ovl) {
		return (ovl & (OVLShrinkLockMask | OVLGrowLockMask | UnlinkedOVL)) != 0;
	}

	private static boolean hasShrunkOrUnlinked(final long orig,
			final long current) {
		return ((orig ^ current) & ~(OVLGrowLockMask | OVLGrowCountMask)) != 0;
	}

	private static boolean hasChangedOrUnlinked(final long orig,
			final long current) {
		return orig != current;
	}

	private static class Node {
		final int key;
		volatile int height;

		/**
		 * null means this node is conceptually not present in the map.
		 * SpecialNull means the value is null.
		 */
		volatile Object vOpt;
		volatile Node parent;
		volatile long changeOVL;
		volatile Node left;
		volatile Node right;

		Node(final int key, final int height, final Object vOpt,
				final Node parent, final long changeOVL,
				final Node left, final Node right) {
			this.key = key;
			this.height = height;
			this.vOpt = vOpt;
			this.parent = parent;
			this.changeOVL = changeOVL;
			this.left = left;
			this.right = right;
		}

		Node child(char dir) {
			return dir == Left ? left : right;
		}

		Node childSibling(char dir) {
			return dir == Left ? right : left;
		}

		void setChild(char dir, Node node) {
			if (dir == Left) {
				left = node;
			} else {
				right = node;
			}
		}

		// ////// per-node blocking

		private void waitUntilChangeCompleted(final long ovl) {
			if (!isChanging(ovl)) {
				return;
			}

			for (int tries = 0; tries < SpinCount; ++tries) {
				if (changeOVL != ovl) {
					return;
				}
			}

			for (int tries = 0; tries < YieldCount; ++tries) {
				Thread.yield();
				if (changeOVL != ovl) {
					return;
				}
			}

			// spin and yield failed, use the nuclear option
			synchronized (this) {
				// we can't have gotten the lock unless the shrink was over
			}
			assert (changeOVL != ovl);
		}

		int validatedHeight() {
			final int hL = left == null ? 0 : left.validatedHeight();
			final int hR = right == null ? 0 : right.validatedHeight();
			assert (Math.abs(hL - hR) <= 1);
			final int h = 1 + Math.max(hL, hR);
			assert (h == height);
			return height;
		}
	}

	// ////// node access functions

	private static int height(final Node node) {
		return node == null ? 0 : node.height;
	}

	private static Object encodeNull(final Object v) {
		return v == null ? SpecialNull : v;
	}

	// ////////////// state

	private final Node rootHolder = new Node(Integer.MIN_VALUE, 1, null, null,
			0L, null, null);
	//private

	// ////////////// public interface

	public LockBasedStanfordTreeSet() {
	}

	public int size(Node node) {
		if (node == null)
			return 0;
		return (node.vOpt != null ? 1 : 0) + size(node.left) + size(node.right);
	}

	@Override
	public int size() {
		return size(rootHolder);
	}

	@Override
	public boolean isEmpty() {
		// removed-but-not-unlinked nodes cannot be leaves, so if the tree is
		// truly empty then the root holder has no right child
		return rootHolder.right == null;
	}

	@Override
	public void clear() {
		synchronized (rootHolder) {
			rootHolder.height = 1;
			rootHolder.right = null;
		}
	}

	// ////// search

	@Override
	public boolean containsInt(final int key) {
		return getImpl(key) == SpecialNull;
	}

	void finishCount1(int nodesTraversed) {
		Vars vars = counts.get();
		vars.getCount++;
		vars.nodesTraversed += nodesTraversed;
	}

	void finishCount2(int nodesTraversed) {
		Vars vars = counts.get();
		vars.nodesTraversed += nodesTraversed;
	}

	/** Returns either a value or SpecialNull, if present, or null, if absent. */
	private Object getImpl(final int key) {
		int nodesTraversed = 0;

		while (true) {
			final Node right = rootHolder.right;
			if (TRAVERSAL_COUNT) {
				nodesTraversed++;
			}
			if (right == null) {
				if (TRAVERSAL_COUNT) {
					finishCount1(nodesTraversed);
				}
				return null;
			} else {
				final int rightCmp = Integer.compare(key, right.key);
				if (rightCmp == 0) {
					// who cares how we got here
					if (TRAVERSAL_COUNT) {
						finishCount1(nodesTraversed);
					}
					return right.vOpt;
				}

				final long ovl = right.changeOVL;
				if (isShrinkingOrUnlinked(ovl)) {
					right.waitUntilChangeCompleted(ovl);
					// RETRY
				} else if (right == rootHolder.right) {
					// the reread of .right is the one protected by our read of
					// ovl
					final Object vo = attemptGet(key, right, (rightCmp < 0 ? Left
							: Right), ovl);
					if (vo != SpecialRetry) {
						if (TRAVERSAL_COUNT) {
							finishCount1(nodesTraversed);
						}
						return vo;
					}
					// else RETRY
				}
			}
		}
	}

	private Object attemptGet(final int k,
			final Node node, final char dirToC, final long nodeOVL) {
		int nodesTraversed = 0;
		while (true) {
			final Node child = node.child(dirToC);
			if (TRAVERSAL_COUNT) {
				nodesTraversed++;
			}

			if (child == null) {
				if (hasShrunkOrUnlinked(nodeOVL, node.changeOVL)) {
					if (TRAVERSAL_COUNT) {
						finishCount2(nodesTraversed);
					}
					return SpecialRetry;
				}

				// Note is not present. Read of node.child occurred while
				// parent.child was valid, so we were not affected by any
				// shrinks.
				if (TRAVERSAL_COUNT) {
					finishCount2(nodesTraversed);
				}
				return null;
			} else {
				final int childCmp = Integer.compare(k, child.key);
				if (childCmp == 0) {
					// how we got here is irrelevant
					if (TRAVERSAL_COUNT) {
						finishCount2(nodesTraversed);
					}
					return child.vOpt;
				}

				// child is non-null
				final long childOVL = child.changeOVL;
				if (isShrinkingOrUnlinked(childOVL)) {
					child.waitUntilChangeCompleted(childOVL);

					if (hasShrunkOrUnlinked(nodeOVL, node.changeOVL)) {
						if (TRAVERSAL_COUNT) {
							finishCount2(nodesTraversed);
						}
						return SpecialRetry;
					}
					// else RETRY
				} else if (child != node.child(dirToC)) {
					// this .child is the one that is protected by childOVL
					if (hasShrunkOrUnlinked(nodeOVL, node.changeOVL)) {
						if (TRAVERSAL_COUNT) {
							finishCount2(nodesTraversed);
						}
						return SpecialRetry;
					}
					// else RETRY
				} else {
					if (hasShrunkOrUnlinked(nodeOVL, node.changeOVL)) {
						if (TRAVERSAL_COUNT) {
							finishCount2(nodesTraversed);
						}
						return SpecialRetry;
					}

					// At this point we know that the traversal our parent took
					// to get to node is still valid. The recursive
					// implementation will validate the traversal from node to
					// child, so just prior to the nodeOVL validation both
					// traversals were definitely okay. This means that we are
					// no longer vulnerable to node shrinks, and we don't need
					// to validate nodeOVL any more.
					final Object vo = attemptGet(k, child, (childCmp < 0 ? Left
							: Right), childOVL);
					if (vo != SpecialRetry) {
						if (TRAVERSAL_COUNT) {
							finishCount2(nodesTraversed);
						}
						return vo;
					}
					// else RETRY
				}
			}
		}
	}

	/** Returns a key if returnKey is true, a SimpleImmutableEntry otherwise. */
	private Object extreme(final int returnType, final char dir) {
		while (true) {
			final Node right = rootHolder.right;
			if (right == null) {
				if (returnType == ReturnNode) {
					return null;
				} else {
					throw new NoSuchElementException();
				}
			} else {
				final long ovl = right.changeOVL;
				if (isShrinkingOrUnlinked(ovl)) {
					right.waitUntilChangeCompleted(ovl);
					// RETRY
				} else if (right == rootHolder.right) {
					// the reread of .right is the one protected by our read of
					// ovl
					final Object vo = attemptExtreme(returnType, dir, right,
							ovl);
					if (vo != SpecialRetry) {
						return vo;
					}
					// else RETRY
				}
			}
		}
	}

	private Object attemptExtreme(final int returnType, final char dir,
			final Node node, final long nodeOVL) {
		while (true) {
			final Node child = node.child(dir);

			if (child == null) {
				// read of the value must be protected by the OVL, because we
				// must linearize against another thread that inserts a new min
				// key and then changes this key's value
				final Object vo = node.vOpt;

				if (hasShrunkOrUnlinked(nodeOVL, node.changeOVL)) {
					return SpecialRetry;
				}

				assert (vo != null);

				switch (returnType) {
				case ReturnKey:
					return node.key;
				default:
					return node;
				}
			} else {
				// child is non-null
				final long childOVL = child.changeOVL;
				if (isShrinkingOrUnlinked(childOVL)) {
					child.waitUntilChangeCompleted(childOVL);

					if (hasShrunkOrUnlinked(nodeOVL, node.changeOVL)) {
						return SpecialRetry;
					}
					// else RETRY
				} else if (child != node.child(dir)) {
					// this .child is the one that is protected by childOVL
					if (hasShrunkOrUnlinked(nodeOVL, node.changeOVL)) {
						return SpecialRetry;
					}
					// else RETRY
				} else {
					if (hasShrunkOrUnlinked(nodeOVL, node.changeOVL)) {
						return SpecialRetry;
					}

					final Object vo = attemptExtreme(returnType, dir, child,
							childOVL);
					if (vo != SpecialRetry) {
						return vo;
					}
					// else RETRY
				}
			}
		}
	}

	// ////////////// update

	private static final int UpdateAlways = 0;
	private static final int UpdateIfAbsent = 1;
	private static final int UpdateIfPresent = 2;
	private static final int UpdateIfEq = 3;

	private static boolean shouldUpdate(final int func, final Object prev,
			final Object expected) {
		switch (func) {
		case UpdateAlways:
			return true;
		case UpdateIfAbsent:
			return prev == null;
		case UpdateIfPresent:
			return prev != null;
		default:
			return prev == expected; // TODO: use .equals
		}
	}

	@Override
	public boolean addInt(final int key) {
		return update(key, UpdateIfAbsent, null, SpecialNull) != SpecialNull;
	}

	@Override
	public boolean removeInt(final int key) {
		return update(key, UpdateAlways, null, null) == SpecialNull;
	}

	@SuppressWarnings("unchecked")
	private Object update(final int key, final int func,
			final Object expected, final Object newValue) {

		while (true) {
			final Node right = rootHolder.right;
			if (right == null) {
				// key is not present
				if (!shouldUpdate(func, null, expected) || newValue == null
						|| attemptInsertIntoEmpty(key, newValue)) {
					// nothing needs to be done, or we were successful, prev
					// value is Absent
					return null;
				}
				// else RETRY
			} else {
				final long ovl = right.changeOVL;
				if (isShrinkingOrUnlinked(ovl)) {
					right.waitUntilChangeCompleted(ovl);
					// RETRY
				} else if (right == rootHolder.right) {
					// this is the protected .right
					final Object vo = attemptUpdate(key, func, expected,
							newValue, rootHolder, right, ovl);
					if (vo != SpecialRetry) {
						return vo;
					}
					// else RETRY
				}
			}
		}
	}

	private boolean attemptInsertIntoEmpty(final int key, final Object vOpt) {
		synchronized (rootHolder) {
			if (rootHolder.right == null) {
				rootHolder.right = new Node(key, 1, vOpt, rootHolder, 0L,
						null, null);
				rootHolder.height = 2;
				return true;
			} else {
				return false;
			}
		}
	}

	/**
	 * If successful returns the non-null previous value, SpecialNull for a null
	 * previous value, or null if not previously in the map. The caller should
	 * retry if this method returns SpecialRetry.
	 */
	@SuppressWarnings("unchecked")
	private Object attemptUpdate(final int key, final int func,
			final Object expected, final Object newValue,
			final Node parent, final Node node, final long nodeOVL) {
		// As the search progresses there is an implicit min and max assumed for
		// the
		// branch of the tree rooted at node. A left rotation of a node x
		// results in
		// the range of keys in the right branch of x being reduced, so if we
		// are at a
		// node and we wish to traverse to one of the branches we must make sure
		// that
		// the node has not undergone a rotation since arriving from the parent.
		//
		// A rotation of node can't screw us up once we have traversed to node's
		// child, so we don't need to build a huge transaction, just a chain of
		// smaller read-only transactions.

		assert (nodeOVL != UnlinkedOVL);

		final int cmp = Integer.compare(key, node.key);
		if (cmp == 0) {
			return attemptNodeUpdate(func, expected, newValue, parent, node);
		}

		final char dirToC = cmp < 0 ? Left : Right;

		while (true) {
			final Node child = node.child(dirToC);

			if (hasShrunkOrUnlinked(nodeOVL, node.changeOVL)) {
				return SpecialRetry;
			}

			if (child == null) {
				// key is not present
				if (newValue == null) {
					// Removal is requested. Read of node.child occurred
					// while parent.child was valid, so we were not affected
					// by any shrinks.
					return null;
				} else {
					// Update will be an insert.
					final boolean success;
					final Node damaged;
					synchronized (node) {
						// Validate that we haven't been affected by past
						// rotations. We've got the lock on node, so no future
						// rotations can mess with us.
						if (hasShrunkOrUnlinked(nodeOVL, node.changeOVL)) {
							return SpecialRetry;
						}

						if (node.child(dirToC) != null) {
							// Lost a race with a concurrent insert. No need
							// to back up to the parent, but we must RETRY in
							// the outer loop of this method.
							success = false;
							damaged = null;
						} else {
							// We're valid. Does the user still want to
							// perform the operation?
							if (!shouldUpdate(func, null, expected)) {
								return null;
							}

							// Create a new leaf
							node.setChild(dirToC, new Node(key, 1,
									newValue, node, 0L, null, null));
							success = true;

							// attempt to fix node.height while we've still got
							// the lock
							damaged = fixHeight_nl(node);
						}
					}
					if (success) {
						fixHeightAndRebalance(damaged);
						return null;
					}
					// else RETRY
				}
			} else {
				// non-null child
				final long childOVL = child.changeOVL;
				if (isShrinkingOrUnlinked(childOVL)) {
					child.waitUntilChangeCompleted(childOVL);
					// RETRY
				} else if (child != node.child(dirToC)) {
					// this second read is important, because it is protected
					// by childOVL
					// RETRY
				} else {
					// validate the read that our caller took to get to node
					if (hasShrunkOrUnlinked(nodeOVL, node.changeOVL)) {
						return SpecialRetry;
					}

					// At this point we know that the traversal our parent took
					// to get to node is still valid. The recursive
					// implementation will validate the traversal from node to
					// child, so just prior to the nodeOVL validation both
					// traversals were definitely okay. This means that we are
					// no longer vulnerable to node shrinks, and we don't need
					// to validate nodeOVL any more.
					final Object vo = attemptUpdate(key, func, expected,
							newValue, node, child, childOVL);
					if (vo != SpecialRetry) {
						return vo;
					}
					// else RETRY
				}
			}
		}
	}

	/**
	 * parent will only be used for unlink, update can proceed even if parent is
	 * stale.
	 */
	private Object attemptNodeUpdate(final int func, final Object expected,
			final Object newValue, final Node parent,
			final Node node) {
		if (newValue == null) {
			// removal
			if (node.vOpt == null) {
				// This node is already removed, nothing to do.
				return null;
			}
		}

		if (newValue == null && (node.left == null || node.right == null)) {
			// potential unlink, get ready by locking the parent
			final Object prev;
			final Node damaged;
			synchronized (parent) {
				if (isUnlinked(parent.changeOVL) || node.parent != parent) {
					return SpecialRetry;
				}

				synchronized (node) {
					prev = node.vOpt;
					if (prev == null || !shouldUpdate(func, prev, expected)) {
						// nothing to do
						return prev;
					}
					if (!attemptUnlink_nl(parent, node)) {
						return SpecialRetry;
					}
				}
				// try to fix the parent while we've still got the lock
				damaged = fixHeight_nl(parent);
			}
			fixHeightAndRebalance(damaged);
			return prev;
		} else {
			// potential update (including remove-without-unlink)
			synchronized (node) {
				// regular version changes don't bother us
				if (isUnlinked(node.changeOVL)) {
					return SpecialRetry;
				}

				final Object prev = node.vOpt;
				if (!shouldUpdate(func, prev, expected)) {
					return prev;
				}

				// retry if we now detect that unlink is possible
				if (newValue == null
						&& (node.left == null || node.right == null)) {
					return SpecialRetry;
				}

				// update in-place
				node.vOpt = newValue;
				return prev;
			}
		}
	}

	/** Does not adjust the size or any heights. */
	private boolean attemptUnlink_nl(final Node parent,
			final Node node) {
		// assert (Thread.holdsLock(parent));
		// assert (Thread.holdsLock(node));
		assert (!isUnlinked(parent.changeOVL));

		final Node parentL = parent.left;
		final Node parentR = parent.right;
		if (parentL != node && parentR != node) {
			// node is no longer a child of parent
			return false;
		}

		assert (!isUnlinked(node.changeOVL));
		assert (parent == node.parent);

		final Node left = node.left;
		final Node right = node.right;
		if (left != null && right != null) {
			// splicing is no longer possible
			return false;
		}
		final Node splice = left != null ? left : right;

		if (parentL == node) {
			parent.left = splice;
		} else {
			parent.right = splice;
		}
		if (splice != null) {
			splice.parent = parent;
		}

		node.changeOVL = UnlinkedOVL;
		node.vOpt = null;

		return true;
	}

	// ////////////// tree balance and height info repair

	private static final int UnlinkRequired = -1;
	private static final int RebalanceRequired = -2;
	private static final int NothingRequired = -3;

	private int nodeCondition(final Node node) {
		// Begin atomic.

		final Node nL = node.left;
		final Node nR = node.right;

		if ((nL == null || nR == null) && node.vOpt == null) {
			return UnlinkRequired;
		}

		final int hN = node.height;
		final int hL0 = height(nL);
		final int hR0 = height(nR);

		// End atomic. Since any thread that changes a node promises to fix
		// it, either our read was consistent (and a NothingRequired conclusion
		// is correct) or someone else has taken responsibility for either node
		// or one of its children.

		final int hNRepl = 1 + Math.max(hL0, hR0);
		final int bal = hL0 - hR0;

		if (bal < -1 || bal > 1) {
			return RebalanceRequired;
		}

		return hN != hNRepl ? hNRepl : NothingRequired;
	}

	private void fixHeightAndRebalance(Node node) {
		while (node != null && node.parent != null) {
			final int condition = nodeCondition(node);
			if (condition == NothingRequired || isUnlinked(node.changeOVL)) {
				// nothing to do, or no point in fixing this node
				return;
			}

			if (condition != UnlinkRequired && condition != RebalanceRequired) {
				synchronized (node) {
					node = fixHeight_nl(node);
				}
			} else {
				final Node nParent = node.parent;
				synchronized (nParent) {
					if (!isUnlinked(nParent.changeOVL)
							&& node.parent == nParent) {
						synchronized (node) {
							node = rebalance_nl(nParent, node);
						}
					}
					// else RETRY
				}
			}
		}
	}

	/**
	 * Attempts to fix the height of a (locked) damaged node, returning the
	 * lowest damaged node for which this thread is responsible. Returns null if
	 * no more repairs are needed.
	 */
	private Node fixHeight_nl(final Node node) {
		final int c = nodeCondition(node);
		switch (c) {
		case RebalanceRequired:
		case UnlinkRequired:
			// can't repair
			return node;
		case NothingRequired:
			// Any future damage to this node is not our responsibility.
			return null;
		default:
			node.height = c;
			// we've damaged our parent, but we can't fix it now
			return node.parent;
		}
	}

	/**
	 * nParent and n must be locked on entry. Returns a damaged node, or null if
	 * no more rebalancing is necessary.
	 */
	private Node rebalance_nl(final Node nParent, final Node n) {

		final Node nL = n.left;
		final Node nR = n.right;

		if ((nL == null || nR == null) && n.vOpt == null) {
			if (attemptUnlink_nl(nParent, n)) {
				// attempt to fix nParent.height while we've still got the lock
				return fixHeight_nl(nParent);
			} else {
				// retry needed for n
				return n;
			}
		}

		final int hN = n.height;
		final int hL0 = height(nL);
		final int hR0 = height(nR);
		final int hNRepl = 1 + Math.max(hL0, hR0);
		final int bal = hL0 - hR0;

		if (bal > 1) {
			return rebalanceToRight_nl(nParent, n, nL, hR0);
		} else if (bal < -1) {
			return rebalanceToLeft_nl(nParent, n, nR, hL0);
		} else if (hNRepl != hN) {
			// we've got more than enough locks to do a height change, no need
			// to
			// trigger a retry
			n.height = hNRepl;

			// nParent is already locked, let's try to fix it too
			return fixHeight_nl(nParent);
		} else {
			// nothing to do
			return null;
		}
	}

	private Node rebalanceToRight_nl(final Node nParent,
			final Node n, final Node nL, final int hR0) {
		// L is too large, we will rotate-right. If L.R is taller
		// than L.L, then we will first rotate-left L.
		synchronized (nL) {
			final int hL = nL.height;
			if (hL - hR0 <= 1) {
				return n; // retry
			} else {
				final Node nLR = nL.right;
				final int hLL0 = height(nL.left);
				final int hLR0 = height(nLR);
				if (hLL0 >= hLR0) {
					// rotate right based on our snapshot of hLR
					return rotateRight_nl(nParent, n, nL, hR0, hLL0, nLR, hLR0);
				} else {
					synchronized (nLR) {
						// If our hLR snapshot is incorrect then we might
						// actually need to do a single rotate-right on n.
						final int hLR = nLR.height;
						if (hLL0 >= hLR) {
							return rotateRight_nl(nParent, n, nL, hR0, hLL0,
									nLR, hLR);
						} else {
							// If the underlying left balance would not be
							// sufficient to actually fix n.left, then instead
							// of rolling it into a double rotation we do it on
							// it's own. This may let us avoid rotating n at
							// all, but more importantly it avoids the creation
							// of damaged nodes that don't have a direct
							// ancestry relationship. The recursive call to
							// rebalanceToRight_nl in this case occurs after we
							// release the lock on nLR.
							final int hLRL = height(nLR.left);
							final int b = hLL0 - hLRL;
							if (b >= -1 && b <= 1) {
								// nParent.child.left won't be damaged after a
								// double rotation
								return rotateRightOverLeft_nl(nParent, n, nL,
										hR0, hLL0, nLR, hLRL);
							}
						}
					}
					// focus on nL, if necessary n will be balanced later
					return rebalanceToLeft_nl(n, nL, nLR, hLL0);
				}
			}
		}
	}

	private Node rebalanceToLeft_nl(final Node nParent,
			final Node n, final Node nR, final int hL0) {
		synchronized (nR) {
			final int hR = nR.height;
			if (hL0 - hR >= -1) {
				return n; // retry
			} else {
				final Node nRL = nR.left;
				final int hRL0 = height(nRL);
				final int hRR0 = height(nR.right);
				if (hRR0 >= hRL0) {
					return rotateLeft_nl(nParent, n, hL0, nR, nRL, hRL0, hRR0);
				} else {
					synchronized (nRL) {
						final int hRL = nRL.height;
						if (hRR0 >= hRL) {
							return rotateLeft_nl(nParent, n, hL0, nR, nRL, hRL,
									hRR0);
						} else {
							final int hRLR = height(nRL.right);
							final int b = hRR0 - hRLR;
							if (b >= -1 && b <= 1) {
								return rotateLeftOverRight_nl(nParent, n, hL0,
										nR, nRL, hRR0, hRLR);
							}
						}
					}
					return rebalanceToRight_nl(n, nR, nRL, hRR0);
				}
			}
		}
	}

	private Node rotateRight_nl(final Node nParent,
			final Node n, final Node nL, final int hR,
			final int hLL, final Node nLR, final int hLR) {
		final long nodeOVL = n.changeOVL;
		final long leftOVL = nL.changeOVL;

		if (STRUCT_MODS)
			counts.get().structMods += 1;

		final Node nPL = nParent.left;

		n.changeOVL = beginShrink(nodeOVL);
		nL.changeOVL = beginGrow(leftOVL);

		// Down links originally to shrinking nodes should be the last to
		// change,
		// because if we change them early a search might bypass the OVL that
		// indicates its invalidity. Down links originally from shrinking nodes
		// should be the first to change, because we have complete freedom when
		// to
		// change them. s/down/up/ and s/shrink/grow/ for the parent links.

		n.left = nLR;
		nL.right = n;
		if (nPL == n) {
			nParent.left = nL;
		} else {
			nParent.right = nL;
		}

		nL.parent = nParent;
		n.parent = nL;
		if (nLR != null) {
			nLR.parent = n;
		}

		// fix up heights links
		final int hNRepl = 1 + Math.max(hLR, hR);
		n.height = hNRepl;
		nL.height = 1 + Math.max(hLL, hNRepl);

		nL.changeOVL = endGrow(leftOVL);
		n.changeOVL = endShrink(nodeOVL);

		// We have damaged nParent, n (now parent.child.right), and nL (now
		// parent.child). n is the deepest. Perform as many fixes as we can
		// with the locks we've got.

		// We've already fixed the height for n, but it might still be outside
		// our allowable balance range. In that case a simple fixHeight_nl
		// won't help.
		final int balN = hLR - hR;
		if (balN < -1 || balN > 1) {
			// we need another rotation at n
			return n;
		}

		// we've already fixed the height at nL, do we need a rotation here?
		final int balL = hLL - hNRepl;
		if (balL < -1 || balL > 1) {
			return nL;
		}

		// try to fix the parent height while we've still got the lock
		return fixHeight_nl(nParent);
	}

	private Node rotateLeft_nl(final Node nParent,
			final Node n, final int hL, final Node nR,
			final Node nRL, final int hRL, final int hRR) {
		final long nodeOVL = n.changeOVL;
		final long rightOVL = nR.changeOVL;

		final Node nPL = nParent.left;

		if (STRUCT_MODS)
			counts.get().structMods += 1;

		n.changeOVL = beginShrink(nodeOVL);
		nR.changeOVL = beginGrow(rightOVL);

		n.right = nRL;
		nR.left = n;
		if (nPL == n) {
			nParent.left = nR;
		} else {
			nParent.right = nR;
		}

		nR.parent = nParent;
		n.parent = nR;
		if (nRL != null) {
			nRL.parent = n;
		}

		// fix up heights
		final int hNRepl = 1 + Math.max(hL, hRL);
		n.height = hNRepl;
		nR.height = 1 + Math.max(hNRepl, hRR);

		nR.changeOVL = endGrow(rightOVL);
		n.changeOVL = endShrink(nodeOVL);

		final int balN = hRL - hL;
		if (balN < -1 || balN > 1) {
			return n;
		}

		final int balR = hRR - hNRepl;
		if (balR < -1 || balR > 1) {
			return nR;
		}

		return fixHeight_nl(nParent);
	}

	private Node rotateRightOverLeft_nl(final Node nParent,
			final Node n, final Node nL, final int hR,
			final int hLL, final Node nLR, final int hLRL) {
		final long nodeOVL = n.changeOVL;
		final long leftOVL = nL.changeOVL;
		final long leftROVL = nLR.changeOVL;

		final Node nPL = nParent.left;
		final Node nLRL = nLR.left;
		final Node nLRR = nLR.right;
		final int hLRR = height(nLRR);

		if (STRUCT_MODS)
			counts.get().structMods += 1;

		n.changeOVL = beginShrink(nodeOVL);
		nL.changeOVL = beginShrink(leftOVL);
		nLR.changeOVL = beginGrow(leftROVL);

		n.left = nLRR;
		nL.right = nLRL;
		nLR.left = nL;
		nLR.right = n;
		if (nPL == n) {
			nParent.left = nLR;
		} else {
			nParent.right = nLR;
		}

		nLR.parent = nParent;
		nL.parent = nLR;
		n.parent = nLR;
		if (nLRR != null) {
			nLRR.parent = n;
		}
		if (nLRL != null) {
			nLRL.parent = nL;
		}

		// fix up heights
		final int hNRepl = 1 + Math.max(hLRR, hR);
		n.height = hNRepl;
		final int hLRepl = 1 + Math.max(hLL, hLRL);
		nL.height = hLRepl;
		nLR.height = 1 + Math.max(hLRepl, hNRepl);

		nLR.changeOVL = endGrow(leftROVL);
		nL.changeOVL = endShrink(leftOVL);
		n.changeOVL = endShrink(nodeOVL);

		// caller should have performed only a single rotation if nL was going
		// to end up damaged
		assert (Math.abs(hLL - hLRL) <= 1);

		// We have damaged nParent, nLR (now parent.child), and n (now
		// parent.child.right). n is the deepest. Perform as many fixes as we
		// can with the locks we've got.

		// We've already fixed the height for n, but it might still be outside
		// our allowable balance range. In that case a simple fixHeight_nl
		// won't help.
		final int balN = hLRR - hR;
		if (balN < -1 || balN > 1) {
			// we need another rotation at n
			return n;
		}

		// we've already fixed the height at nLR, do we need a rotation here?
		final int balLR = hLRepl - hNRepl;
		if (balLR < -1 || balLR > 1) {
			return nLR;
		}

		// try to fix the parent height while we've still got the lock
		return fixHeight_nl(nParent);
	}

	private Node rotateLeftOverRight_nl(final Node nParent,
			final Node n, final int hL, final Node nR,
			final Node nRL, final int hRR, final int hRLR) {
		final long nodeOVL = n.changeOVL;
		final long rightOVL = nR.changeOVL;
		final long rightLOVL = nRL.changeOVL;

		final Node nPL = nParent.left;
		final Node nRLL = nRL.left;
		final int hRLL = height(nRLL);
		final Node nRLR = nRL.right;

		if (STRUCT_MODS)
			counts.get().structMods += 1;

		n.changeOVL = beginShrink(nodeOVL);
		nR.changeOVL = beginShrink(rightOVL);
		nRL.changeOVL = beginGrow(rightLOVL);

		n.right = nRLL;
		nR.left = nRLR;
		nRL.right = nR;
		nRL.left = n;
		if (nPL == n) {
			nParent.left = nRL;
		} else {
			nParent.right = nRL;
		}

		nRL.parent = nParent;
		nR.parent = nRL;
		n.parent = nRL;
		if (nRLL != null) {
			nRLL.parent = n;
		}
		if (nRLR != null) {
			nRLR.parent = nR;
		}

		// fix up heights
		final int hNRepl = 1 + Math.max(hL, hRLL);
		n.height = hNRepl;
		final int hRRepl = 1 + Math.max(hRLR, hRR);
		nR.height = hRRepl;
		nRL.height = 1 + Math.max(hNRepl, hRRepl);

		nRL.changeOVL = endGrow(rightLOVL);
		nR.changeOVL = endShrink(rightOVL);
		n.changeOVL = endShrink(nodeOVL);

		assert (Math.abs(hRR - hRLR) <= 1);

		final int balN = hRLL - hL;
		if (balN < -1 || balN > 1) {
			return n;
		}
		final int balRL = hRRepl - hNRepl;
		if (balRL < -1 || balRL > 1) {
			return nRL;
		}
		return fixHeight_nl(nParent);
	}

	// ////////////// iteration (node successor)
}
