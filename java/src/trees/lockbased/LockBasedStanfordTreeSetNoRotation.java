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
public class LockBasedStanfordTreeSetNoRotation extends AbstractCompositionalIntSet {
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

		/**
		 * null means this node is conceptually not present in the map.
		 * SpecialNull means the value is null.
		 */
		volatile Object vOpt;
		volatile Node parent;
		volatile long changeOVL;
		volatile Node left;
		volatile Node right;

		Node(final int key, final Object vOpt,
				final Node parent, final long changeOVL,
				final Node left, final Node right) {
			this.key = key;
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
	}

	// ////// node access functions

	private static Object encodeNull(final Object v) {
		return v == null ? SpecialNull : v;
	}

	// ////////////// state

	private final Node rootHolder = new Node(Integer.MIN_VALUE, null, null,
			0L, null, null);

	// ////////////// public interface

	public LockBasedStanfordTreeSetNoRotation() {
	}

	public int size(Node node) {
		if (node == null)
			return 0;
		return (node.vOpt == SpecialNull ? 1 : 0) + size(node.left) + size(node.right);
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
				rootHolder.right = new Node(key, vOpt, rootHolder, 0L,
						null, null);
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
	private Object attemptUpdate(final int key,
			final int func,
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
							node.setChild(dirToC, new Node(key,
									newValue, node, 0L, null, null));
							success = true;
						}
					}
					if (success) {
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
			}
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
}
