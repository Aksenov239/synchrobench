/**
 *  This is an implementation of the non-blocking, concurrent binary search tree of
 *  Faith Ellen, Panagiota Fatourou, Eric Ruppert and Franck van Breugel.
 *
 *  Copyright (C) 2011  Trevor Brown, Joanna Helga
 *  Contact Trevor Brown (tabrown@cs.toronto.edu) with any questions or comments.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package trees.lockfree;

import contention.abstractions.AbstractCompositionalIntSet;
import contention.abstractions.CompositionalMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class NonBlockingTorontoSet extends AbstractCompositionalIntSet {

	// --------------------------------------------------------------------------------
	// Class: Node
	// --------------------------------------------------------------------------------
	protected final static class Node {
		final int key;
		volatile Node left;
		volatile Node right;
		volatile Info info;

		/** FOR MANUAL CREATION OF NODES (only used directly by testbed) **/
		Node(final int key, final Node left,
				final Node right) {
			this.key = key;
			this.left = left;
			this.right = right;
			this.info = null;
		}

		/** TO CREATE A LEAF NODE **/
		Node(final int key) {
			this(key, null, null);
		}

	}

	// --------------------------------------------------------------------------------
	// Class: Info, DInfo, IInfo, Mark, Clean
	// May 25th: trying to make CAS to update field static
	// instead of using <state, Info>, we extends Info to all 4 states
	// to see a state of a node, see what kind of Info class it has
	// --------------------------------------------------------------------------------
	protected static abstract class Info {
	}

	protected final static class DInfo
			extends Info {
		final Node p;
		final Node l;
		final Node gp;
		final Info pinfo;

		DInfo(final Node leaf, final Node parent,
				final Node grandparent, final Info pinfo) {
			this.p = parent;
			this.l = leaf;
			this.gp = grandparent;
			this.pinfo = pinfo;
		}
	}

	protected final static class IInfo
			extends Info {
		final Node p;
		final Node l;
		final Node newInternal;

		IInfo(final Node leaf, final Node parent,
				final Node newInternal) {
			this.p = parent;
			this.l = leaf;
			this.newInternal = newInternal;
		}
	}

	protected final static class Mark
			extends Info {
		final DInfo dinfo;

		Mark(final DInfo dinfo) {
			this.dinfo = dinfo;
		}
	}

	protected final static class Clean
			extends Info {
	}

	// --------------------------------------------------------------------------------
	// DICTIONARY
	// --------------------------------------------------------------------------------
	private static final AtomicReferenceFieldUpdater<Node, Node> leftUpdater = AtomicReferenceFieldUpdater
			.newUpdater(Node.class, Node.class, "left");
	private static final AtomicReferenceFieldUpdater<Node, Node> rightUpdater = AtomicReferenceFieldUpdater
			.newUpdater(Node.class, Node.class, "right");
	private static final AtomicReferenceFieldUpdater<Node, Info> infoUpdater = AtomicReferenceFieldUpdater
			.newUpdater(Node.class, Info.class, "info");

	final Node root;

	public NonBlockingTorontoSet() {
		// to avoid handling special case when <= 2 nodes,
		// create 2 dummy nodes, both contain key null
		// All real keys inside BST are required to be non-null
		root = new Node(Integer.MAX_VALUE, new Node(Integer.MAX_VALUE), new Node(
				Integer.MAX_VALUE));
	}

	public NonBlockingTorontoSet(final Node root) {
		this.root = root;
	}

	// --------------------------------------------------------------------------------
	// PUBLIC METHODS:
	// - find : boolean
	// - insert : boolean
	// - delete : boolean
	// --------------------------------------------------------------------------------

	/** PRECONDITION: k CANNOT BE NULL **/
	public final boolean containsInt(final int key) {
		Node l = root.left;
		while (l.left != null) {
			l = (key < l.key) ? l.left : l.right;
		}
		return (l.key != Integer.MAX_VALUE && Integer.compare(key, l.key) == 0) ? true : false;
	}

	// Insert key to dictionary, returns the previous value associated with the
	// specified key,
	// or null if there was no mapping for the key
	/** PRECONDITION: k CANNOT BE NULL **/
	@Override
	public final boolean addInt(final int key) {
		Node newInternal;
		Node newSibling, newNode;

		/** SEARCH VARIABLES **/
		Node p;
		Info pinfo;
		Node l;
		/** END SEARCH VARIABLES **/

		newNode = new Node(key);

		while (true) {

			/** SEARCH **/
			p = root;
			pinfo = p.info;
			l = p.left;
			while (l.left != null) {
				p = l;
				l = (key < l.key) ? l.left
						: l.right;
			}
			pinfo = p.info; // read pinfo once instead of every iteration
			if (l != p.left && l != p.right)
				continue; // then confirm the child link to l is valid
							// (just as if we'd read p's info field before the
							// reference to l)
			/** END SEARCH **/

			if (key == l.key) {
				return false; // key already in the tree, no duplicate allowed
			} else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
				help(pinfo);
			} else {
				newSibling = new Node(l.key);
				if (key < l.key) // newinternal =
																// max(ret.l.key,
																// key);
					newInternal = new Node(l.key, newNode, newSibling);
				else
					newInternal = new Node(key, newSibling, newNode);

				final IInfo newPInfo = new IInfo(l, p, newInternal);

				// try to IFlag parent
				if (infoUpdater.compareAndSet(p, pinfo, newPInfo)) {
					helpInsert(newPInfo);
					return true;
				} else {
					// if fails, help the current operation
					// [CHECK]
					// need to get the latest p.info since CAS doesnt return
					// current value
					help(p.info);
				}
			}
		}
	}

	// Delete key from dictionary, return the associated value when successful,
	// null otherwise
	/** PRECONDITION: k CANNOT BE NULL **/
	@Override
	public final boolean removeInt(final int key) {
		/** SEARCH VARIABLES **/
		Node gp;
		Info gpinfo;
		Node p;
		Info pinfo;
		Node l;
		/** END SEARCH VARIABLES **/

		while (true) {

			/** SEARCH **/
			gp = null;
			gpinfo = null;
			p = root;
			pinfo = p.info;
			l = p.left;
			while (l.left != null) {
				gp = p;
				p = l;
				l = (key < l.key) ? l.left
						: l.right;
			}
			// note: gp can be null here, because clearly the root.left.left ==
			// null
			// when the tree is empty. however, in this case, l.key will be
			// null,
			// and the function will return null, so this does not pose a
			// problem.
			if (gp != null) {
				gpinfo = gp.info; // - read gpinfo once instead of every
									// iteration
				if (p != gp.left && p != gp.right)
					continue; // then confirm the child link to p is valid
				pinfo = p.info; // (just as if we'd read gp's info field before
								// the reference to p)
				if (l != p.left && l != p.right)
					continue; // - do the same for pinfo and l
			}
			/** END SEARCH **/

			if (key != l.key)
				return false;
			if (!(gpinfo == null || gpinfo.getClass() == Clean.class)) {
				help(gpinfo);
			} else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
				help(pinfo);
			} else {
				// try to DFlag grandparent
				final DInfo newGPInfo = new DInfo(l, p, gp, pinfo);

				if (infoUpdater.compareAndSet(gp, gpinfo, newGPInfo)) {
					if (helpDelete(newGPInfo))
						return true;
				} else {
					// if fails, help grandparent with its latest info value
					help(gp.info);
				}
			}
		}
	}

	// --------------------------------------------------------------------------------
	// PRIVATE METHODS
	// - helpInsert
	// - helpDelete
	// --------------------------------------------------------------------------------

	private void helpInsert(final IInfo info) {
		(info.p.left == info.l ? leftUpdater : rightUpdater).compareAndSet(
				info.p, info.l, info.newInternal);
		infoUpdater.compareAndSet(info.p, info, new Clean());
	}

	private boolean helpDelete(final DInfo info) {
		final boolean result;

		result = infoUpdater.compareAndSet(info.p, info.pinfo, new Mark(info));
		final Info currentPInfo = info.p.info;
		// if CAS succeed or somebody else already suceed helping, the
		// helpMarked
		if (result
				|| (currentPInfo.getClass() == Mark.class && ((Mark) currentPInfo).dinfo == info)) {
			helpMarked(info);
			return true;
		} else {
			help(currentPInfo);
			infoUpdater.compareAndSet(info.gp, info, new Clean());
			return false;
		}
	}

	private void help(final Info info) {
		if (info.getClass() == IInfo.class)
			helpInsert((IInfo) info);
		else if (info.getClass() == DInfo.class)
			helpDelete((DInfo) info);
		else if (info.getClass() == Mark.class)
			helpMarked(((Mark) info).dinfo);
	}

	private void helpMarked(final DInfo info) {
		final Node other = (info.p.right == info.l) ? info.p.left
				: info.p.right;
		(info.gp.left == info.p ? leftUpdater : rightUpdater).compareAndSet(
				info.gp, info.p, other);
		infoUpdater.compareAndSet(info.gp, info, new Clean());
	}

	/**
	 *
	 * DEBUG CODE (FOR TESTBED)
	 *
	 */

	/**
	 * If size() returns -1, then concurrent access prevented its successful
	 * read. To ensure it returns the tree size, call it using the pattern:
	 * <code>
	 *      while((sz = tree.size()) == -1) {}
	 *  </code> This allows the caller to decide whether it should block.
	 */
	@Override
	public final int size() {
		Node root = getSnapshot();
		if (root == null)
			return -1;
		return sequentialSize(root);
	}

	@Override
	public final NonBlockingTorontoSet clone() {
		Node newroot = null;
		for (;;)
			if ((newroot = getSnapshot()) != null)
				return new NonBlockingTorontoSet(newroot);
	}

	/**
	 * WARNING: This assumes that there are no concurrent accesses occurring. If
	 * concurrent accesses can occur, use size() (or this on a clone()).
	 */
	public int sequentialSize(final Node node) {
		if (node == null)
			return 0;
		if (node.left == null && node.key != Integer.MAX_VALUE)
			return 1;
		return sequentialSize(node.left) + sequentialSize(node.right);
	}

	private void readRefs(final Node node,
			final HashMap<Node, Pair<Node, Node>> refs) {
		if (node == null)
			return;
		refs.put(node, new Pair<Node, Node>(node.left, node.right));
		readRefs(node.left, refs);
		readRefs(node.right, refs);
	}

	private boolean checkRefs(final Node node,
			final HashMap<Node, Pair<Node, Node>> refs) {
		if (node == null)
			return true;
		Pair<Node, Node> p = refs.get(node);
		if (!p.equals(new Pair<Node, Node>(node.left, node.right)))
			return false;
		return checkRefs(p.key, refs) && checkRefs(p.value, refs);
	}

	private Node buildRefs(final Node node,
			final HashMap<Node, Pair<Node, Node>> refs) {
		if (node == null)
			return null;
		Pair<Node, Node> children = refs.get(node);
		return new Node(node.key, buildRefs(children.key, refs),
				buildRefs(children.value, refs));
	}

	private Node getSnapshot() {
		final HashMap<Node, Pair<Node, Node>> refs = new HashMap<Node, Pair<Node, Node>>();
		readRefs(root, refs);
		if (!checkRefs(root, refs))
			return null;
		return buildRefs(root, refs);
	}

	class Pair<K, V> {
		K key;
		V value;

		public Pair(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null || !o.getClass().equals(getClass()))
				return false; // CAN DO AWAY WITH THIS AT THE COST OF TYPE
								// SAFETY
			Pair<K, V> p = (Pair<K, V>) o;
			return p.key == key && p.value == value;
		}
	}

	private int sumDepths(Node node, int depth) {
		if (node == null)
			return 0;
		return depth + sumDepths(node.left, depth + 1)
				+ sumDepths(node.right, depth + 1);
	}

	public final int getSumOfDepths() {
		return sumDepths(root, 0);
	}

	/**
	 * WARNING: This assumes that there are no concurrent accesses occurring. If
	 * concurrent accesses can occur, use size() (or this on a clone()).
	 */
	public final int sequentialSize() {
		return sequentialSize(root);
	}

	@Override
	public boolean isEmpty() {
		return (this.size() == 0) ? true: false;
	}

	@Override
	public void clear() {
		// root = new Node<K, V>(null, new Node<K, V>(null, null), new Node<K,
		// V>(
		// null, null));
		root.left = new Node(Integer.MAX_VALUE);
		root.right = new Node(Integer.MAX_VALUE);
		root.info = null;
	}

}
