package trees.lockbased;

import contention.abstractions.AbstractCompositionalIntSet;
import contention.abstractions.CompositionalMap;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of concurrent AVL tree based on the paper 
 * "Practical Concurrent Binary Search Trees via Logical Ordering" by 
 * Dana Drachsler (Technion), Martin Vechev (ETH) and Eran Yahav (Technion).
 *
 * Copyright 2013 Dana Drachsler (ddana [at] cs [dot] technion [dot] ac [dot] il).
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * @author Dana Drachsler
 */
public class LogicalOrderingAVLSet extends AbstractCompositionalIntSet {

	/** The tree's root */
	private AVLMapNode root;

	/** A constant object for the use of the {@code insert} method.  */
	private final static Object EMPTY_ITEM = new Object();


	public LogicalOrderingAVLSet() {
		AVLMapNode parent = new AVLMapNode(Integer.MIN_VALUE);
		root = new AVLMapNode(Integer.MAX_VALUE, parent, parent, parent);
		root.parent = parent;
		parent.right = root;
		parent.succ = root;
	}

	/**
	 * Constructor, initialize the tree and the logical ordering layouts.
	 * The logical ordering is initialized by creating two nodes, where their
	 * keys are the minimal and maximal values.
	 * The tree layout is initialized by setting the root to point to the node
	 * with the maximal value.
	 *
	 * @param min The minimal value
	 * @param max The maximal value
	 */
	public LogicalOrderingAVLSet(final int min, final int max) {
		AVLMapNode parent = new AVLMapNode(min);
		root = new AVLMapNode(max, parent, parent, parent);
		root.parent = parent;
		parent.right = root;
		parent.succ = root;
	}

	/**
	 * Traverses the tree to find a node with the given key.
	 *
	 * @see java.util.Map#containsKey(Object)
	 */
	@Override
	final public boolean containsInt(final int key) {
		AVLMapNode node = root;
		AVLMapNode child;
		int res = -1;
		int val;
		while (true) {
			if (res == 0) break;
			if (res > 0) {
				child = node.right;
			} else {
				child = node.left;
			}
			if (child == null) break;
			node = child;
			val = node.key;
			res = Integer.compare(key, val);
		}
		while (key < node.key) {
			node = node.pred;
		}
		while (key > node.key) {
			node = node.succ;
		}
		return (res == 0 && node.valid);
	}

	/**
	 * @see java.util.Map#put(Object, Object)
	 */
	public boolean addInt(final int key) {
		AVLMapNode node = null;
		int nodeValue;
		int res = -1;
		while (true) {
			node = root;
			AVLMapNode child;
			res = -1;
			while (true) {
				if (res == 0) break;
				if (res > 0) {
					child = node.right;
				} else {
					child = node.left;
				}
				if (child == null) break;
				node = child;
				nodeValue = node.key;
				res = Integer.compare(key, nodeValue);
			}
			final AVLMapNode pred = res > 0 ? node : node.pred;
			pred.lockSuccLock();
			if (pred.valid) {
				final int predVal = pred.key;
				final int predRes = pred== node? res: Integer.compare(key, predVal);
				if (predRes > 0) {
					final AVLMapNode succ = pred.succ;
					final int succVal = succ.key;
					final int res2 = succ == node? res: Integer.compare(key, succVal);
					if (res2 <= 0) {
						if (res2 == 0) {
							pred.unlockSuccLock();
							return false;
						}
						final AVLMapNode parent = chooseParent(pred, succ, node);
						final AVLMapNode newNode = new AVLMapNode(key, pred, succ, parent);
						succ.pred = newNode;
						pred.succ = newNode;
						pred.unlockSuccLock();
						insertToTree(parent, newNode, parent == pred);
						return true;
					}
				}
			}
			pred.unlockSuccLock();
		}
	}

	/**
	 * Choose and lock the correct parent, given the new node's predecessor,
	 * successor, and the node returned from the traversal.
	 *
	 * @param pred The predecessor
	 * @param succ The successor
	 * @param firstCand The node returned from the traversal
	 * @return The correct parent
	 */
	final private AVLMapNode chooseParent(final AVLMapNode pred,
			final AVLMapNode succ, final AVLMapNode firstCand) {
		AVLMapNode candidate = firstCand == pred || firstCand == succ? firstCand: pred;
		while (true) {
			candidate.lockTreeLock();
			if (candidate == pred) {
				if (candidate.right == null) {
					return candidate;
				}
				candidate.unlockTreeLock();
				candidate = succ;
			} else {
				if (candidate.left == null) {
					return candidate;
				}
				candidate.unlockTreeLock();
				candidate = pred;
			}
			Thread.yield();
		}
	}

	/**
	 * Update the tree layout by connecting the new node to its parent.
	 * Then, the parent's height is updated, and {@link #rebalance} is called.
	 *
	 * @param parent The new node's parent
	 * @param newNode The new node
	 * @param isRight Is the new node should be the parent's right child?
	 */
	final private void insertToTree(final AVLMapNode parent, final AVLMapNode newNode, final boolean isRight) {
		if (isRight) {
			parent.right = newNode;
			parent.rightHeight = 1;
		} else {
			parent.left = newNode;
			parent.leftHeight = 1;
		}
		if (parent != root) {
			AVLMapNode grandParent = lockParent(parent);
			rebalance(grandParent, parent, grandParent.left == parent);
		} else {
			parent.unlockTreeLock();
		}
	}

	/**
	 * Lock the given node's parent.
	 * The operation begins by first reading the node's parent from the node,
	 * then acquiring the parent's lock, and then checking whether this is the
	 * correct parent. If not, the lock is released, and the operation restarts.
	 *
	 * @param node The node
	 * @return The node's parent (which is locked)
	 */
	final private AVLMapNode lockParent(final AVLMapNode node) {
		AVLMapNode parent = node.parent;
		parent.lockTreeLock();
		while (node.parent != parent || !parent.valid) {
			parent.unlockTreeLock();
			parent = node.parent;
			while (!parent.valid) {
				Thread.yield();
				parent = node.parent;
			}
			parent.lockTreeLock();
		}
		return parent;
	}

	/**
	 * @see java.util.Map#remove(Object)
	 */
	final public boolean removeInt(final int key) {
		AVLMapNode pred, node = null;
		int nodeValue;
		int res = 0;
		while (true) {
			node = root;
			AVLMapNode child;
			res = -1;
			while (true) {
				if (res == 0) break;
				if (res > 0) {
					child = node.right;
				} else {
					child = node.left;
				}
				if (child == null) break;
				node = child;
				nodeValue = node.key;
				res = Integer.compare(key, nodeValue);
			}
			pred = res > 0 ? node : node.pred;
			pred.lockSuccLock();
			if (pred.valid) {
				final int predVal = pred.key;
				final int predRes = pred== node? res: Integer.compare(key, predVal);
				if (predRes > 0) {
					AVLMapNode succ = pred.succ;
					final int succVal = succ.key;
					int res2 = succ == node? res: Integer.compare(key, succVal);
					if (res2 <= 0) {
						if (res2 != 0) {
							pred.unlockSuccLock();
							return false;
						}
						succ.lockSuccLock();
						AVLMapNode successor = acquireTreeLocks(succ);
						AVLMapNode succParent = lockParent(succ);
						succ.valid = false;
						AVLMapNode succSucc = succ.succ;
						succSucc.pred = pred;
						pred.succ = succSucc;
						succ.unlockSuccLock();
						pred.unlockSuccLock();
						removeFromTree(succ, successor, succParent);
						return true;
					}
				}
			}
			pred.unlockSuccLock();
		}
	}

	/**
	 * Acquire the treeLocks of the following nodes:
	 * <ul>
	 * <li> The given node
	 * <li> The node's child - if the given node has less than two children
	 * <li> The node's successor, and the successor's parent and child - if the
	 * given node has two children
	 * </ul>
	 *
	 * @param node The given node
	 * @return The node's successor, if the node has two children, and null,
	 * otherwise
	 */
	final private AVLMapNode acquireTreeLocks(final AVLMapNode node) {
		while (true) {
			node.lockTreeLock();
			final AVLMapNode right = node.right;
			final AVLMapNode left = node.left;
			if (right == null || left == null) {
				if (right != null && !right.tryLockTreeLock()) {
					node.unlockTreeLock();
					Thread.yield();
					continue;
				}
				if (left != null && !left.tryLockTreeLock()) {
					node.unlockTreeLock();
					Thread.yield();
					continue;
				}
				return null;
			}

			final AVLMapNode successor = node.succ;

			final AVLMapNode parent = successor.parent;
			if (parent != node) {
				if (!parent.tryLockTreeLock()) {
					node.unlockTreeLock();
					Thread.yield();
					continue;
				} else if (parent != successor.parent || !parent.valid) {
					parent.unlockTreeLock();
					node.unlockTreeLock();
					Thread.yield();
					continue;
				}
			}
			if (!successor.tryLockTreeLock()) {
				node.unlockTreeLock();
				if (parent != node) parent.unlockTreeLock();
				Thread.yield();
				continue;
			}
			final AVLMapNode succRightChild = successor.right; // there is no left child to the successor, perhaps there is a right one, which we need to lock.
			if (succRightChild != null && !succRightChild.tryLockTreeLock()) {
				node.unlockTreeLock();
				successor.unlockTreeLock();
				if (parent != node) parent.unlockTreeLock();
				Thread.yield();
				continue;
			}
			return successor;
		}
	}

	/**
	 * Removes the given node from the tree layout.
	 * If the node has less than two children, its successor, {@code succ}, is
	 * null, and the removal is applied by connecting the node's parent to the
	 * node's child. Otherwise, the successor is relocated to the node's location.
	 *
	 * @param node The node to remove
	 * @param succ The node's successor
	 * @param parent The node's parent
	 */
	private void removeFromTree(AVLMapNode node, AVLMapNode succ,
			AVLMapNode parent) {
		if (succ == null) {
			AVLMapNode right = node.right;
			final AVLMapNode child = right == null ? node.left : right;
			boolean left = updateChild(parent, node, child);
			node.unlockTreeLock();
			rebalance(parent,  child, left);
			return;
		}
		AVLMapNode oldParent = succ.parent;
		AVLMapNode oldRight = succ.right;
		updateChild(oldParent, succ, oldRight);

		succ.leftHeight = node.leftHeight;
		succ.rightHeight = node.rightHeight;
		AVLMapNode left = node.left;
		AVLMapNode right = node.right;
		succ.parent = parent;
		succ.left = left;
		succ.right = right;
		left.parent = succ;
		if (right != null) {
			right.parent = succ;
		}
		if (parent.left == node) {
			parent.left = succ;
		} else {
			parent.right = succ;
		}
		boolean isLeft = oldParent != node;
		boolean violated = Math.abs(succ.getBalanceFactor()) >= 2;
		if (!isLeft) {
			oldParent = succ;
		} else {
			succ.unlockTreeLock();
		}
		node.unlockTreeLock();
		parent.unlockTreeLock();
		rebalance(oldParent, oldRight, isLeft);

		if (violated) {
			succ.lockTreeLock();
			int bf = succ.getBalanceFactor();
			if (succ.valid && Math.abs(bf) >=2) {
				rebalance(succ, null, bf >=2? false: true);
			} else {
				succ.unlockTreeLock();
			}
		}
	}

	/**
	 * Given a node, {@code parent}, its old child and a new child, update the
	 * old child with the new one.
	 *
	 * @param parent The node
	 * @param oldChild The old child
	 * @param newChild The new child
	 * @return true if the old child was a left child
	 */
	private boolean updateChild(AVLMapNode parent, AVLMapNode oldChild,
			final AVLMapNode newChild) {
		if (newChild != null) {
			newChild.parent = parent;
		}
		boolean left = parent.left == oldChild;
		if (left) {
			parent.left = newChild;
		} else {
			parent.right = newChild;
		}
		return left;
	}

	/**
	 * Rebalance the tree.
	 * The rebalance is done by traversing the tree (starting from the given
	 * node) and applying rotations when detecting imbalanced nodes.
	 *
	 * @param node The node to begin the traversal from
	 * @param child The node's child
	 * @param isLeft Is the given child a left child?
	 */
	final private void rebalance(AVLMapNode node, AVLMapNode child, boolean isLeft) {
		if (node == root) {
			node.unlockTreeLock();
			if (child != null) child.unlockTreeLock();
			return;
		}
		AVLMapNode parent = null;
		try {
			while (node != root) {
				boolean updateHeight = updateHeight(child, node, isLeft);
				int bf = node.getBalanceFactor();
				if (!updateHeight && Math.abs(bf) < 2) return;
				while (bf >= 2 || bf <= -2) {
					if ((isLeft && bf <= -2) || (!isLeft && bf >= 2)) {
						if (child != null) child.unlockTreeLock();
						child = isLeft? node.right : node.left;
						if (!child.tryLockTreeLock()) {
							child = restart(node, parent);
							if (!node.treeLock.isHeldByCurrentThread()) {
								return;
							}
							parent = null;
							isLeft = node.left == child;
							bf = node.getBalanceFactor();
							continue;
						}
						isLeft = !isLeft;
					}
					if ((isLeft && child.getBalanceFactor() < 0) || (!isLeft && child.getBalanceFactor() > 0)) {
						AVLMapNode grandChild =  isLeft? child.right : child.left;
						if (!grandChild.tryLockTreeLock()) {
							child.unlockTreeLock();
							child = restart(node, parent);
							if (!node.treeLock.isHeldByCurrentThread()) {
								return;
							}
							parent = null;
							isLeft = node.left == child;
							bf = node.getBalanceFactor();
							continue;
						}
						rotate(grandChild, child, node, isLeft);
						child.unlockTreeLock();
						child = grandChild;
					}
					if (parent == null) {
						parent = lockParent(node);
					}
					rotate(child,  node, parent, !isLeft);
					bf = node.getBalanceFactor();
					if (bf >= 2 || bf <= -2) {
						parent.unlockTreeLock();
						parent = child;
						child = null;
						isLeft = bf >= 2? false: true; // enforces to lock child
						continue;
					}
					AVLMapNode temp = child;
					child = node;
					node = temp;
					isLeft = node.left == child;
					bf = node.getBalanceFactor();
				}
				if (child != null) {
					child.unlockTreeLock();
				}
				child = node;
				node = parent != null && parent.treeLock.isHeldByCurrentThread()? parent: lockParent(node);
				isLeft = node.left == child;
				parent = null;
			}
		} finally {
			if (child != null && child.treeLock.isHeldByCurrentThread()) {
				child.unlockTreeLock();
			}
			if (node.treeLock.isHeldByCurrentThread()) node.unlockTreeLock();
			if (parent != null && parent.treeLock.isHeldByCurrentThread()) parent.unlockTreeLock();
		}
	}

	/**
	 * Release all current treeLocks (of the given node and its parent), and
	 * re-acquire the treeLocks of node and its child.
	 *
	 * @param node The node
	 * @param parent The node's parent
	 *
	 * @return The node's (locked) child
	 */
	final private AVLMapNode restart(AVLMapNode node, AVLMapNode parent) {
		if (parent != null) {
			parent.unlockTreeLock();
		}
		node.unlockTreeLock();
		Thread.yield();
		while (true) {
			node.lockTreeLock();
			if (!node.valid) {
				node.unlockTreeLock();
				return null;
			}
			AVLMapNode child = node.getBalanceFactor() >= 2? node.left : node.right;
			if (child == null) return null;
			if (child.tryLockTreeLock()) return child;
			node.unlockTreeLock();
			Thread.yield();
		}
	}

	/**
	 * Update the height of the given node, based on the given child.
	 *
	 * @param child The node's child
	 * @param node The node
	 * @param isLeft Is the child a left child?
	 *
	 * @return true if the height was updated, and false otherwise
	 */
	final private boolean updateHeight(AVLMapNode child, AVLMapNode node, boolean isLeft) {
		int newHeight = child == null? 0: Math.max(child.leftHeight, child.rightHeight) + 1;
		int oldHeight = isLeft? node.leftHeight : node.rightHeight;
		if (newHeight == oldHeight) return false;
		if (isLeft) {
			node.leftHeight = newHeight;
		} else {
			node.rightHeight = newHeight;
		}
		return true;
	}

	/**
	 * Apply a single rotation to the given node.
	 *
	 * @param child The node's child
	 * @param node The node to rotate
	 * @param parent The node's parent
	 * @param left Is this a left rotation?
	 */
	final private void rotate(final AVLMapNode child, final AVLMapNode node, final AVLMapNode parent, boolean left) {
		if (parent.left == node) {
			parent.left = child;
		} else {
			parent.right = child;
		}
		child.parent = parent;
		node.parent = child;
		AVLMapNode grandChild = left? child.left : child.right;
		if (left) {
			node.right = grandChild;
			if (grandChild != null) {
				grandChild.parent = node;
			}
			child.left = node;
			node.rightHeight = child.leftHeight;
			child.leftHeight = Math.max(node.leftHeight, node.rightHeight) + 1;
		} else {
			node.left = grandChild;
			if (grandChild != null) {
				grandChild.parent = node;
			}
			child.right = node;
			node.leftHeight = child.rightHeight;
			child.rightHeight = Math.max(node.leftHeight, node.rightHeight) + 1;
		}
	}

	/**
	 * @see java.util.AbstractMap#clear()
	 */
	@Override
	public void clear() {
		root.parent.lockSuccLock();
		root.lockTreeLock();
		root.parent.succ = root;
		root.pred = root.parent;
		root.left = null;
		root.leftHeight = 1;
		root.parent.unlockSuccLock();
		root.unlockTreeLock();
	}

	/**
	 * @return The height of the tree
	 */
	final public int height() {
		return height(root.left);
	}

	/**
	 * Returns the height of the sub-tree rooted at the given node.
	 *
	 * @param node The given node
	 * @return The height of the sub-tree rooted by node
	 */
	final public int height(AVLMapNode node) {
		if (node == null) return 0;
		int rMax = height(node.right);
		int lMax = height(node.left);
		return Math.max(rMax, lMax) + 1;
	}

	/**
	 * @see java.util.AbstractMap#size()
	 */
	@Override
	final public int size() {
		return size(root.left);
	}

	/**
	 * Returns the number of nodes in the sub-tree rooted at the given node.
	 *
	 * @param node The given node
	 * @return The number of nodes in the sub-tree rooted at node
	 */
	final public int size(AVLMapNode node) {
		if (node == null) return 0;
		int rMax = size(node.right);
		int lMax = size(node.left);
		return rMax+lMax + 1;
	}

	/**
	 * The tree is empty if the root's left child is empty
	 * @see java.util.AbstractMap#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return root.left == null;
	}

	/**
	 * A tree node
	 *
	 * @author Dana
	 *
	 */
	class AVLMapNode {

		/** The node's key. */
		public final int key;

		/** Is the node valid? i.e. it was not marked as removed. */
		public volatile boolean valid;

		/** The predecessor of the node (with respect to the ordering layout). */
		public volatile AVLMapNode pred;

		/** The successor of the node (with respect to the ordering layout). */
		public volatile AVLMapNode succ;

		/** The lock that protects the node's {@code succ} field and the {@code pred} field of the node pointed by {@code succ}. */
		final public Lock succLock;

		/** The parent of the node (with respect to the tree layout). */
		public volatile AVLMapNode parent;

		/** The left child of the node (with respect to the tree layout). */
		public volatile AVLMapNode left;

		/** The right child of the node (with respect to the tree layout). */
		public volatile AVLMapNode right;

		/** The height of the sub-tree rooted at {@code left}. */
		public int leftHeight;

		/** The height of the sub-tree rooted at {@code right}. */
		public int rightHeight;

		/** The lock that protects the node's tree fields, that is, {@code parent, left, right, leftHeight, rightHeight}. */
		final public ReentrantLock treeLock;

		/**
		 * Constructor, create a new node.
		 *
		 * @param key The new node's key
		 * @param pred The new node's predecessor (with respect to the ordering layout)
		 * @param succ The new node's successor (with respect to the ordering layout)
		 * @param parent The new node's parent (with respect to the tree layout)
		 */
		public AVLMapNode(final int key, final AVLMapNode pred, final AVLMapNode succ, final AVLMapNode parent) {
			this.key = key;
			valid = true;

			this.pred = pred;
			this.succ = succ;
			succLock = new ReentrantLock();

			this.parent = parent;
			right = null;
			left = null;
			leftHeight = 0;
			rightHeight = 0;
			treeLock = new ReentrantLock();
		}

		/**
		 * Constructor, create a new node with the given key.
		 *
		 * @param key The new node's key
		 */
		public AVLMapNode(int key) {
			this(key, null, null, null);
		}


		/**
		 * Lock the node's {@code treeLock}.
		 */
		public void lockTreeLock() {
			treeLock.lock();
		}

		/**
		 * Attempt to lock the node's {@code treeLock} without blocking.
		 *
		 * @return true if the lock was acquired, and false otherwise
		 */
		public boolean tryLockTreeLock() {
			return treeLock.tryLock();
		}

		/**
		 * Release the node's {@code treeLock}.
		 */
		public void unlockTreeLock() {
			treeLock.unlock();
		}

		/**
		 * Returns the balance factor of the node, that is, the difference
		 * between the heights of the left sub-tree and the right sub-tree.
		 *
		 * @return the node's balance factor
		 */
		public int getBalanceFactor() {
			return leftHeight - rightHeight;
		}

		/**
		 * Lock the node's {@code succLock}.
		 */
		public void lockSuccLock() {
			succLock.lock();
		}

		/**
		 * Release the node's {@code succLock}.
		 */
		public void unlockSuccLock() {
			succLock.unlock();
		}

		/**
		 * @see Object#toString()
		 */
		@Override
		public String toString() {
			String delimiter = "  ";
			StringBuilder sb = new StringBuilder();

			sb.append("(" + key + delimiter + ", " + valid + ")" + delimiter);

			return sb.append(" [" + leftHeight + ":" + rightHeight + "]").toString();
		}
	}
}
