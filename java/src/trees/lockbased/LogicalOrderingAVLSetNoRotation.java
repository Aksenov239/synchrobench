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
 * <p>
 * Copyright 2013 Dana Drachsler (ddana [at] cs [dot] technion [dot] ac [dot] il).
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author Dana Drachsler
 */
public class LogicalOrderingAVLSetNoRotation extends AbstractCompositionalIntSet {

    /**
     * The tree's root
     */
    private AVLMapNode root;

    /**
     * A constant object for the use of the {@code insert} method.
     */
    private final static Object EMPTY_ITEM = new Object();


    public LogicalOrderingAVLSetNoRotation() {
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
    public LogicalOrderingAVLSetNoRotation(final int min, final int max) {
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
    final public boolean containsInt(int key) {
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
        while (res < 0) {
            node = node.pred;
            val = node.key;
            res = Integer.compare(key, val);
        }
        while (res > 0) {
            node = node.succ;
            val = node.key;
            res = Integer.compare(key, val);
        }
        return (res == 0 && node.valid);
    }

    /**
     * Insert the pair (key, item) to the tree.
     * If the key is already present, update the item if putIfAbsent equals {@code false}.
     * If {@code isReplace} equals {@code true}, the operation takes place only
     * if the key is already present. Before applying the replacement, the
     * operation considers the {@code replaceItem}. If this item equals
     * {@code EmptyItem}, the replacement is applied without considering the
     * current item associated with that key. Otherwise, the replacement is
     * applied only if the current item equals to {@code replaceItem}.
     *
     * @param key         The key
     * @return The item that was associated with the given key, or null if the
     * key was not present in the tree
     */
    final public boolean addInt(final int key) {
        AVLMapNode node = null;
        int nodeValue = 0;
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
                final int predRes = pred == node ? res : Integer.compare(key, predVal);
                if (predRes > 0) {
                    final AVLMapNode succ = pred.succ;
                    final int succVal = succ.key;
                    final int res2 = succ == node ? res : Integer.compare(key, succVal);
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
     * @param pred      The predecessor
     * @param succ      The successor
     * @param firstCand The node returned from the traversal
     * @return The correct parent
     */
    final private AVLMapNode chooseParent(final AVLMapNode pred,
                                                final AVLMapNode succ, final AVLMapNode firstCand) {
        AVLMapNode candidate = firstCand == pred || firstCand == succ ? firstCand : pred;
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
     * @param parent  The new node's parent
     * @param newNode The new node
     * @param isRight Is the new node should be the parent's right child?
     */
    final private void insertToTree(final AVLMapNode parent, final AVLMapNode newNode, final boolean isRight) {
        if (isRight) {
            parent.right = newNode;
        } else {
            parent.left = newNode;
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
     * Remove the given key from the tree.
     * If the flag {@code compareItem} equals true, remove the key only if the
     * node is associated with the given item.
     *
     * @param key         The key to remove
     * @return The item of the node that was removed, or null if no node was
     * removed
     */
    final public boolean removeInt(final int key) {
        AVLMapNode pred, node = null;
        int nodeValue = 0;
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
                final int predRes = pred == node ? res : Integer.compare(key, predVal);
                if (predRes > 0) {
                    AVLMapNode succ = pred.succ;
                    final int succVal = succ.key;
                    int res2 = succ == node ? res : Integer.compare(key, succVal);
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
     * @param node   The node to remove
     * @param succ   The node's successor
     * @param parent The node's parent
     */
    private void removeFromTree(AVLMapNode node, AVLMapNode succ,
                                AVLMapNode parent) {
        if (succ == null) {
            AVLMapNode right = node.right;
            final AVLMapNode child = right == null ? node.left : right;
            boolean left = updateChild(parent, node, child);
            node.unlockTreeLock();
            rebalance(parent, child, left);
            return;
        }
        AVLMapNode oldParent = succ.parent;
        AVLMapNode oldRight = succ.right;
        updateChild(oldParent, succ, oldRight);

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
        if (!isLeft) {
            oldParent = succ;
        } else {
            succ.unlockTreeLock();
        }
        node.unlockTreeLock();
        parent.unlockTreeLock();
        rebalance(oldParent, oldRight, isLeft);
    }

    /**
     * Given a node, {@code parent}, its old child and a new child, update the
     * old child with the new one.
     *
     * @param parent   The node
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
     * @param node   The node to begin the traversal from
     * @param child  The node's child
     * @param isLeft Is the given child a left child?
     */
    final private void rebalance(AVLMapNode node, AVLMapNode child, boolean isLeft) {
        if (node == root) {
            node.unlockTreeLock();
            if (child != null) child.unlockTreeLock();
            return;
        }
        if (child != null && child.treeLock.isHeldByCurrentThread()) {
            child.unlockTreeLock();
        }
        if (node.treeLock.isHeldByCurrentThread()) node.unlockTreeLock();
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
        return rMax + lMax + 1;
    }

    /**
     * The tree is empty if the root's left child is empty
     *
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
     */
    class AVLMapNode {

        /**
         * The node's key.
         */
        public final int key;

        /**
         * Is the node valid? i.e. it was not marked as removed.
         */
        public volatile boolean valid;

        /**
         * The predecessor of the node (with respect to the ordering layout).
         */
        public volatile AVLMapNode pred;

        /**
         * The successor of the node (with respect to the ordering layout).
         */
        public volatile AVLMapNode succ;

        /**
         * The lock that protects the node's {@code succ} field and the {@code pred} field of the node pointed by {@code succ}.
         */
        final public Lock succLock;

        /**
         * The parent of the node (with respect to the tree layout).
         */
        public volatile AVLMapNode parent;

        /**
         * The left child of the node (with respect to the tree layout).
         */
        public volatile AVLMapNode left;

        /**
         * The right child of the node (with respect to the tree layout).
         */
        public volatile AVLMapNode right;

        /**
         * The lock that protects the node's tree fields, that is, {@code parent, left, right, leftHeight, rightHeight}.
         */
        final public ReentrantLock treeLock;

        /**
         * Constructor, create a new node.
         *
         * @param key    The new node's key
         * @param pred   The new node's predecessor (with respect to the ordering layout)
         * @param succ   The new node's successor (with respect to the ordering layout)
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

            return sb.toString();
        }
    }
}
