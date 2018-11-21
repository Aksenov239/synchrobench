package trees.kset;

import contention.abstractions.AbstractCompositionalIntSet;
import contention.abstractions.MaintenanceAlg;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * User: Aksenov Vitaly
 * Date: 23.10.2018
 * Time: 12:38
 */
public class KSetAVL8 extends AbstractCompositionalIntSet implements MaintenanceAlg {
    public static final int K = 8;

    public static final int EMPTY = Integer.MIN_VALUE + 1;

    public class Node {
        ReentrantLock treeLock = new ReentrantLock();
        volatile Node l;
        volatile Node r;
        volatile Node p;
        volatile int leftHeight;
        volatile int rightHeight;

        ReentrantLock lock = new ReentrantLock();
        volatile Node prev;
        volatile Node succ;

        volatile int min;
        AtomicIntegerArray values = new AtomicIntegerArray(K);

        volatile boolean deleted = false;

        public Node() {
            for (int i = 0; i < K; i++) {
                values.set(i, EMPTY);
            }
        }

        public int balance() {
            return rightHeight - leftHeight;
        }
    }

    Node root;

    public Node traverse(int v) {
        Node curr = root;
        Node last = null;
        while (curr != null) {
            if (curr.min > v) {
                curr = curr.l;
            } else {
                last = curr;
                curr = curr.r;
            }
        }
        return last;
    }

    public Node adjustToLeft(int v, Node curr) {
        while (curr.min <= v) {
            curr = curr.succ;
        }
        while (curr != null && curr.min > v) {
            curr = curr.prev;
        }
        return curr;
    }

    public Node blockingAdjustment(int v, Node curr) {
        curr.lock.lock();
        while (curr.min > v || curr.deleted) {
            Node next = curr.prev;
            next.lock.lock();
            curr.lock.unlock();
            curr = next;
        }
        assert !curr.deleted && curr.succ.min > v && curr.min <= v && !curr.succ.deleted;
        return curr;
    }

    public void verify(Node node) {
        assert !node.lock.isLocked();
        if (node.l != null) {
            assert node.l.p == node;
            verify(node.l);
        }
        if (node.r != null) {
            assert node.r.p == node;
            verify(node.r);
        }
    }

    private Node lockParent(Node node) {
        while (!node.deleted) {
            Node parent = node.p;
            parent.treeLock.lock();
            if (parent.deleted) {
                parent.treeLock.unlock();
                continue;
            }
            if (node.min < parent.min) {
                if (parent.l != node) {
                    parent.treeLock.unlock();
                    continue;
                }
            } else {
                if (parent.r != node) {
                    parent.treeLock.unlock();
                    continue;
                }
            }
            return parent;
        }
        return null;
    }

    public void checkHeight(Node curr) {
        assert curr.leftHeight == (curr.l == null ? 0 : Math.max(curr.l.leftHeight, curr.l.rightHeight) + 1);
        assert curr.rightHeight == (curr.r == null ? 0 : Math.max(curr.r.leftHeight, curr.r.rightHeight) + 1);
        assert curr.p == null || curr.p.l == curr || curr.p.r == curr;
    }

    public void allCheckHeight(Node curr) {
        if (curr == null)
            return;
        checkHeight(curr);
    }

    public void recalcHeight(Node node, boolean isLeft) {
        Node child = isLeft ? node.l : node.r;
        if (isLeft) {
            node.leftHeight = child == null ? 0 : Math.max(child.leftHeight, child.rightHeight) + 1;
        } else {
            node.rightHeight = child == null ? 0 : Math.max(child.leftHeight, child.rightHeight) + 1;
        }
    }

    public void updateParent(Node parent, Node oldNode, Node newNode) {
        if (oldNode.min < parent.min) {
            parent.l = newNode;
        } else {
            parent.r = newNode;
        }
    }

    public void rebalance(Node node) {
        while (node != root) {
            Node parent = lockParent(node);
            if (parent == null) {
                return;
            }
            node.treeLock.lock();
            int balance = node.balance();
            while (balance <= -2 || 2 <= balance) {
                if (balance >= 2) {
                    node.r.treeLock.lock();

                    Node child = node.r;

                    if (child.balance() >= 0) {
                        if (child.l != null) {
                            child.l.p = node;
                        }
                        node.r = child.l;
                        node.p = child;
                        child.l = node;
                        child.p = parent;
                        updateParent(parent, node, child);

                        recalcHeight(node, false);
                        recalcHeight(child, true);

//                        checkHeight(node);
//                        checkHeight(child);

                        node.treeLock.unlock();
                        node = child;
                    } else {
                        Node grandChild = child.l;
                        grandChild.treeLock.lock();
                        if (grandChild.l != null) {
                            grandChild.l.p = node;
                        }
                        node.r = grandChild.l;
                        if (grandChild.r != null) {
                            grandChild.r.p = child;
                        }
                        child.l = grandChild.r;
                        node.p = grandChild;
                        grandChild.l = node;
                        child.p = grandChild;
                        grandChild.r = child;

                        grandChild.p = parent;
                        updateParent(parent, node, grandChild);

                        recalcHeight(node, false);
                        recalcHeight(child, true);
                        recalcHeight(grandChild, true);
                        recalcHeight(grandChild, false);
//                        checkHeight(node);
//                        checkHeight(child);
//                        checkHeight(grandChild);

                        child.treeLock.unlock();
                        node.treeLock.unlock();
                        node = grandChild;
                    }
                } else {
                    node.l.treeLock.lock();

                    Node child = node.l;

                    if (child.balance() <= 0) {
                        if (child.r != null) {
                            child.r.p = node;
                        }
                        node.l = child.r;
                        node.p = child;
                        child.r = node;
                        child.p = parent;
                        updateParent(parent, node, child);

                        recalcHeight(node, true);
                        recalcHeight(child, false);

//                        checkHeight(node);
//                        checkHeight(child);

                        node.treeLock.unlock();
                        node = child;
                    } else {
                        Node grandChild = child.r;
                        grandChild.treeLock.lock();
                        if (grandChild.r != null) {
                            grandChild.r.p = node;
                        }
                        node.l = grandChild.r;
                        if (grandChild.l != null) {
                            grandChild.l.p = child;
                        }
                        child.r = grandChild.l;
                        node.p = grandChild;
                        grandChild.r = node;
                        child.p = grandChild;
                        grandChild.l = child;

                        grandChild.p = parent;
                        updateParent(parent, node, grandChild);

                        recalcHeight(node, true);
                        recalcHeight(child, false);
                        recalcHeight(grandChild, true);
                        recalcHeight(grandChild, false);

//                        checkHeight(node);
//                        checkHeight(child);
//                        checkHeight(grandChild);

                        child.treeLock.unlock();
                        node.treeLock.unlock();
                        node = grandChild;
                    }
                }

                balance = node.balance();
            }

            boolean left = node.min < parent.min;
            int oldHeight = left ? parent.leftHeight : parent.rightHeight;
            recalcHeight(parent, left);
            int newHeight = left ? parent.leftHeight : parent.rightHeight;
//            checkHeight(parent);

            boolean stop = oldHeight == newHeight && Math.abs(parent.balance()) < 2;

            Node oldNode = node;
            node = parent;
            oldNode.treeLock.unlock();
            parent.treeLock.unlock();

            if (stop) {
                break;
            }
        }
    }

    public boolean addInt(int v) {
//        System.err.println("Insert " + v);
        Node curr = traverse(v);
        curr = adjustToLeft(v, curr);
        curr = blockingAdjustment(v, curr);

        assert !curr.deleted;

        int emptySlot = -1;
        for (int i = 0; i < K; i++) {
            if (curr.values.get(i) == v) {
                curr.lock.unlock();
                return false;
            }
            if (curr.values.get(i) == EMPTY) {
                emptySlot = i;
            }
        }

        if (emptySlot != -1) {
            curr.values.set(emptySlot, v);
            curr.lock.unlock();
//            System.err.println(toString());
            return true;
        }

//        System.err.println("Block insert");

        Node prev = curr.prev;
        while (true) {
            prev.lock.lock();
            if (prev.deleted) {
                prev.lock.unlock();
                prev = curr.prev;
            } else {
                break;
            }
        }
        assert !prev.deleted;

        int[] copy = new int[K];
        for (int i = 0; i < K; i++) {
            copy[i] = curr.values.get(i);
        }
        Arrays.sort(copy);
        Node newNode = new Node();
//        newNode.lock.lock();
        int m = copy.length / 2;
        for (int i = 0; i < m; i++) {
            newNode.values.set(i, copy[i]);
        }

        if (copy[m] > v) {
            newNode.values.set(m, v);
        }

        newNode.min = copy[0];
        newNode.prev = prev;
        newNode.succ = curr;

        prev.succ = newNode;
        curr.prev = newNode;

        curr.min = copy[m];
        for (int i = 0; i < K; i++) {
            if (curr.values.get(i) < copy[m]) {
                curr.values.set(i, EMPTY);
            }
        }

        if (curr.min < v) {
            for (int i = 0; i < K; i++) {
                if (curr.values.get(i) == EMPTY) {
                    curr.values.set(i, v);
                    break;
                }
            }
        }

        Node parent = chooseParent(curr, prev);

        if (parent.min > copy[0]) {
            newNode.p = parent;
            parent.l = newNode;
            parent.leftHeight = 1;
        } else {
            assert parent.r == null;
            newNode.p = parent;
            parent.r = newNode;
            parent.rightHeight = 1;
        }

//        checkHeight(parent);

        parent.treeLock.unlock();

        prev.lock.unlock();
//        newNode.lock.unlock();
        curr.lock.unlock();

        rebalance(parent);

//        verify(root);
//        System.err.println(toString());

//        allCheckHeight(root);

        return true;
    }

    public Node chooseParent(Node curr, Node prev) {
        while (true) {
            if (curr.l == null) {
                curr.treeLock.lock();
                if (curr.l != null) {
                    curr.treeLock.unlock();
                } else {
                    return curr;
                }
            } else {
                prev.treeLock.lock();
                if (prev.r != null) {
                    prev.treeLock.unlock();
                } else {
                    return prev;
                }
            }
        }
    }

    public boolean removeInt(int v) {
//        System.err.println("Remove " + v);
        Node curr = traverse(v);
        curr = adjustToLeft(v, curr);
        curr = blockingAdjustment(v, curr);

        int nonEmpty = 0;
        boolean found = false;
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < K; i++) {
            int value = curr.values.get(i);
            if (value == v) {
                curr.values.set(i, EMPTY);
                found = true;
            } else {
                if (value != EMPTY) {
                    min = Math.min(min, value);
                    nonEmpty++;
                }
            }
        }
        if (!found) {
            curr.lock.unlock();
            return false;
        }

        if (nonEmpty != 0) {
            curr.min = min;
            curr.lock.unlock();
            return true;
        }

//        System.err.println("Block remove");

        Node prev = curr.prev;
        while (true) {
            prev.lock.lock();
            if (prev.deleted) {
                prev.lock.unlock();
                prev = curr.prev;
            } else {
                break;
            }
        }

        Node parent = lockParent(curr);

        assert curr.p == parent;

        curr.treeLock.lock();

//        if (curr.l != null) {
//            curr.l.treeLock.lock();
//        }
        Node left = curr.l;
        if (left != null) assert left.p == curr;
//        if (curr.r != null) {
//            curr.r.treeLock.lock();
//        }
        Node right = curr.r;
        if (right != null) assert right.p == curr;

        curr.deleted = true;

        prev.succ = curr.succ;
        curr.succ.prev = prev;

        Node toRebalance = null;

        if (left == null || right == null) {
            if (parent.l == curr) {
                parent.l = left == null ? right : left;
                recalcHeight(parent, true);
            } else {
                parent.r = left == null ? right : left;
                recalcHeight(parent, false);
            }
            if (left != null) {
                left.p = parent;
            }
            if (right != null) {
                right.p = parent;
            }
//            if (left != null) {
//                left.treeLock.unlock();
//            }
//            if (right != null) {
//                right.treeLock.unlock();
//            }

            toRebalance = parent;
//            checkHeight(parent);

            curr.treeLock.unlock();
            parent.treeLock.unlock();
        } else {
            Node sp = prev.p;
//            while ((sp = prev.p) == null) {}
            while (true) {
                sp.treeLock.lock();
                if ((sp.l != prev && sp.r != prev) || (sp.deleted && sp != curr)) {
                    sp.treeLock.unlock();
                    sp = prev.p;
                } else {
                    break;
                }
            }

            prev.treeLock.lock();

            Node sl = prev.l;
//            if (sl != null && sl != curr) {
//                sl.treeLock.lock();
//            }
            assert prev.r == null;

            if (sp.l == prev) {
                sp.l = sl;
                recalcHeight(sp, true);
            } else {
                sp.r = sl;
                recalcHeight(sp, false);
            }
            if (sl != null) {
                sl.p = sp;
            }

            prev.l = curr.l;
            prev.r = curr.r;
            prev.p = curr.p;
            prev.leftHeight = curr.leftHeight;
            prev.rightHeight = curr.rightHeight;

            if (parent.l == curr) {
                parent.l = prev;
            } else {
                parent.r = prev;
            }
            if (curr.l != null) {
                prev.l.p = prev;
            }
            if (curr.r.p != null) {
                prev.r.p = prev;
            }

//            if (sl != null) {
//                sl.treeLock.unlock();
//            }
            prev.treeLock.unlock();

            toRebalance = sp;
//            if (!sp.deleted) {
//                checkHeight(sp);
//            }
//            checkHeight(parent);
//            checkHeight(prev);

            sp.treeLock.unlock();

//            if (left != sp && left != sl) {
//            left.treeLock.unlock();
//            }
//            if (right != sp) {
//            right.treeLock.unlock();
//            }
//            if (curr != sp && curr != sl) {
            curr.treeLock.unlock();
//            }
            parent.treeLock.unlock();
        }

        prev.lock.unlock();
        curr.lock.unlock();

//        verify(root);
//        System.err.println(toString());

        rebalance(toRebalance);

//        allCheckHeight(root);

        return true;
    }

    public boolean containsInt(int v) {
        Node curr = traverse(v);
        curr = adjustToLeft(v, curr);

        while (true) {
            if (curr.min <= v) {
                for (int i = 0; i < K; i++) {
                    if (curr.values.get(i) == v && !curr.deleted) {
                        return true;
                    }
                }
                if (curr.min <= v && !curr.deleted) {
                    return false;
                }
            }
            curr = curr.prev;
        }
    }

    public int size() {
        Node curr = root;
        int total = 0;
        while (curr != null) {
            for (int i = 0; i < K; i++) {
                if (curr.values.get(i) > EMPTY) {
                    total++;
                }
            }
            curr = curr.succ;
        }
        return total - 1;
    }

    public void clear() {
        root = new Node();
        root.values.set(0, Integer.MIN_VALUE);
        root.min = Integer.MIN_VALUE;
        root.rightHeight = 2;

        Node mid = new Node();
        mid.values.set(0, Integer.MIN_VALUE);
        mid.min = Integer.MIN_VALUE;
        mid.p = root;
        root.r = mid;
        root.succ = mid;
        mid.prev = root;
        mid.rightHeight = 1;

        Node max = new Node();
        max.values.set(0, Integer.MAX_VALUE);
        max.min = Integer.MAX_VALUE;
        max.p = mid;
        mid.r = max;
        mid.succ = max;
        max.prev = mid;
    }

    public String toString() {
        String res = "";
        Node curr = root;
        while (curr != null) {
            res += (curr != root ? " -> " : "") + "(" + curr.min + "," + curr.values + "," + curr.lock.isLocked() + ")";
            curr = curr.succ;
        }
        return res;
    }

    public KSetAVL8() {
        clear();
    }

    public long getStructMods() {
        return 0;
    }

    public int height(Node node) {
        if (node == null)
            return 0;
        return Math.max(height(node.l), height(node.r)) + 1;
    }

    public boolean stopMaintenance() {
        System.out.println("Height of the tree: " + height(root));
        return true;
    }

    public int numNodes() {
        return size();
    }
}
