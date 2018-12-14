package trees.lockfree;

import contention.abstractions.CompositionalMap;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * User: Aksenov Vitaly
 * Date: 13.12.2018
 * Time: 18:05
 */
public class LockFreeCASNAVL<K, V> extends AbstractMap<K, V>
        implements CompositionalMap<K, V> {
    public class Node<K, V> {
        final K key;
        final V value;
        volatile int height;
        volatile Object l, r;

        volatile Object version;

        public Node(K key, V value, int height, Object l, Object r) {
            this.key = key;
            this.value = value;
            this.height = 0;
            this.l = l;
            this.r = r;
            version = new Integer(1);
        }

        public Node<K, V> getLeft() {
            Object left = l;
            while (left != null && left instanceof Descriptor) {
                CASN((Descriptor) left);
                left = l;
            }
            return (Node<K, V>) left;
        }

        public Node<K, V> getRight() {
            Object right = r;
            while (right != null && right instanceof Descriptor) {
                CASN((Descriptor) right);
                right = r;
            }
            return (Node<K, V>) right;
        }

        public Integer getVersion() {
            Object version = this.version;
            while (version instanceof Descriptor) {
                CASN((Descriptor) version);
                version = this.version;
            }
            return (Integer) version;
        }
    }

    public enum Status {
        UNDECIDED,
        FAILED,
        FINISHED
    }

    public class Descriptor<K, V> {
        volatile Node<K, V>[] toRemove;
        volatile Integer[] versions;
        volatile Node<K, V> parent;
        volatile Node<K, V> child;
        volatile Node<K, V> newChild;
        volatile Integer parentVersion;
        volatile Status status;

        public Descriptor(Node<K, V> parent, Node<K, V> child, Node<K, V> newChild, Integer parentVersion,
                          Node<K, V>[] toRemove, Integer[] versions) {
            this.status = Status.UNDECIDED;
            this.parent = parent;
            this.child = child;
            this.newChild = newChild;
            this.parentVersion = parentVersion;
            this.toRemove = toRemove;
            this.versions = versions;
        }
    }

    private Comparator<? super K> comparator;
    private AtomicReferenceFieldUpdater<Node, Object> updateLeft, updateRight, updateVersion;
    private AtomicReferenceFieldUpdater<Descriptor, Status> updateDescriptorStatus;
    private Node<K, V> root;

    public LockFreeCASNAVL() {
        updateLeft = AtomicReferenceFieldUpdater.newUpdater(Node.class, Object.class, "l");
        updateRight = AtomicReferenceFieldUpdater.newUpdater(Node.class, Object.class, "r");
        updateVersion = AtomicReferenceFieldUpdater.newUpdater(Node.class, Object.class, "version");
        updateDescriptorStatus = AtomicReferenceFieldUpdater.newUpdater(Descriptor.class, Status.class, "status");
        clear();
    }

    public LockFreeCASNAVL(Comparator<? super K> comparator) {
        this();
        this.comparator = comparator;
    }

    private Comparable<? super K> comparable(final Object key) {
        if (key == null) {
            throw new NullPointerException();
        }
        if (comparator == null) {
            return (Comparable<? super K>) key;
        }
        return new Comparable<K>() {

            final Comparator<? super K> _cmp = comparator;

            @SuppressWarnings("unchecked")
            public int compareTo(final K rhs) {
                return _cmp.compare((K) key, rhs);
            }
        };
    }

    public int compare(final K k1, final K k2) {
        if (comparator == null) {
            return k1 == null || k2 == null ? -1 : ((Comparable<? super K>) k1).compareTo(k2);
        }
        return comparator.compare(k1, k2);
    }

    public boolean containsKey(final Object key) {
        return get(key) != null;
    }

    public final V get(final Object key) {
        final Comparable<? super K> k = comparable(key);

        Node<K, V> node = root;
        while (node.l != null) {
            Object next = k.compareTo(node.key) < 0 ? node.l : node.r;
            if (next instanceof Node) {
                node = (Node) next;
            } else {
                Descriptor desc = (Descriptor) next;
                node = desc.child;
            }
        }
        return k.compareTo(node.key) == 0 ? (V) node.value : null;
    }

    public final V put(final K key, final V value) {
        return doPut(key, value, false);
    }

    public final V putIfAbsent(final K key, final V value) {
        return doPut(key, value, true);
    }

    public V doPut(final K key, final V value, boolean putIfAbsent) {
        final Comparable<? super K> k = comparable(key);

        boolean found;
        Descriptor desc;
        while (true) {
            Node<K, V> p = null;
            Node<K, V> l = root;
            while (l.l != null) {
                Object next = l.key == null || k.compareTo(l.key) < 0 ? l.l : l.r;
                if (next instanceof Node) {
                    p = l;
                    l = (Node) next;
                } else {
                    CASN((Descriptor) next);
                }
            }
            if (l.key != null && k.compareTo(l.key) == 0) {
//                if (putIfAbsent) {
                return l.value;
//                }
//                desc = createReplaceDesc(p, l, key, value);
//                if (desc == null) {
//                    continue;
//                }
//                found = true;
            } else {
                desc = createInsertDesc(p, l, key, value);
                if (desc == null) {
                    continue;
                }
                found = false;
            }
            CASN(desc);
//            System.err.println("Insert " + key);
//            System.err.println(toString());
//            assert binarySearch(root);
            if (desc.status == Status.FINISHED) {
                return found ? l.value : null;
            }
        }
    }

    public Descriptor<K, V> createReplaceDesc(Node<K, V> p, Node<K, V> node, K key, V value) {
        Integer parentVersion = p.getVersion();
        Node<K, V> nodeL = node.getLeft();
        Node<K, V> nodeR = node.getRight();
        Integer nodeVersion = node.getVersion();
        boolean left = compare(node.key, p.key) < 0;

        if (Integer.compare(parentVersion, 0) == 0 ||
                Integer.compare(nodeVersion, 0) == 0) {
            return null;
        }

        if (node != (left ? p.l : p.r)) {
            return null;
        }
        if (node.l != nodeL) {
            return null;
        }
        if (node.r != nodeR) {
            return null;
        }
        Node<K, V> newNode = new Node<>(key, value, 0, nodeL, nodeR);
        return new Descriptor<>(p, node, newNode, parentVersion, new Node[]{node}, new Integer[]{nodeVersion});
    }

    public Descriptor<K, V> createInsertDesc(Node<K, V> p, Node<K, V> node, K key, V value) {
        Integer parentVersion = p.getVersion();
        boolean leftParent = compare(node.key, p.key) < 0;
        boolean leftSibling = compare(node.key, key) < 0;
        if (Integer.compare(parentVersion, 0) == 0) {
            return null;
        }

        if (node != (leftParent ? p.l : p.r)) {
            return null;
        }

        Node<K, V> newNode = new Node<>(key, value, 0, null, null);
        Node<K, V> newParent = leftSibling ?
                new Node<>(key, null, 1, node, newNode) : new Node<>(node.key, null, 1, newNode, node);

        return new Descriptor<>(p, node, newParent, parentVersion, new Node[]{}, new Integer[]{});
    }

    public final V remove(final Object key) {
        final Comparable<? super K> k = comparable(key);
        Descriptor<K, V> desc;

        while (true) {
            Node<K, V> gp = null;
            Node<K, V> p = null;
            Node<K, V> l = root;
            while (l.l != null) {
                Object next = l.key == null || k.compareTo(l.key) < 0 ? l.l : l.r;
                if (next instanceof Node) {
                    gp = p;
                    p = l;
                    l = (Node<K, V>) next;
                } else {
                    CASN((Descriptor<K, V>) next);
                }
            }
            if (l.key != null && k.compareTo(l.key) == 0) {
                desc = createDeleteDesc(gp, p, l);
                if (desc == null) {
                    continue;
                }
                CASN(desc);
//                System.err.println("Remove " + key);
//                System.err.println(toString());
//                assert binarySearch(root);
                if (desc.status == Status.FINISHED) {
                    return l.value;
                }
            } else {
                return null;
            }
        }
    }

    public Descriptor<K, V> createDeleteDesc(Node<K, V> gp, Node<K, V> p, Node<K, V> l) {
        boolean gpLeft = compare(p.key, gp.key) < 0;
        boolean pLeft = compare(l.key, p.key) < 0;
        Node<K, V> sibling = pLeft ? p.getRight() : p.getLeft();
        Integer gpVersion = gp.getVersion();
        Integer pVersion = p.getVersion();
        Integer lVersion = l.getVersion();
        if (Integer.compare(gpVersion, 0) == 0 ||
                Integer.compare(pVersion, 0) == 0 ||
                Integer.compare(lVersion, 0) == 0) {
            return null;
        }
        if (p != (gpLeft ? gp.l : gp.r)) {
            return null;
        }
        if (l != (pLeft ? p.l : p.r)) {
            return null;
        }
        if (sibling != (pLeft ? p.r : p.l)) {
            return null;
        }
        return new Descriptor<>(gp, p, sibling, gpVersion, new Node[]{p, l}, new Integer[]{pVersion, lVersion});
    }

    public void CASN(Descriptor<K, V> desc) {
        boolean left = compare(desc.child.key, desc.parent.key) < 0;
        Status status;
        if (desc.status == Status.UNDECIDED) {
            status = Status.FINISHED;
            if (left) {
                while (!updateLeft.compareAndSet(desc.parent, desc.child, desc) &&
                        status != Status.FAILED) {
                    Object currentLeft = desc.parent.l;
                    if (currentLeft instanceof Descriptor) {
                        if (currentLeft != desc) {
                            CASN((Descriptor<K, V>) currentLeft);
                        } else {
                            break;
                        }
                    } else {
                        if (currentLeft != desc.child) {
                            status = Status.FAILED;
                        }
                    }
                }
            } else {
                while (!updateRight.compareAndSet(desc.parent, desc.child, desc) &&
                        status != Status.FAILED) {
                    Object currentRight = desc.parent.r;
                    if (currentRight instanceof Descriptor) {
                        if (currentRight != desc) {
                            CASN((Descriptor<K, V>) currentRight);
                        } else {
                            break;
                        }
                    } else {
                        if (currentRight != desc.child) {
                            status = Status.FAILED;
                        }
                    }
                }
            }
            if (desc.status != Status.UNDECIDED) {
                return;
            }
            while (!updateVersion.compareAndSet(desc.parent, desc.parentVersion, desc) &&
                    status != Status.FAILED) {
                Object currentVersion = desc.parent.version;
                if (currentVersion instanceof Descriptor) {
                    if (currentVersion != desc) {
                        CASN((Descriptor<K, V>) currentVersion);
                    } else {
                        break;
                    }
                } else {
                    if (Integer.compare((Integer) currentVersion, desc.parentVersion) != 0) {
                        status = Status.FAILED;
                    }
                }
            }
            for (int i = 0; i < desc.toRemove.length; i++) {
                while (!updateVersion.compareAndSet(desc.toRemove[i], desc.versions[i], desc) &&
                        status != Status.FAILED) {
                    Object currentVersion = desc.toRemove[i].version;
                    if (currentVersion instanceof Descriptor) {
                        if (currentVersion != desc) {
                            CASN((Descriptor<K, V>) currentVersion);
                        } else {
                            break;
                        }
                    } else {
                        if (Integer.compare((Integer) currentVersion, desc.versions[i]) != 0) {
                            status = Status.FAILED;
                        }
                    }
                }
            }
            updateDescriptorStatus.compareAndSet(desc, Status.UNDECIDED, status);
        }

        status = updateDescriptorStatus.get(desc);
        if (left) {
            boolean zero = desc.newChild.getVersion() != 0;
            if (updateLeft.compareAndSet(desc.parent, desc, status == Status.FINISHED ? desc.newChild : desc.child)
                && status == Status.FINISHED)
                assert zero;
        } else {
            boolean zero = desc.newChild.getVersion() != 0;
            if (updateRight.compareAndSet(desc.parent, desc, status == Status.FINISHED ? desc.newChild : desc.child)
                && status == Status.FINISHED)
                assert zero;
        }
        updateVersion.compareAndSet(desc.parent, desc, status == Status.FINISHED ? desc.parentVersion + 1 : desc.parentVersion);
        for (int i = 0; i < desc.toRemove.length; i++) {
            updateVersion.compareAndSet(desc.toRemove[i], desc, status == Status.FINISHED ? new Integer(0) : desc.versions[i]);
        }
    }

    public Set<Entry<K, V>> entrySet() {
        throw new AssertionError("EntrySet function is not implemented");
    }

    public int size(Node<K, V> root) {
        if (root == null) {
            return 0;
        }
        return (root.value == null ? 0 : 1) + size((Node<K, V>) root.l) + size((Node<K, V>) root.r);
    }

    public int size() {
        return size(root);
    }

    public void clear() {
        root = new Node<K, V>(null, null, 0, new Node<K, V>(null, null, 0, null, null), null);
    }

    public String toString(Node<K, V> root) {
        if (root == null) {
            return "{}";
        }
        return "{" + toString((Node<K, V>) root.l) + "," + root.key + "," + toString((Node<K, V>) root.r) + "}";
    }

    public String toString() {
        return toString(root);
    }

    public boolean binarySearch(Node<K, V> root) {
        if (root.l == null) {
            return true;
        }
        boolean left = binarySearch((Node<K, V>) root.l) && compare(((Node<K, V>) root.l).key, root.key) < 0;
        boolean right = root.r == null || (binarySearch((Node<K, V>) root.r) && compare(((Node<K, V>) root.r).key, root.key) >= 0);
        return left && right;
    }
}
