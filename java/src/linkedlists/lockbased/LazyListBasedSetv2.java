package linkedlists.lockbased;

import contention.abstractions.AbstractCompositionalIntSet;
import linkedlists.lockbased.lazyutils.Nodev2;

import java.util.Collection;
import java.util.Random;

/**
 * The code follows the lazy list-based set of Ch.9 of Herlihy and Shavit's book:
 * "The Art of Multiprocessor Programming".
 * 
 * @author gramoli
 * 
 */
public class LazyListBasedSetv2 extends AbstractCompositionalIntSet {

    final public Nodev2 head;
    final public Nodev2 tail;

    public LazyListBasedSetv2() {
        head = new Nodev2(Integer.MIN_VALUE);
        tail = new Nodev2(Integer.MAX_VALUE);
        head.next = tail;
        tail.next = null;
    }

    @Override
    public void fill(int range, long size) {
        int i = 0;
        while (i < size)
            if (addInt(new Random().nextInt(range)))
                i++;
    }

    private boolean validate(Nodev2 pred, Nodev2 curr) {
        return !pred.marked && pred.next == curr;
    }

    @Override
    public boolean addInt(int v) {
        while (true) {
            Nodev2 pred = head;
            Nodev2 curr = head.next;
            while (curr.value < v) {
                pred = curr;
                curr = curr.next;
            }
            pred.lock();
            try {
                if (!pred.marked) {
                    curr = pred.next;
                    if (curr.value == v) {
                        return false;
                    } else {
                        Nodev2 node = new Nodev2(v);
                        node.next = curr;
                        pred.next = node;
                        return true;
                    }
                }
            } finally {
                pred.unlock();
            }
        }
    }

    @Override
    public boolean removeInt(int v) {
        while (true) {
            Nodev2 pred = head;
            Nodev2 curr = head.next;
            while (curr.value < v) {
                pred = curr;
                curr = curr.next;
            }
            pred.lock();
            try {
                curr.lock();
                try {
                    if (validate(pred, curr)) {
                        if (curr.value != v) {
                            return false;
                        } else {
                            curr.marked = true;
                            pred.next = curr.next;
                            return true;
                        }
                    }
                } finally {
                    curr.unlock();
                }
            } finally {
                pred.unlock();
            }
        }
    }

    @Override
    public boolean containsInt(int v) {
        Nodev2 curr = head;
        while (curr.value < v) {
            curr = curr.next;
        }
        return curr.value == v && !curr.marked;
    }

    @Override
    public Object getInt(int x) {
        throw new RuntimeException("unimplemented method");
        // TODO Auto-generated method stub
    }

    @Override
    public boolean addAll(Collection<Integer> c) {
        throw new RuntimeException("unimplemented method");
        // TODO Auto-generated method stub
    }

    @Override
    public boolean removeAll(Collection<Integer> c) {
        throw new RuntimeException("unimplemented method");
        // TODO Auto-generated method stub
    }

    @Override
    public int size() {
        int cpt = 0;
        Nodev2 curr = head;
        while (curr.value < Integer.MAX_VALUE)
            curr = curr.next;
        if (!curr.marked)
            cpt++;
        return cpt;
    }

    @Override
    public void clear() {
        head.next = tail;
    }
}
