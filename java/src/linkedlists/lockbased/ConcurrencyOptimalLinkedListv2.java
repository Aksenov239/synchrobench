package linkedlists.lockbased;

import contention.abstractions.AbstractCompositionalIntSet;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by vaksenov on 07.02.2019.
 */
public class ConcurrencyOptimalLinkedListv2 extends AbstractCompositionalIntSet {
    public class Node {
        final int value;
        volatile Node next;
        volatile boolean deleted;

        AtomicBoolean lock;

        public Node(int value) {
            this.value = value;
            lock = new AtomicBoolean();
        }

        public boolean lock(Node next) {
            if (this.next != next || deleted) {
                return false;
            }
            while (!lock.compareAndSet(false, true)) {}
            if (this.next != next || deleted) {
                lock.set(false);
                return false;
            }
            return true;
        }

        public boolean lock(int next) {
            if (this.next.value != next || deleted) {
                return false;
            }
            while (!lock.compareAndSet(false, true)) {}
            if (this.next.value != next || deleted) {
                lock.set(false);
                return false;
            }
            return true;
        }

        public void unlock() {
            lock.set(false);
        }
    }

    Node head;

    public ConcurrencyOptimalLinkedListv2() {
        head = new Node(Integer.MIN_VALUE);
        head.next = new Node(Integer.MAX_VALUE);
    }

    public class Window {
        Node prev;
        Node curr;

        public Window() {
            prev = head;
        }

        public void setValues(Node prev, Node curr) {
            this.prev = prev;
            this.curr = curr;
        }
    }

    public Window traverse(final int value, Window window) {
        Node prev = window.prev;
        if (prev.deleted) {
            prev = head;
        }
        Node curr = prev.next;
        while (curr.value < value) {
            prev = curr;
            curr = curr.next;
        }

        window.setValues(prev, curr);
        return window;
    }

    @Override
    public boolean addInt(int x) {
        Window window = new Window();

        Node newNode = null;

        Node prev, curr;

        while (true) {
            traverse(x, window);
            curr = window.curr;
            if (curr.value == x) {
                return false;
            }

            if (newNode == null) {
                newNode = new Node(x);
            }
            newNode.next = curr;
            prev = window.prev;
            if (!prev.lock(curr)) {
                continue;
            }

            prev.next = newNode;

            prev.unlock();
            return true;
        }
    }

    @Override
    public boolean removeInt(int x) {
        Window window = new Window();

        Node prev, curr;

        while (true) {
            traverse(x, window);
            curr = window.curr;
            if (curr.value != x || curr.deleted) {
                return false;
            }

            Node next = curr.next;

            prev = window.prev;
            if (!prev.lock(x)) {
                continue;
            }

            curr = prev.next;
            if (!curr.lock(next)) {
                prev.unlock();
                continue;
            }

            curr.deleted = true;
            prev.next = next;

            curr.unlock();
            prev.unlock();
            return true;
        }
    }

    @Override
    public boolean containsInt(int x) {
        Node curr = head;
        while (curr.value < x) {
            curr = curr.next;
        }
        return curr.value == x && !curr.deleted;
    }

    @Override
    public int size() {
        int total = 0;
        Node curr = head;
        while (curr != null) {
            total++;
            curr = curr.next;
        }
        return total - 2;
    }

    @Override
    public void clear() {
        head = new Node(Integer.MIN_VALUE);
        head.next = new Node(Integer.MAX_VALUE);
    }
}
