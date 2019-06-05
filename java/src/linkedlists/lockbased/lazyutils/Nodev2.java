package linkedlists.lockbased.lazyutils;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Nodev2 {
    public final int value;

    /** next pointer */
    public volatile Nodev2 next;

    /** deleted flag */
    public volatile boolean marked;

    private final AtomicBoolean lock;

    public Nodev2(final int value) {
        this.value = value;
        this.lock = new AtomicBoolean();
        marked = false;
    }

    public void lock() {
        while (!lock.compareAndSet(false, true)) {}
    }

    public void unlock() {
        lock.set(false);
    }
}
