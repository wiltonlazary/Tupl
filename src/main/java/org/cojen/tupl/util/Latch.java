/*
 *  Copyright 2011-2016 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl.util;

import java.util.ArrayList;
import java.util.Collection;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import java.util.concurrent.locks.AbstractOwnableSynchronizer;
import java.util.concurrent.locks.LockSupport;

/**
 * A latch is a lightweight non-reentrant mutex, supporting shared and exclusive modes. Latch
 * acquisition is typically unfair, but fair handoff is performed as necessary to prevent
 * starvation.
 *
 * @author Brian S O'Neill
 * @see LatchCondition
 */
@SuppressWarnings("serial")
public class Latch /*extends AbstractOwnableSynchronizer*/ {
    public static final int UNLATCHED = 0, EXCLUSIVE = 0x80000000, SHARED = 1;

    static final int SPIN_LIMIT = Runtime.getRuntime().availableProcessors();

    static final AtomicIntegerFieldUpdater<Latch> cStateUpdater =
        AtomicIntegerFieldUpdater.newUpdater(Latch.class, "mLatchState");

    private static final AtomicReferenceFieldUpdater<Latch, Node> cLastUpdater =
        AtomicReferenceFieldUpdater.newUpdater(Latch.class, Node.class, "mLatchLast");

    private static final AtomicReferenceFieldUpdater<Latch, Node> cFirstUpdater =
        AtomicReferenceFieldUpdater.newUpdater(Latch.class, Node.class, "mLatchFirst");

    private static final AtomicReferenceFieldUpdater<Node, Thread> cWaiterUpdater =
        AtomicReferenceFieldUpdater.newUpdater(Node.class, Thread.class, "mWaiter");

    /*
      unlatched:           0               latch is available
      exclusive:  0x80000000               latch is held exclusively
      shared:              1..0x7fffffff   latch is held shared
      xshared:    0x80000001..0xffffffff   latch is held shared, and exclusive is requested
     */ 
    volatile int mLatchState;

    // Queue of waiting threads.
    private transient volatile Node mLatchFirst;
    private transient volatile Node mLatchLast;

    public Latch() {
    }

    /**
     * @param initialState UNLATCHED, EXCLUSIVE or SHARED
     */
    public Latch(int initialState) {
        mLatchState = initialState;
    }

    /**
     * Try to acquire the exclusive latch, barging ahead of any waiting threads if possible.
     */
    public final boolean tryAcquireExclusive() {
        return mLatchState == 0 && cStateUpdater.compareAndSet(this, 0, EXCLUSIVE);
    }

    /**
     * Attempt to acquire the exclusive latch, aborting if interrupted.
     *
     * @param nanosTimeout pass -1 for infinite timeout
     */
    public final boolean tryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException {
        int trials = 0;
        while (true) {
            int state = mLatchState;

            if (state == 0) {
                if (cStateUpdater.compareAndSet(this, 0, EXCLUSIVE)) {
                    return true;
                }
            } else {
                if (nanosTimeout == 0) {
                    return false;
                }

                // Shared latches prevent an exclusive latch from being immediately acquired,
                // but no new shared latches can be granted once the exclusive bit is set.
                if (state > 0 && !cStateUpdater.compareAndSet(this, state, state | EXCLUSIVE)) {
                    trials = spin(trials);
                    continue;
                }
            }

            return acquire(new Timed(nanosTimeout));
        }
    }

    /**
     * Acquire the exclusive latch, barging ahead of any waiting threads if possible.
     */
    public final void acquireExclusive() {
        int trials = 0;
        while (true) {
            int state = mLatchState;

            if (state == 0) {
                if (cStateUpdater.compareAndSet(this, 0, EXCLUSIVE)) {
                    return;
                }
            } else {
                // Shared latches prevent an exclusive latch from being immediately acquired,
                // but no new shared latches can be granted once the exclusive bit is set.
                if (state > 0 && !cStateUpdater.compareAndSet(this, state, state | EXCLUSIVE)) {
                    trials = spin(trials);
                    continue;
                }
            }

            acquire(new Node());
            return;
        }
    }

    /**
     * Acquire the exclusive latch, aborting if interrupted.
     */
    public final void acquireExclusiveInterruptibly() throws InterruptedException {
        tryAcquireExclusiveNanos(-1);
    }

    /**
     * Downgrade the held exclusive latch into a shared latch. Caller must later call
     * releaseShared instead of releaseExclusive.
     */
    public final void downgrade() {
        fairHandoff: {
            Node last = mLatchLast;
            if (last == null) {
                // No waiters, so release the latch.
                mLatchState = 1;

                // Need to check if any waiters again, due to race with enqueue. If cannot
                // immediately re-acquire the latch, then let the new owner (which barged in)
                // unpark the waiters when it releases the latch.
                last = mLatchLast;
                if (last == null) {
                    return;
                }

                if (!cStateUpdater.compareAndSet(this, 1, EXCLUSIVE) && !tryUpgrade()) {
                    // Without the latch, fair handoff is not possible. Fair handoff must
                    // dequeue nodes, but only the exclusive latch owner or lone shared latch
                    // owner can do that.
                    break fairHandoff;
                }
            }

            // Sweep through the queue, finding a contiguous run of shared waiters.
            Node first;
            int trials = 0;
            while (true) {
                // Although the last waiter has been observed to exist, the first waiter field
                // might not be set yet.
                first = mLatchFirst;
                if (first != null) {
                    break;
                }
                trials = spin(trials);
            }

            // The first pass determines the maximum number of waiters to fairly unpark. Scan
            // the queue to find the last one.
            int newState = 1;
            Node tail = null;
            {
                Node node = first;
                while (true) {
                    Thread waiter = node.mWaiter;
                    if (waiter != null) {
                        if (node instanceof Shared) {
                            newState++;
                            tail = node;
                        } else {
                            // An exclusive waiter is in the queue, so disallow new shared
                            // latches. This is indicated by setting the exclusive bit, along
                            // with a non-zero shared latch count.
                            newState |= EXCLUSIVE;
                            break;
                        }
                    }
                    if (node == last) {
                        break;
                    }
                    Node next = node.get();
                    if (next == null) {
                        break;
                    }
                    node = next;
                }
            }

            // Switch the state and fairly unpark the waiters, preventing a thundering herd.
            mLatchState = newState;

            if (tail != null) {
                Node node = first;
                while (true) {
                    Thread waiter = node.mWaiter;
                    if (waiter != null && cWaiterUpdater.compareAndSet(node, waiter, null)) {
                        LockSupport.unpark(waiter);
                        // Count the actual number of unparks.
                        newState--;
                    }
                    if (node == tail) {
                        break;
                    }
                    node = node.get();
                }

                int diff = newState & ~EXCLUSIVE;
                if (diff > 1) {
                    // Fewer unparks than expected, so fix the state.
                    cStateUpdater.addAndGet(this, 1 - diff);
                }

                // Advance the first node along the queue.

                trials = 0;
                while (true) {
                    Node next = tail.get();
                    if (next != null) {
                        mLatchFirst = next;
                        break;
                    } else {
                        // Queue is now empty, unless an enqueue is in progress.
                        if (tail == last && cLastUpdater.compareAndSet(this, last, null)) {
                            cFirstUpdater.compareAndSet(this, first, null);
                            break;
                        }
                    }
                    trials = spin(trials);
                }
            }

            if (newState < 0) {
                // First in the queue is exclusive, so skip checking for new shared waiters.
                return;
            }
        }

        // If any shared waiters were added concurrently, unpark them, but leave them in the
        // queue. They cannot be fairly unparked, because the latch state was released. Another
        // thread might now be dequeuing nodes.

        for (Node node = first(); node != null; node = node.get()) {
            Thread waiter = node.mWaiter;
            if (waiter != null) {
                if (node instanceof Shared) {
                    LockSupport.unpark(waiter);
                } else {
                    break;
                }
            }
        }
    }

    /**
     * Release the held exclusive latch.
     */
    public final void releaseExclusive() {
        int trials = 0;
        while (true) {
            Node last = mLatchLast;

            if (last == null) {
                // No waiters, so release the latch.
                mLatchState = 0;

                // Need to check if any waiters again, due to race with enqueue. If cannot
                // immediately re-acquire the latch, then let the new owner (which barged in)
                // unpark the waiters when it releases the latch.
                last = mLatchLast;
                if (last == null || !cStateUpdater.compareAndSet(this, 0, EXCLUSIVE)) {
                    return;
                }
            }

            // Although the last waiter has been observed to exist, the first waiter field
            // might not be set yet.
            Node first = mLatchFirst;

            unpark: if (first != null) {
                Thread waiter = first.mWaiter;

                if (waiter != null) {
                    if (first instanceof Shared) {
                        // TODO: can this be combined into one downgrade step?
                        downgrade();
                        releaseShared();
                        return;
                    }

                    if (!first.mDenied) {
                        // Unpark the waiter, but allow another thread to barge in.
                        mLatchState = 0;
                        LockSupport.unpark(waiter);
                        return;
                    }
                }

                // Remove first from the queue.
                {
                    Node next = first.get();
                    if (next != null) {
                        mLatchFirst = next;
                    } else {
                        // Queue is now empty, unless an enqueue is in progress.
                        if (last != first || !cLastUpdater.compareAndSet(this, last, null)) {
                            break unpark;
                        }
                        cFirstUpdater.compareAndSet(this, last, null);
                    }
                }

                if (waiter != null && cWaiterUpdater.compareAndSet(first, waiter, null)) {
                    // Fair handoff to waiting thread.
                    LockSupport.unpark(waiter);
                    return;
                }
            }

            trials = spin(trials);
        }
    }

    /**
     * Convenience method, which releases the held exclusive or shared latch.
     *
     * @param exclusive call releaseExclusive if true, else call releaseShared
     */
    public final void release(boolean exclusive) {
        if (exclusive) {
            releaseExclusive();
        } else {
            releaseShared();
        }
    }

    /**
     * Releases an exclusive or shared latch.
     */
    public final void releaseEither() {
        int state = mLatchState;
        if (state == EXCLUSIVE) {
            releaseExclusive();
        } else {
            releaseShared(state);
        }
    }

    /**
     * Try to acquire the shared latch, barging ahead of any waiting threads if possible.
     */
    public final boolean tryAcquireShared() {
        int state = mLatchState;
        return state >= 0 && cStateUpdater.compareAndSet(this, state, state + 1);
    }

    /**
     * Attempt to acquire a shared latch, aborting if interrupted.
     *
     * @param nanosTimeout pass -1 for infinite timeout
     */
    public final boolean tryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
        int trials = 0;
        while (true) {
            int state = mLatchState;
            if (state < 0) {
                return acquire(new TimedShared(nanosTimeout));
            }
            if (cStateUpdater.compareAndSet(this, state, state + 1)) {
                return true;
            }
            trials = spin(trials);
        }
    }

    /**
     * Acquire the shared latch, barging ahead of any waiting threads if possible.
     */
    public final void acquireShared() {
        int trials = 0;
        while (true) {
            int state = mLatchState;
            if (state < 0) {
                acquire(new Shared());
                return;
            }
            if (cStateUpdater.compareAndSet(this, state, state + 1)) {
                return;
            }
            trials = spin(trials);
        }
    }

    /**
     * Acquire a shared latch, aborting if interrupted.
     */
    public final void acquireSharedInterruptibly() throws InterruptedException {
        tryAcquireSharedNanos(-1);
    }

    /**
     * Attempt to upgrade a held shared latch into an exclusive latch. Upgrade fails if shared
     * latch is held by more than one thread. If successful, caller must later call
     * releaseExclusive instead of releaseShared.
     */
    public final boolean tryUpgrade() {
        while (true) {
            int state = mLatchState;
            if ((state & ~EXCLUSIVE) != 1) {
                return false;
            }
            if (cStateUpdater.compareAndSet(this, state, EXCLUSIVE)) {
                return true;
            }
            // Try again if exclusive bit flipped. Don't bother with spin yielding, because the
            // exclusive bit usually switches to 1, not 0.
        }
    }

    /**
     * Release a held shared latch.
     */
    public final void releaseShared() {
        releaseShared(mLatchState);
    }

    private void releaseShared(int state) {
        int trials = 0;
        while (true) {
            if (state < 0) {
                // An exclusive latch is waiting in the queue.
                if (cStateUpdater.compareAndSet(this, state, --state)) {
                    if (state == EXCLUSIVE) {
                        // This thread just released the last shared latch, and now it owns the
                        // exclusive latch. Release it for the next in the queue.
                        releaseExclusive();
                    }
                    return;
                }
            } else {
                Node last = mLatchLast;
                if (last == null) {
                    // No waiters, so release the latch.
                    if (cStateUpdater.compareAndSet(this, state, --state)) {
                        if (state == 0) {
                            // Need to check if any waiters again, due to race with enqueue. If
                            // cannot immediately re-acquire the latch, then let the new owner
                            // (which barged in) unpark the waiters when it releases the latch.
                            last = mLatchLast;
                            if (last != null && cStateUpdater.compareAndSet(this, 0, EXCLUSIVE)) {
                                releaseExclusive();
                            }
                        }
                        return;
                    }
                } else if (state == 1) {
                    // Try to switch to exclusive, and then let releaseExclusive deal with
                    // unparking the waiters.
                    if (cStateUpdater.compareAndSet(this, 1, EXCLUSIVE) || tryUpgrade()) {
                        releaseExclusive();
                        return;
                    }
                } else if (cStateUpdater.compareAndSet(this, state, --state)) {
                    return;
                }
            }

            trials = spin(trials);
            state = mLatchState;
        }
    }

    private boolean acquire(final Node node) {
        // Enqueue the node.
        Node prev;
        {
            node.mWaiter = Thread.currentThread();
            prev = cLastUpdater.getAndSet(this, node);
            if (prev == null) {
                mLatchFirst = node;
            } else {
                prev.set(node);
                Node pp = prev.mPrev;
                if (pp != null) {
                    // The old last node was intended to be removed, but the last node cannot
                    // be removed unless it's also the first. Bypass it now that a new last
                    // node has been enqueued.
                    pp.lazySet(node);
                }
            }
        }

        int acquireResult = node.acquire(this);

        if (acquireResult < 0) {
            while (true) {
                boolean parkAbort = node.park(this);

                if (node.mWaiter == null) {
                    // Fair handoff, and so node is no longer in the queue.
                    return true;
                }

                acquireResult = node.acquire(this);

                if (acquireResult >= 0) {
                    // Latch acquired after parking.
                    break;
                }

                if (parkAbort) {
                    cWaiterUpdater.lazySet(node, null);
                    // FIXME: if xshared state, clear it?
                    // FIXME: remove from queue

                    if (Thread.interrupted()) {
                        Latch.<RuntimeException>castAndThrow(new InterruptedException());
                    }

                    return false;
                }

                // Lost the race. Request fair handoff.
                node.mDenied = true;
            }
        }

        if (acquireResult != 0) {
            // Only one thread is allowed to remove nodes.
            return true;
        }

        // Remove the node now, releasing memory. Because the latch is held, no other dequeues
        // are in progress, but enqueues still are.

        if (mLatchFirst == node) {
            while (true) {
                Node next = node.get();
                if (next != null) {
                    mLatchFirst = next;
                    return true;
                } else {
                    // Queue is now empty, unless an enqueue is in progress.
                    Node last = mLatchLast;
                    if (last == node && cLastUpdater.compareAndSet(this, last, null)) {
                        cFirstUpdater.compareAndSet(this, last, null);
                        return true;
                    }
                }
            }
        } else {
            Node next = node.get();
            if (next == null) {
                // Removing the last node creates race conditions with enqueues. Instead, stash
                // a reference to the previous node and let the enqueue deal with it after a
                // new node has been enqueued.
                node.mPrev = prev;
                next = node.get();
                // Double check in case an enqueue just occurred that may have failed to notice
                // the previous node assignment.
                if (next == null) {
                    return true;
                }
            }
            // Bypass the removed node, allowing it to be released.
            prev.lazySet(next);
            return true;
        }
    }

    // Define some methods from AbstractQueuedSynchronizer...

    public final boolean hasQueuedThreads() {
        return mLatchLast != null;
    }

    public final Thread getFirstQueuedThread() {
        for (Node node = first(); node != null; node = node.get()) {
            Thread waiter = node.mWaiter;
            if (waiter != null) {
                return waiter;
            }
        }
        return null;
    }

    public final boolean isQueued(Thread thread) {
        if (thread == null) {
            throw new NullPointerException();
        }
        for (Node node = first(); node != null; node = node.get()) {
            if (node.mWaiter == thread) {
                return true;
            }
        }
        return false;
    }

    public final boolean hasQueuedPredecessors() {
        Node node = first();
        if (node != null) {
            Thread current = Thread.currentThread();
            do {
                Thread waiter = node.mWaiter;
                if (waiter == current) {
                    return false;
                } else if (waiter != null) {
                    return true;
                }
                node = node.get();
            } while (node != null);
        }
        return false;
    }

    public final int getQueueLength() {
        int count = 0;
        for (Node node = first(); node != null; node = node.get()) {
            Thread waiter = node.mWaiter;
            if (waiter != null) {
                count++;
            }
        }
        return count;
    }

    public final Collection<Thread> getQueuedThreads() {
        ArrayList<Thread> threads = new ArrayList<>();
        for (Node node = first(); node != null; node = node.get()) {
            Thread waiter = node.mWaiter;
            if (waiter != null) {
                threads.add(waiter);
            }
        }
        return threads;
    }

    public final Collection<Thread> getExclusiveQueuedThreads() {
        ArrayList<Thread> threads = new ArrayList<>();
        for (Node node = first(); node != null; node = node.get()) {
            if (!(node instanceof Shared)) {
                Thread waiter = node.mWaiter;
                if (waiter != null) {
                    threads.add(waiter);
                }
            }
        }
        return threads;
    }

    public final Collection<Thread> getSharedQueuedThreads() {
        ArrayList<Thread> threads = new ArrayList<>();
        for (Node node = first(); node != null; node = node.get()) {
            if (node instanceof Shared) {
                Thread waiter = node.mWaiter;
                if (waiter != null) {
                    threads.add(waiter);
                }
            }
        }
        return threads;
    }

    private Node first() {
        int trials = 0;
        while (true) {
            Node last = mLatchLast;
            if (last == null) {
                return null;
            }
            // Although the last waiter has been observed to exist, the first waiter field
            // might not be set yet.
            Node first = mLatchFirst;
            if (first != null) {
                return first;
            }
            trials = spin(trials);
        }
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        appendMiniString(b, this);
        b.append(" {state=");

        int state = mLatchState;
        if (state == 0) {
            b.append("unlatched");
        } else if (state == EXCLUSIVE) {
            b.append("exclusive");
        } else if (state >= 0) {
            b.append("shared:").append(state);
        } else {
            b.append("xshared:").append(state & ~EXCLUSIVE);
        }

        Node last = mLatchLast;

        if (last != null) {
            b.append(", ");
            Node first = mLatchFirst;
            if (first == last) {
                b.append("firstQueued=").append(last);
            } else if (first == null) {
                b.append("lastQueued=").append(last);
            } else {
                b.append("firstQueued=").append(first)
                    .append(", lastQueued=").append(last);
            }
        }

        return b.append('}').toString();
    }

    /**
     * @return new trials value
     */
    static int spin(int trials) {
        trials++;
        if (trials >= SPIN_LIMIT) {
            Thread.yield();
            trials = 0;
        }
        return trials;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void castAndThrow(Throwable e) throws T {
        throw (T) e;
    }

    private static void appendMiniString(StringBuilder b, Object obj) {
        if (obj == null) {
            b.append("null");
            return;
        }
        b.append(obj.getClass().getName()).append('@').append(Integer.toHexString(obj.hashCode()));
    }

    /**
     * Atomic reference is to the next node in the chain.
     */
    static class Node extends AtomicReference<Node> {
        volatile Thread mWaiter;
        volatile boolean mDenied;

        // Only set if node was deleted and must be bypassed when a new node is enqueued.
        volatile Node mPrev;

        /**
         * @return true if timed out or interrupted
         */
        boolean park(Latch latch) {
            LockSupport.park(latch);
            return false;
        }

        /**
         * @return <0 if thread should park; 0 if acquired and node should also be removed; >0
         * if acquired and node should not be removed
         */
        int acquire(Latch latch) {
            int trials = 0;
            while (true) {
                int state = latch.mLatchState;
                if (state < 0) {
                    return state;
                }

                // Try to acquire exclusive latch, or at least deny new shared latches.
                if (cStateUpdater.compareAndSet(latch, state, state | EXCLUSIVE)) {
                    if (state == 0) {
                        // Acquired, so no need to reference the thread anymore.
                        cWaiterUpdater.lazySet(this, null);
                        return state;
                    } else {
                        return -1;
                    }
                }

                trials = spin(trials);
            }
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            appendMiniString(b, this);
            b.append(" {waiter=").append(mWaiter);
            b.append(", denied=").append(mDenied);
            b.append(", next="); appendMiniString(b, get());
            return b.append('}').toString();
        }
    }

    static class Timed extends Node {
        private long mNanosTimeout;
        private long mEndNanos;

        Timed(long nanosTimeout) {
            mNanosTimeout = nanosTimeout;
            if (nanosTimeout >= 0) {
                mEndNanos = System.nanoTime() + nanosTimeout;
            }
        }

        @Override
        final boolean park(Latch latch) {
            if (mNanosTimeout < 0) {
                LockSupport.park(latch);
                return Thread.currentThread().isInterrupted();
            } else {
                LockSupport.parkNanos(latch, mNanosTimeout);
                if (Thread.currentThread().isInterrupted()) {
                    return true;
                }
                return (mNanosTimeout = mEndNanos - System.nanoTime()) <= 0;
            }
        }
    }

    static class Shared extends Node {
        @Override
        final int acquire(Latch latch) {
            int trials = 0;
            while (true) {
                int state = latch.mLatchState;
                if (state < 0) {
                    return state;
                }

                if (cStateUpdater.compareAndSet(latch, state, state + 1)) {
                    // Acquired, so no need to reference the thread anymore.
                    Thread waiter = mWaiter;
                    if (waiter == null || !cWaiterUpdater.compareAndSet(this, waiter, null)) {
                        // Handoff was actually fair, and now an extra shared latch must be
                        // released.
                        if (state < 1) {
                            throw new AssertionError(state);
                        }
                        if (!cStateUpdater.compareAndSet(latch, state + 1, state)) {
                            cStateUpdater.decrementAndGet(latch);
                        }
                        // Already removed from the queue.
                        return 1;
                    }

                    // Only remove node if this thread is the first shared latch owner. This
                    // guarantees that no other thread will be concurrently removing nodes.
                    // Nodes for other threads will have their nodes removed later, as latches
                    // are released. Early removal is a garbage collection optimization.
                    return state;
                }

                trials = spin(trials);
            }
        }
    }

    static class TimedShared extends Shared {
        private long mNanosTimeout;
        private long mEndNanos;

        TimedShared(long nanosTimeout) {
            mNanosTimeout = nanosTimeout;
            if (nanosTimeout >= 0) {
                mEndNanos = System.nanoTime() + nanosTimeout;
            }
        }

        @Override
        final boolean park(Latch latch) {
            if (mNanosTimeout < 0) {
                LockSupport.park(latch);
                return Thread.currentThread().isInterrupted();
            } else {
                LockSupport.parkNanos(latch, mNanosTimeout);
                if (Thread.currentThread().isInterrupted()) {
                    return true;
                }
                return (mNanosTimeout = mEndNanos - System.nanoTime()) <= 0;
            }
        }
    }
}
