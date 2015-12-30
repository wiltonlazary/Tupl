/*
 *  Copyright 2015 Cojen.org
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

package org.cojen.tupl;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import java.util.concurrent.locks.LockSupport;

/**
 * Simple unfair latch implementation.
 *
 * @author Brian S O'Neill
 */
@org.junit.Ignore
public class UnfairLatch {
    public static void main(String[] args) throws Exception {
        final UnfairLatch latch = new UnfairLatch();

        if (false) {
            class T extends Thread {
                @Override
                public void run() {
                    latch.acquireExclusive();
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                    }
                    System.out.println("*** release: " + latch);
                    latch.releaseExclusive();
                }
            };

            Thread t1 = new T();
            Thread t2 = new T();

            t1.start();
            t2.start();

            Thread.sleep(100);
            System.out.println(latch);
            latch.acquireExclusive();
            System.out.println(latch);

            latch.releaseExclusive();
            System.out.println(latch);

            t1.join();
            t2.join();
        }

        final java.util.concurrent.locks.Lock lock =
            new java.util.concurrent.locks.ReentrantLock(false);

        if (true) {
            final long[] countValue = new long[1];

            class Counter extends Thread {
                final int mNum;

                Counter(int num) {
                    mNum = num;
                }

                @Override
                public void run() {
                    for (int i=0; i<mNum; i++) {
                        //lock.lock();
                        //countValue[0]++;
                        //lock.unlock();

                        latch.acquireExclusive();
                        countValue[0]++;
                        latch.releaseExclusive();
                    }
                }
            };

            final int num = 1_000_000_000;

            Counter[] counters = new Counter[50];
            for (int i=0; i<counters.length; i++) {
                counters[i] = new Counter(num);
            }

            long start = System.currentTimeMillis();

            for (Counter c : counters) {
                c.start();
            }

            for (Counter c : counters) {
                c.join();
                System.out.println(c);
            }

            long end = System.currentTimeMillis();

            System.out.println("count value: " + countValue[0]);
            System.out.println("duration: " + (end - start));
        }
    }

    private static final int SPIN_LIMIT = Runtime.getRuntime().availableProcessors();

    private static final AtomicIntegerFieldUpdater<UnfairLatch> cStateUpdater =
        AtomicIntegerFieldUpdater.newUpdater(UnfairLatch.class, "mLatchState");

    private static final AtomicReferenceFieldUpdater<UnfairLatch, Node> cLastUpdater =
        AtomicReferenceFieldUpdater.newUpdater(UnfairLatch.class, Node.class, "mLatchLast");

    private static final AtomicReferenceFieldUpdater<UnfairLatch, Node> cFirstUpdater =
        AtomicReferenceFieldUpdater.newUpdater(UnfairLatch.class, Node.class, "mLatchFirst");

    private static final AtomicReferenceFieldUpdater<Node, Thread> cWaiterUpdater =
        AtomicReferenceFieldUpdater.newUpdater(Node.class, Thread.class, "mWaiter");

    private volatile int mLatchState;
    private volatile Node mLatchFirst;
    private volatile Node mLatchLast;

    public UnfairLatch() {
    }

    /**
     * Acquire the exclusive latch, barging ahead of any waiting threads if possible.
     */
    public final void acquireExclusive() {
        if (mLatchState == 0 && cStateUpdater.compareAndSet(this, 0, 0x80000000)) {
            return;
        }

        Node node = new Node();

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

        while (true) {
            if (mLatchState == 0 && cStateUpdater.compareAndSet(this, 0, 0x80000000)) {
                // Latch just became available, so don't park.
                break;
            }

            LockSupport.park(this);

            if (node.mWaiter == null) {
                // Fair handoff, and so node is no longer in the queue.
                return;
            }

            if (mLatchState == 0 && cStateUpdater.compareAndSet(this, 0, 0x80000000)) {
                // Latch acquired after parking.
                break;
            }

            // Lost the race. Request fair handoff.
            node.mState = Node.DENIED;
        }

        // Discard the thread reference, indicating that the acquire request is finished.
        cWaiterUpdater.lazySet(node, null);

        // Remove the node now, releasing memory. Because the latch is held, no dequeues are in
        // progress, but enqueues still are.

        if (mLatchFirst == node) {
            while (true) {
                Node next = node.get();
                if (next != null) {
                    mLatchFirst = next;
                    return;
                } else {
                    // Queue is now empty, unless an enqueue is in progress.
                    Node last = mLatchLast;
                    if (last == node && cLastUpdater.compareAndSet(this, last, null)) {
                        cFirstUpdater.compareAndSet(this, last, null);
                        return;
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
                    return;
                }
            }
            // Bypass the removed node, allowing it to be released.
            prev.lazySet(next);
            return;
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
                // signal the waiter when it releases the latch.
                last = mLatchLast;
                if (last == null || !cStateUpdater.compareAndSet(this, 0, 0x80000000)) {
                    return;
                }
            }

            // Although the last waiter has been observed to exist, the first waiter field
            // might not be set yet.
            Node first = mLatchFirst;

            unpark: if (first != null) {
                Thread waiter = first.mWaiter;

                if (waiter != null) {
                    int state = first.mState;
                    if (state < Node.DENIED) {
                        // Unpark the waiter, but allow another thread to barge in.
                        mLatchState = 0;
                        if (state == Node.READY) {
                            first.mState = Node.UNPARKED;
                            LockSupport.unpark(waiter);
                        }
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

            trials++;

            if (trials >= SPIN_LIMIT) {
                // Spinning too much if first waiting-thread-to-be is stalled. Back off a tad.
                Thread.yield();
                trials = 0;
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder()
            .append(super.toString()).append(" {state=");

        int state = mLatchState;
        if (state == 0) {
            b.append("unlatched");
        } else if (state == 0x80000000) {
            b.append("exclusive");
        } else if (state >= 0) {
            b.append("shared:").append(state);
        } else {
            b.append("illegal:").append(Integer.toUnsignedString(state, 16));
        }

        Node last = mLatchLast;

        if (last != null) {
            b.append(", ");
            Node first = mLatchFirst;
            if (first == last) {
                b.append("firstWaiter=").append(last.mWaiter);
            } else if (first == null) {
                b.append("lastWaiter=").append(last.mWaiter);
            } else {
                b.append("firstWaiter=").append(first.mWaiter)
                    .append(", lastWaiter=").append(last.mWaiter);
            }
        }

        return b.append('}').toString();
    }

    /**
     * Atomic reference is to the next node in the chain.
     */
    static class Node extends AtomicReference<Node> {
        static final int READY = 0, UNPARKED = 1, DENIED = 2;

        volatile Thread mWaiter;
        volatile int mState;

        // Only set if node was deleted and must be bypassed when a new node is enqueued.
        volatile Node mPrev;

        @Override
        public String toString() {
            return getClass().getName() + '@' + Integer.toHexString(hashCode()) +
                " {waiter=" + mWaiter + ", state=" + mState + ", next=" + get() +
                '}';
        }
    }
}
