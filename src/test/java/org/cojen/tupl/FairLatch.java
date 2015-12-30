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
 * Simple fair latch implementation.
 *
 * @author Brian S O'Neill
 */
@org.junit.Ignore
public class FairLatch {
    public static void main(String[] args) throws Exception {
        final FairLatch latch = new FairLatch();

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

        //final java.util.concurrent.locks.Lock lock =
        //    new java.util.concurrent.locks.ReentrantLock(true);

        if (true) {
            final int[] countValue = new int[1];

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

            final int num = 1_000_000;

            Counter[] counters = new Counter[5];
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

    private static final AtomicIntegerFieldUpdater<FairLatch> cStateUpdater =
        AtomicIntegerFieldUpdater.newUpdater(FairLatch.class, "mLatchState");

    private static final AtomicReferenceFieldUpdater<FairLatch, Node> cLastUpdater =
        AtomicReferenceFieldUpdater.newUpdater(FairLatch.class, Node.class, "mLatchLast");

    private static final AtomicReferenceFieldUpdater<FairLatch, Node> cFirstUpdater =
        AtomicReferenceFieldUpdater.newUpdater(FairLatch.class, Node.class, "mLatchFirst");

    private static final AtomicReferenceFieldUpdater<Node, Thread> cWaiterUpdater =
        AtomicReferenceFieldUpdater.newUpdater(Node.class, Thread.class, "mWaiter");

    private volatile int mLatchState;
    private volatile Node mLatchFirst;
    private volatile Node mLatchLast;

    public FairLatch() {
    }

    /**
     * Acquire the exclusive latch, barging ahead of any waiting threads if possible.
     */
    public final void acquireExclusive() {
        for (int i=0; i<SPIN_LIMIT; i++) {
            if (mLatchState == 0 && cStateUpdater.compareAndSet(this, 0, 0x80000000)) {
                return;
            }
        }

        Node node = new Node();

        // Enqueue the node.
        {
            node.mWaiter = Thread.currentThread();
            Node prev = cLastUpdater.getAndSet(this, node);
            if (prev == null) {
                mLatchFirst = node;
            } else {
                prev.set(node);
            }
        }

        if (mLatchState == 0 && cStateUpdater.compareAndSet(this, 0, 0x80000000)) {
            // Latch just became available, so don't park. Node will be removed later.
            cWaiterUpdater.lazySet(node, null);
        } else {
            do {
                LockSupport.park(this);
            } while (node.mWaiter != null);
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

                Thread waiter = first.mWaiter;
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
        volatile Thread mWaiter;

        @Override
        public String toString() {
            return getClass().getName() + '@' + Integer.toHexString(hashCode()) +
                " {waiter=" + mWaiter +
                '}';
        }
    }
}
