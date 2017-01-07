/*
 *  Copyright 2016 Cojen.org
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

import java.io.InterruptedIOException;

import java.lang.ref.WeakReference;

import java.util.concurrent.atomic.LongAdder;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

import java.util.concurrent.TimeUnit;

/**
 * Lock implementation which supports highly concurrent shared requests, but exclusive requests
 * are a little more expensive. Shared lock acquisition is reentrant, but exclusive is not.
 *
 * @author Brian S O'Neill
 */
final class CommitLock implements Lock {
    // See: "Using LongAdder to make a Reader-Writer Lock" by Concurrency Freaks, and also
    // "NUMA-Aware Reader Writer Locks".

    private final LongAdder mSharedAcquire = new LongAdder();
    private final LongAdder mSharedRelease = new LongAdder();

    private final AltLatch mFullLatch = new AltLatch();

    private volatile Thread mExclusiveThread;

    /**
     * Shared acquire counter, for supporting reentrancy.
     */
    static final class Shared extends WeakReference<CommitLock> {
        int count;

        Shared(CommitLock lock) {
            super(lock);
        }

        void release() {
            CommitLock lock = get();
            if (lock != null) {
                lock.releaseShared();
            }
            count--;
        }
    }

    private final ThreadLocal<Shared> mShared = ThreadLocal.withInitial(() -> new Shared(this));

    /**
     * Acquire shared lock.
     */
    @Override
    public boolean tryLock() {
        return tryAcquireShared() != null;
    }

    /**
     * Acquire shared lock.
     *
     * @return shared object to unlock; is null if acquire failed
     */
    Shared tryAcquireShared() {
        mSharedAcquire.increment();
        Shared shared = mShared.get();
        if (mExclusiveThread != null && shared.count == 0) {
            releaseShared();
            return null;
        } else {
            shared.count++;
            return shared;
        }
    }

    /**
     * Acquire shared lock.
     */
    @Override
    public void lock() {
        acquireShared();
    }

    /**
     * Acquire shared lock.
     *
     * @return shared object to unlock
     */
    Shared acquireShared() {
        mSharedAcquire.increment();
        Shared shared = mShared.get();
        if (mExclusiveThread != null && shared.count == 0) {
            releaseShared();
            mFullLatch.acquireShared();
            try {
                mSharedAcquire.increment();
            } finally {
                mFullLatch.releaseShared();
            }
        }
        shared.count++;
        return shared;
    }

    /**
     * Acquire shared lock.
     */
    @Override
    public void lockInterruptibly() throws InterruptedException {
        acquireSharedInterruptibly();
    }

    /**
     * Acquire shared lock.
     *
     * @return shared object to unlock
     */
    Shared acquireSharedInterruptibly() throws InterruptedException {
        mSharedAcquire.increment();
        Shared shared = mShared.get();
        if (mExclusiveThread != null && shared.count == 0) {
            releaseShared();
            mFullLatch.acquireSharedInterruptibly();
            try {
                mSharedAcquire.increment();
            } finally {
                mFullLatch.releaseShared();
            }
        }
        shared.count++;
        return shared;
    }

    /**
     * Acquire shared lock.
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return tryAcquireShared(time, unit) != null;
    }

    /**
     * Acquire shared lock.
     *
     * @return shared object to unlock; is null if acquire failed
     */
    Shared tryAcquireShared(long time, TimeUnit unit) throws InterruptedException {
        mSharedAcquire.increment();
        Shared shared = mShared.get();
        if (mExclusiveThread != null && shared.count == 0) {
            releaseShared();
            if (time < 0) {
                mFullLatch.acquireShared();
            } else if (time == 0 || !mFullLatch.tryAcquireSharedNanos(unit.toNanos(time))) {
                return null;
            }
            try {
                mSharedAcquire.increment();
            } finally {
                mFullLatch.releaseShared();
            }
        }
        shared.count++;
        return shared;
    }

    /**
     * Release shared lock.
     */
    @Override
    public void unlock() {
        releaseShared();
        mShared.get().count--;
    }

    void releaseShared() {
        mSharedRelease.increment();
        Thread t = mExclusiveThread;
        if (t != null && !hasSharedLockers()) {
            LockSupport.unpark(t);
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    void acquireExclusive() throws InterruptedIOException {
        // If full exclusive lock cannot be immediately obtained, it's due to a shared lock
        // being held for a long time. While waiting for the exclusive lock, all other shared
        // requests are queued. By waiting a timed amount and giving up, the exclusive lock
        // request is effectively de-prioritized. For each retry, the timeout is doubled, to
        // ensure that the exclusive request is not starved.

        long nanosTimeout = 1000; // 1 microsecond
        while (!finishAcquireExclusive(nanosTimeout)) {
            nanosTimeout <<= 1;
        }
    }

    private boolean finishAcquireExclusive(long nanosTimeout) throws InterruptedIOException {
        try {
            mFullLatch.acquireExclusiveInterruptibly();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }

        // Signal that shared locks cannot be granted anymore.
        mExclusiveThread = Thread.currentThread();

        try {
            if (hasSharedLockers()) {
                // Wait for shared locks to be released.

                long nanosEnd = nanosTimeout <= 0 ? 0 : System.nanoTime() + nanosTimeout;

                while (true) {
                    if (nanosTimeout < 0) {
                        LockSupport.park(this);
                    } else {
                        LockSupport.parkNanos(this, nanosTimeout);
                    }

                    if (Thread.interrupted()) {
                        throw new InterruptedIOException();
                    }

                    if (!hasSharedLockers()) {
                        break;
                    }

                    if (nanosTimeout >= 0 &&
                        (nanosTimeout == 0 || (nanosTimeout = nanosEnd - System.nanoTime()) <= 0))
                    {
                        mExclusiveThread = null;
                        mFullLatch.releaseExclusive();
                        return false;
                    }
                }
            }

            mShared.get().count++;
            return true;
        } catch (Throwable e) {
            mExclusiveThread = null;
            mFullLatch.releaseExclusive();
            throw e;
        }
    }

    void releaseExclusive() {
        mExclusiveThread = null;
        mFullLatch.releaseExclusive();
        mShared.get().count--;
    }

    boolean hasQueuedThreads() {
        return mFullLatch.hasQueuedThreads();
    }

    private boolean hasSharedLockers() {
        // Ordering is important here. It prevents observing a release too soon.
        return mSharedRelease.sum() != mSharedAcquire.sum();
    }
}
