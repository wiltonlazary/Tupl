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

import java.util.concurrent.atomic.LongAdder;

import java.util.concurrent.locks.LockSupport;

/**
 * A clutch is a specialized latch which can support highly concurrent shared requests, under
 * the assumption that exclusive requests are infrequent. When too many shared requests are
 * denied due to high contention, the clutch switches to a special contended mode. Later, when
 * an exclusive clutch is acquired, the mode switches back to non-contended mode. This design
 * allows the clutch to be adaptive, by relying on the exclusive clutch as a signal that access
 * patterns have changed.
 *
 * @author Brian S O'Neill
 */
class Clutch extends AltLatch {
    // Inherited latch methods are used for non-contended mode, and for switching to it.

    // Lock is set when in contended mode. Proper deadlock-free order permits the contended
    // lock to be acquired when the latch is held, but not the other way around.
    private volatile Contended mLock;

    Clutch() {
    }

    /**
     * @param initialState UNLATCHED, EXCLUSIVE, SHARED or CONTENDED
     */
    Clutch(int initialState) {
        super(initialState);
    }

    @Override
    public boolean tryAcquireExclusive() {
        if (!super.tryAcquireExclusive()) {
            return false;
        }
        Contended lock = mLock;
        if (lock != null) {
            if (!lock.contendedTryAcquireExclusive()) {
                super.releaseExclusive();
                return false;
            }
            mLock = null;
            lock.contendedReleaseExclusive();
            // FIXME: recycle lock instances
        }
        return true;
    }

    //public boolean tryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException;

    @Override
    public void acquireExclusive() {
        super.acquireExclusive();
        Contended lock = mLock;
        if (lock != null) {
            lock.contendedAcquireExclusive();
            mLock = null;
            lock.contendedReleaseExclusive();
            // FIXME: recycle lock instances
        }
    }

    //public void acquireExclusiveInterruptibly() throws InterruptedException {

    @Override
    public boolean tryAcquireShared() {
        while (true) {
            Contended lock = mLock;
            if (lock == null) {
                break;
            }
            if (lock.contendedTryAcquireShared(this)) {
                return true;
            }
            break;
        }

        if (super.tryAcquireShared()) {
            Contended lock = mLock;
            if (lock != null) {
                if (!lock.contendedTryAcquireShared(this)) {
                    throw new AssertionError();
                }
                super.releaseShared();
            }
            return true;
        }

        return false;
    }

    //public boolean tryAcquireSharedNanos(long nanosTimeout) throws InterruptedException;

    @Override
    public boolean weakAcquireShared() {
        while (true) {
            Contended lock = mLock;
            if (lock == null) {
                break;
            }
            if (lock.contendedTryAcquireShared(this)) {
                return true;
            }
            break;
        }

        if (super.weakAcquireShared()) {
            Contended lock = mLock;
            if (lock != null) {
                if (!lock.contendedTryAcquireShared(this)) {
                    throw new AssertionError();
                }
                super.releaseShared();
            }
            return true;
        }

        return false;
    }

    @Override
    public void acquireShared() {
        if (!weakAcquireShared()) {
            super.acquireExclusive();

            Contended lock = mLock;
            if (lock == null) {
                // FIXME: recycle lock instances
                lock = new Contended();
                lock.takeOwnership(this);
                mLock = lock;
            } else if (!lock.contendedTryAcquireShared(this)) {
                throw new AssertionError();
            }

            super.releaseExclusive();
        }
    }

    //public void acquireSharedInterruptibly() throws InterruptedException;

    @Override
    public boolean tryUpgrade() {
        // With shared clutch held, another thread cannot switch to contended mode. Hence, no
        // double check is required here.
        return mLock == null && super.tryUpgrade();
    }

    @Override
    public void releaseShared() {
        // TODO: can be non-volatile read
        Contended lock = mLock;
        if (lock != null) {
            lock.contendedReleaseShared();
        } else {
            super.releaseShared();
        }
    }

    @Override
    public String toString() {
        if (mLock == null) {
            return super.toString();
        }
        StringBuilder b = new StringBuilder();
        Utils.appendMiniString(b, this);
        return b.append(" {state=").append("contended").append('}').toString();
    }

    /**
     * Specialized non-reentrant variant of CommitLock.
     */
    static class Contended extends AltLatch {
        private final LongAdder mSharedAcquire = new LongAdder();
        private final LongAdder mSharedRelease = new LongAdder();

        private volatile Thread mExclusiveThread;
        private volatile Clutch mOwner;

        void takeOwnership(Clutch owner) {
            mOwner = owner;
            mSharedAcquire.increment();
        }

        /**
         * @return false if exclusive is requested or ownership has changed; this indicates
         * that the clutch is leaving the contended state
         */
        boolean contendedTryAcquireShared(Clutch owner) {
            mSharedAcquire.increment();
            if (mExclusiveThread == null && mOwner == owner) {
                return true;
            }
            contendedReleaseShared();
            return false;
        }

        void contendedReleaseShared() {
            mSharedRelease.increment();
            Thread t = mExclusiveThread;
            if (t != null && !hasSharedLockers()) {
                LockSupport.unpark(t);
            }
        }

        boolean contendedTryAcquireExclusive() {
            if (super.tryAcquireExclusive()) {
                // Signal that shared locks cannot be granted anymore.
                mExclusiveThread = Thread.currentThread();

                if (!hasSharedLockers()) {
                    mOwner = null;
                    return true;
                }

                contendedReleaseExclusive();
            }

            return false;
        }

        void contendedAcquireExclusive() {
            super.acquireExclusive();

            // Signal that shared locks cannot be granted anymore.
            mExclusiveThread = Thread.currentThread();

            // Wait for shared locks to be released.
            while (hasSharedLockers()) {
                LockSupport.park(this);
            }

            mOwner = null;
        }

        void contendedReleaseExclusive() {
            mExclusiveThread = null;
            super.releaseExclusive();
        }

        private boolean hasSharedLockers() {
            // Ordering is important here. It prevents observing a release too soon.
            return mSharedRelease.sum() != mSharedAcquire.sum();
        }
    }
}
