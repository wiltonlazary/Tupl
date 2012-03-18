/*
 *  Copyright 2011 Brian S O'Neill
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

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Used internally by Locker. Only detects deadlocks caused by independent
 * threads. A thread "self deadlock" caused by separate lockers in the same
 * thread is not detected. This is because there is only one thread blocked.
 * The detector relies on multiple threads to be blocked waiting on a lock.
 * Lockers aren't registered with any specific thread, and therefore locks
 * cannot be owned by threads. If this policy changes, then the detector could
 * see that the thread is self deadlocked.
 *
 * @author Brian S O'Neill
 */
class DeadlockDetector {
    // Note: This code does not consider proper thread-safety and directly
    // examines the contents of locks and lockers. It never modifies anything,
    // so it is relatively safe and deadlocks are usually detectable. All
    // involved threads had to acquire latches at some point, which implies a
    // memory barrier.

    private final Locker mOrigin;
    private final Set<Locker> mLockers;

    private boolean mGuilty;

    DeadlockDetector(Locker locker, long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException
    {
        mOrigin = locker;
        mLockers = new LinkedHashSet<Locker>();
        if (scan(locker)) {
            throw new DeadlockException(nanosTimeout, mGuilty);
        }
    }

    /**
     * @return true if deadlock was found
     */
    private boolean scan(Locker locker) {
        boolean found = false;

        outer: while (true) {
            Lock lock = locker.mWaitingFor;
            if (lock == null) {
                return found;
            }

            if (mLockers.isEmpty()) {
                mLockers.add(locker);
            } else {
                // Any graph edge flowing into the original locker indicates guilt.
                mGuilty |= mOrigin == locker;
                if (!mLockers.add(locker)) {
                    return true;
                }
            }

            Locker owner = lock.mLocker;
            Object shared = lock.mSharedLockersObj;

            // If the owner is the locker, then it is trying to upgrade. It's
            // waiting for another locker to release the shared lock.
            if (owner != null && owner != locker) {
                if (shared == null) {
                    // Tail call.
                    locker = owner;
                    continue outer;
                }
                found |= scan(owner);
            }

            if (shared != null) {
                if (shared instanceof Locker) {
                    // Tail call.
                    locker = (Locker) shared;
                    continue outer;
                }

                Lock.LockerHTEntry[] entries = (Lock.LockerHTEntry[]) shared;
                for (int i=entries.length; --i>=0; ) {
                    for (Lock.LockerHTEntry e = entries[i]; e != null; ) {
                        Lock.LockerHTEntry next = e.mNext;
                        if (i == 0 && next == null) {
                            // Tail call.
                            locker = e.mLocker;
                            continue outer;
                        }
                        found |= scan(e.mLocker);
                        e = next;
                    }
                }
            }

            return found;
        }
    }
}
