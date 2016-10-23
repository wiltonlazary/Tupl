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

/**
 * Base class for any object which can own or acquire locks.
 *
 * @author Brian S O'Neill
 */
/*P*/
abstract class LockOwner {
    private final int mHash;

    // LockOwner is currently waiting to acquire this lock. Used for deadlock detection.
    Lock mWaitingFor;

    LockOwner() {
        mHash = Utils.cheapRandom();
    }

    @Override
    public final int hashCode() {
        return mHash;
    }

    public abstract void attach(Object obj);

    public abstract Object attachment();
}
