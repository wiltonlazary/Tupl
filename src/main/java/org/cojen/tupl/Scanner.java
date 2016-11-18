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

import java.io.IOException;

/**
 * Supports accessing all entries within a {@link View}, in the same fashion as an {@link
 * java.util.Iterator Iterator}. Scanner instances can only be safely used by one thread at a
 * time, and they must be {@link #reset reset} when no longer needed. Instances can be
 * exchanged by threads, as long as a happens-before relationship is established. Without
 * proper exclusion, multiple threads interacting with a Scanner instance may cause database
 * corruption.
 *
 * <p>Methods which return {@link LockResult} might acquire a lock to access the requested
 * entry. The return type indicates if the lock is still {@link LockResult#isHeld held}, and in
 * what fashion. Except where indicated, a {@link LockTimeoutException} is thrown when a lock
 * cannot be acquired in time. When scanner is {@link #link linked} to a transaction, it
 * defines the locking behavior and timeout. Otherwise, a lock is always acquired, with the
 * {@link DatabaseConfig#lockTimeout default} timeout.
 *
 * <p>If a {@link LockFailureException} is thrown from any method, the Scanner
 * is positioned at the desired key, but the value is {@link #NOT_LOADED}.
 *
 * @author Brian S O'Neill
 * @see View#newScanner View.newScanner
 */
public interface Scanner {
    /**
     * Empty marker which indicates that value exists but has not been {@link #load loaded}.
     */
    public static final byte[] NOT_LOADED = new byte[0];

    /**
     * Returns the transaction the scanner is linked to.
     */
    public Transaction link();

    /**
     * Returns true if autoload mode is enabled.
     */
    public boolean autoload();

    /**
     * Returns an uncopied reference to the current key, or null if Scanner is
     * unpositioned. Array contents must not be modified.
     */
    public byte[] key();

    /**
     * Returns an uncopied reference to the current value, which might be null
     * or {@link #NOT_LOADED}. Array contents can be safely modified.
     */
    public byte[] value();

    /**
     * Moves the Scanner by a relative amount of entries. If less than the given amount of
     * entries are available, the Scanner is reset.
     *
     * <p>Skipping by 1 is equivalent to calling {@link #next next}, and a skip of 0 merely
     * checks and returns the lock state for the current key. Lock acquisition only applies to
     * the target entry &mdash; no locks are acquired for entries in between.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if position is undefined at invocation time
     * @throws IllegalArgumentException if negative skip isn't supported
     */
    public LockResult skip(long amount) throws IOException;

    /**
     * Moves to the Scanner to the next available entry. Scanner key and value
     * are set to null if no next entry exists, and position will be undefined.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult next() throws IOException;

    /**
     * Loads or reloads the value at the scanner's current position. Scanner
     * value is set to null if entry no longer exists, but the position remains
     * unmodified.
     *
     * @throws IllegalStateException if position is undefined at invocation time
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    public LockResult load() throws IOException;

    /**
     * Stores a value into the current entry, leaving the position
     * unchanged. An entry may be inserted, updated or deleted by this
     * method. A null value deletes the entry. Unless an exception is thrown,
     * the object returned by the {@link #value value} method will be the same
     * instance as was provided to this method.
     *
     * @param value value to store; pass null to delete
     * @throws IllegalStateException if position is undefined at invocation time
     * @throws ViewConstraintException if value is not permitted
     * @throws UnsupportedOperationException if not supported by scanner implementation
     */
    public void store(byte[] value) throws IOException;

    /**
     * Combined store and commit to the linked transaction. Although similar to storing and
     * committing explicitly, additional optimizations can be applied. In particular, no undo
     * log entry is required when committing the outermost transaction scope. This is the same
     * optimization used by null transactions (auto-commit).
     *
     * @param value value to store; pass null to delete
     * @throws IllegalStateException if position is undefined at invocation time
     * @throws ViewConstraintException if value is not permitted
     * @throws UnsupportedOperationException if not supported by scanner implementation
     */
    public default void commit(byte[] value) throws IOException {
        ViewUtils.commit(this, value);
    }

    /**
     * Resets Scanner and moves it to an undefined position. The key and value references are
     * also cleared.
     */
    public void reset();
}
