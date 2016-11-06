/*
 *  Copyright 2011-2015 Cojen.org
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

import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.Lock;

/**
 * Defines a logical unit of work. Transaction instances can only be safely
 * used by one thread at a time, and they must be {@link #reset reset} when no
 * longer needed. Instances can be exchanged by threads, as long as a
 * happens-before relationship is established. Without proper exclusion,
 * multiple threads interacting with a Transaction instance may cause database
 * corruption.
 *
 * <p>Transactions also contain various methods for directly controlling locks,
 * although their use is not required. Methods which operate upon transactions
 * acquire and release locks automatically. Direct control over locks is
 * provided for advanced use cases. One such use is record filtering:
 *
 * <pre>
 * Transaction txn = ...
 * Cursor c = index.newCursor(txn);
 * for (LockResult result = c.first(); c.key() != null; result = c.next()) {
 *     if (shouldDiscard(c.value()) &amp;&amp; result == LockResult.ACQUIRED) {
 *         // Unlock record which doesn't belong in the transaction.
 *         txn.unlock();
 *         continue;
 *     }
 *     ...
 * }
 * </pre>
 *
 * <p>Note: Transaction instances are never fully closed after they are reset
 * or have fully exited. Any operation which acts upon a reset transaction can
 * resurrect it.
 *
 * @author Brian S O'Neill
 * @see Database#newTransaction Database.newTransaction
 */
public interface Transaction {
    /**
     * Transaction instance which isn't a transaction at all. It always
     * operates in an {@link LockMode#UNSAFE unsafe} lock mode and a {@link
     * DurabilityMode#NO_REDO no-redo} durability mode. For safe auto-commit
     * transactions, pass null for the transaction argument.
     */
    public static final Transaction BOGUS = LocalTransaction.BOGUS;

    /**
     * Sets the lock mode for the current scope. Transactions begin in {@link
     * LockMode#UPGRADABLE_READ UPGRADABLE_READ} mode, and newly entered scopes
     * begin at the outer scope's current mode. Exiting a scope reverts the
     * lock mode.
     *
     * @param mode new lock mode
     * @throws IllegalArgumentException if mode is null
     */
    void lockMode(LockMode mode);

    /**
     * Returns the current lock mode.
     */
    LockMode lockMode();

    /**
     * Sets the lock timeout for the current scope. A negative timeout is
     * infinite.
     *
     * @param unit required unit if timeout is more than zero
     */
    void lockTimeout(long timeout, TimeUnit unit);

    /**
     * Returns the current lock timeout, in the given unit.
     */
    long lockTimeout(TimeUnit unit);

    /**
     * Sets the durability mode for the entire transaction, not just the current scope. The
     * durability mode primarily affects commit behavior at the top-level scope, but it can also
     * be used to switch redo logging behavior.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param mode new durability mode
     * @throws IllegalArgumentException if mode is null
     */
    void durabilityMode(DurabilityMode mode);

    /**
     * Returns the durability mode of this transaction.
     */
    DurabilityMode durabilityMode();

    /**
     * Checks the validity of the transaction.
     *
     * @throws InvalidTransactionException if transaction is broken or was invalidated by
     * an earlier exception
     * @throws IllegalStateException if transaction is bogus
     */
    void check() throws DatabaseException;

    /**
     * Commits all modifications made within the current transaction scope. The
     * current scope is still valid after this method is called, unless an
     * exception is thrown. Call exit or reset to fully release transaction
     * resources.
     */
    void commit() throws IOException;

    /**
     * Commits and exits all transaction scopes.
     */
    void commitAll() throws IOException;

    /**
     * Enters a nested transaction scope.
     */
    void enter() throws IOException;

    /**
     * Exits the current transaction scope, rolling back all uncommitted
     * modifications made within. The transaction is still valid after this
     * method is called, unless an exception is thrown.
     */
    void exit() throws IOException;

    /**
     * Exits all transaction scopes, rolling back all uncommitted modifications. Equivalent to:
     *
     * <pre>
     * while (txn.isNested()) {
     *     txn.exit();
     * }
     * txn.exit();
     * </pre>
     */
    void reset() throws IOException;

    /**
     * Reset the transaction due to the given cause. This provides an opportunity to prevent
     * this transaction from being used any further. No exception is thrown when invoking this
     * method.
     *
     * @param cause pass a cause to reset and disable the transaction; pass null to simply
     * reset the transaction and ignore any exception when doing so
     */
    default void reset(Throwable cause) {
        try {
            reset();
        } catch (Throwable e) {
            // Ignore.
        }
    }

    /**
     * Attempts to acquire a shared lock for the given key, denying exclusive
     * locks. If return value is {@link LockResult#alreadyOwned owned}, transaction
     * already owns a strong enough lock, and no extra unlock should be
     * performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if too many shared locks
     * @throws LockFailureException if interrupted or timed out
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    LockResult lockShared(long indexId, byte[] key) throws LockFailureException;

    /**
     * Attempts to acquire an upgradable lock for the given key, denying
     * exclusive and additional upgradable locks. If return value is {@link
     * LockResult#alreadyOwned owned}, transaction already owns a strong enough
     * lock, and no extra unlock should be performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    LockResult lockUpgradable(long indexId, byte[] key) throws LockFailureException;

    /**
     * Attempts to acquire an exclusive lock for the given key, denying any
     * additional locks. If return value is {@link LockResult#alreadyOwned owned},
     * transaction already owns exclusive lock, and no extra unlock should be
     * performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link LockResult#UPGRADED
     * UPGRADED}, or {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    LockResult lockExclusive(long indexId, byte[] key) throws LockFailureException;

    /**
     * Supply a message for a custom redo handler. Redo operations should be paired with undo
     * operations.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param message message to pass to transaction handler
     * @param indexId index for lock acquisition; zero if not applicable
     * @param key key which has been locked exclusively; null if not applicable
     * @throws IllegalStateException if no transaction handler is installed
     * @see org.cojen.tupl.ext.TransactionHandler
     */
    void customRedo(byte[] message, long indexId, byte[] key) throws IOException;

    /**
     * Supply a message for a custom undo handler. Undo operations should be paired with redo
     * operations.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param message message to pass to transaction handler
     * @throws IllegalStateException if no transaction handler is installed
     * @see org.cojen.tupl.ext.TransactionHandler
     */
    void customUndo(byte[] message) throws IOException;

    /**
     * Returns true if the current transaction scope is nested.
     */
    boolean isNested();

    /**
     * Counts the current transaction scope nesting level. Count is zero if non-nested.
     */
    int nestingLevel();

    /**
     * Attempts to acquire a shared lock for the given key, denying exclusive
     * locks. If return value is {@link LockResult#alreadyOwned owned}, transaction
     * already owns a strong enough lock, and no extra unlock should be
     * performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#INTERRUPTED INTERRUPTED}, {@link
     * LockResult#TIMED_OUT_LOCK TIMED_OUT_LOCK}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if too many shared locks
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    LockResult tryLockShared(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException;

    /**
     * Attempts to acquire a shared lock for the given key, denying exclusive
     * locks. If return value is {@link LockResult#alreadyOwned owned}, transaction
     * already owns a strong enough lock, and no extra unlock should be
     * performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if too many shared locks
     * @throws LockFailureException if interrupted or timed out
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    LockResult lockShared(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException;

    /**
     * Attempts to acquire an upgradable lock for the given key, denying
     * exclusive and additional upgradable locks. If return value is {@link
     * LockResult#alreadyOwned owned}, transaction already owns a strong enough
     * lock, and no extra unlock should be performed. If {@link
     * LockResult#ILLEGAL ILLEGAL} is returned, transaction holds a shared
     * lock, which cannot be upgraded.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ILLEGAL ILLEGAL}, {@link
     * LockResult#INTERRUPTED INTERRUPTED}, {@link LockResult#TIMED_OUT_LOCK
     * TIMED_OUT_LOCK}, {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    LockResult tryLockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException;

    /**
     * Attempts to acquire an upgradable lock for the given key, denying
     * exclusive and additional upgradable locks. If return value is {@link
     * LockResult#alreadyOwned owned}, transaction already owns a strong enough
     * lock, and no extra unlock should be performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    LockResult lockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException;

    /**
     * Attempts to acquire an exclusive lock for the given key, denying any
     * additional locks. If return value is {@link LockResult#alreadyOwned
     * owned}, transaction already owns exclusive lock, and no extra unlock
     * should be performed. If {@link LockResult#ILLEGAL ILLEGAL} is returned,
     * transaction holds a shared lock, which cannot be upgraded.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ILLEGAL ILLEGAL}, {@link
     * LockResult#INTERRUPTED INTERRUPTED}, {@link LockResult#TIMED_OUT_LOCK
     * TIMED_OUT_LOCK}, {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#UPGRADED UPGRADED}, or {@link LockResult#OWNED_EXCLUSIVE
     * OWNED_EXCLUSIVE}
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    LockResult tryLockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException;

    /**
     * Attempts to acquire an exclusive lock for the given key, denying any
     * additional locks. If return value is {@link LockResult#alreadyOwned owned},
     * transaction already owns exclusive lock, and no extra unlock should be
     * performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link LockResult#UPGRADED
     * UPGRADED}, or {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    LockResult lockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException;

    /**
     * Checks the lock ownership for the given key.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link
     * LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    LockResult lockCheck(long indexId, byte[] key);

    /**
     * Returns the index id of the last lock acquired, within the current scope.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @return locked index id
     * @throws IllegalStateException if no locks held
     */
    long lastLockedIndex();

    /**
     * Returns the key of the last lock acquired, within the current scope.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @return locked key; instance is not cloned
     * @throws IllegalStateException if no locks held
     */
    byte[] lastLockedKey();

    /**
     * Fully releases last lock acquired, within the current scope. If the last
     * lock operation was an upgrade, for a lock not immediately acquired,
     * unlock is not allowed. Instead, an IllegalStateException is thrown.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i> Also, the current
     * implementation does not accurately track scopes. It may permit an unlock operation to
     * cross a scope boundary, which has undefined behavior.
     *
     * @throws IllegalStateException if no locks held, or if unlocking a
     * non-immediate upgrade
     */
    void unlock();

    /**
     * Releases last lock acquired, within the current scope, retaining a
     * shared lock. If the last lock operation was an upgrade, for a lock not
     * immediately acquired, unlock is not allowed. Instead, an
     * IllegalStateException is thrown.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i> Also, the current
     * implementation does not accurately track scopes. It may permit an unlock operation to
     * cross a scope boundary, which has undefined behavior.
     *
     * @throws IllegalStateException if no locks held, or if too many shared
     * locks, or if unlocking a non-immediate upgrade
     */
    void unlockToShared();

    /**
     * Releases last lock acquired or upgraded, within the current scope,
     * retaining an upgradable lock.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i> Also, the current
     * implementation does not accurately track scopes. It may permit an unlock operation to
     * cross a scope boundary, which has undefined behavior.
     *
     * @throws IllegalStateException if no locks held, or if last lock is shared
     */
    void unlockToUpgradable();

    /**
     * Attach an arbitrary object to this transaction instance, for tracking it. Attachments
     * can become visible to other threads as a result of a {@link LockTimeoutException}. A
     * happens-before relationship is guaranteed only if the attachment is set before the
     * associated lock is acquired. Also, the LockTimeoutException constructor calls toString
     * on the attachment, and includes it in the message. A non-default toString implementation
     * must consider thread-safety.
     *
     * @param obj the object to be attached; may be null
     */
    default void attach(Object obj) {
        // Throw an exception for compatibility. All known implementations support the feature.
        throw new UnsupportedOperationException();
    }

    /**
     * Returns any attachment which was set earlier.
     *
     * @return the current attachment, or null if none
     */
    default Object attachment() {
        return null;
    }
}
