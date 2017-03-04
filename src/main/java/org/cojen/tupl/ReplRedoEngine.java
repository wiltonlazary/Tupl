/*
 *  Copyright 2012-2015 Cojen.org
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

import java.lang.ref.SoftReference;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.cojen.tupl.ext.ReplicationManager;

import org.cojen.tupl.util.Latch;

import static org.cojen.tupl.Utils.*;

import org.cojen.tupl.ext.TransactionHandler;

/**
 * 
 *
 * @author Brian S O'Neill
 */
/*P*/
final class ReplRedoEngine implements RedoVisitor {
    private static final long INFINITE_TIMEOUT = -1L;
    private static final int MIN_SPINS = 20;
    private static final int MAX_SPINS = 2000;

    // Hash spreader. Based on rounded value of 2 ** 63 * (sqrt(5) - 1) equivalent 
    // to unsigned 11400714819323198485.
    private static final long HASH_SPREAD = -7046029254386353131L;

    final ReplicationManager mManager;
    final LocalDatabase mDatabase;

    final ReplRedoController mController;

    // Maintain soft references to indexes, allowing them to get closed if not
    // used for awhile. Without the soft references, Database maintains only
    // weak references to indexes. They'd get closed too soon.
    private final LHashTable.Obj<SoftReference<Index>> mIndexes;

    private final Latch[] mLatches;
    private final int mLatchesMask;

    private final TxnTable mTransactions;

    private final int mMaxThreads;
    private final AtomicInteger mTotalThreads;
    private final AtomicInteger mIdleThreads;
    private final ConcurrentMap<DecodeTask, Object> mTaskThreadSet;

    // Latch must be held exclusively while reading from decoder.
    private final Latch mDecodeLatch;
    private final Latch mCustomLatch;

    private final AtomicInteger mCommitSpins;
    private final AtomicInteger mDecodeSpins;

    private ReplRedoDecoder mDecoder;

    // Shared latch held when applying operations. Checkpoint suspends all tasks by acquiring
    // an exclusive latch. If any operation fails to be applied, shared latch is still held,
    // preventing checkpoints.
    final Latch mOpLatch;

    // Updated with exclusive decode latch and shared op latch. Values can be read with op
    // latch exclusively held, when engine is suspended.
    long mDecodePosition;
    long mDecodeTransactionId;

    /**
     * @param manager already started
     * @param txns recovered transactions; can be null; cleared as a side-effect
     */
    ReplRedoEngine(ReplicationManager manager, int maxThreads,
                   LocalDatabase db, LHashTable.Obj<LocalTransaction> txns)
        throws IOException
    {
        if (maxThreads <= 0) {
            int procs = Runtime.getRuntime().availableProcessors();
            maxThreads = maxThreads == 0 ? procs : (-maxThreads * procs);
            if (maxThreads <= 0) {
                maxThreads = Integer.MAX_VALUE;
            }
        }

        mManager = manager;
        mDatabase = db;

        mController = new ReplRedoController(this);

        mIndexes = new LHashTable.Obj<>(16);

        mDecodeLatch = new Latch();
        mCustomLatch = new Latch();
        mOpLatch = new Latch();

        mCommitSpins = new AtomicInteger(); 
        mDecodeSpins = new AtomicInteger(); 

        mMaxThreads = maxThreads;
        mTotalThreads = new AtomicInteger();
        mIdleThreads = new AtomicInteger();
        mTaskThreadSet = new ConcurrentHashMap<>(16, 0.75f, 1);

        int latchCount = roundUpPower2(maxThreads * 2);
        if (latchCount <= 0) {
            latchCount = 1 << 30;
        }

        mLatches = new Latch[latchCount];
        mLatchesMask = mLatches.length - 1;
        for (int i=0; i<mLatches.length; i++) {
            mLatches[i] = new Latch();
        }

        final TxnTable txnTable;
        if (txns == null) {
            txnTable = new TxnTable(16);
        } else {
            txnTable = new TxnTable(txns.size());

            txns.traverse((entry) -> {
                // Reduce hash collisions.
                long scrambledTxnId = mix(entry.key);
                Latch latch = selectLatch(scrambledTxnId);
                LocalTransaction txn = entry.value;
                if (!txn.recoveryCleanup(false)) {
                    txnTable.insert(scrambledTxnId).init(txn, latch);
                }
                // Delete entry.
                return true;
            });
        }

        mTransactions = txnTable;

        // Initialize the decode position early.
        mDecodeLatch.acquireExclusive();
        mDecodePosition = manager.readPosition();
        mDecodeLatch.releaseExclusive();
    }

    public RedoWriter initWriter(long redoNum) {
        mController.initCheckpointNumber(redoNum);
        return mController;
    }

    public void startReceiving(long initialPosition, long initialTxnId) {
        mDecodeLatch.acquireExclusive();
        if (mDecoder == null) {
            mOpLatch.acquireExclusive();
            try {
                try {
                    mDecoder = new ReplRedoDecoder(mManager, initialPosition, initialTxnId);
                } catch (Throwable e) {
                    mDecodeLatch.releaseExclusive();
                    throw e;
                }
                mDecodeTransactionId = initialTxnId;
                nextTask();
            } finally {
                mOpLatch.releaseExclusive();
            }
        } else {
            mDecodeLatch.releaseExclusive();
        }
    }

    @Override
    public boolean reset() throws IOException {
        // Acquire latch before performing operations with side-effects.
        mOpLatch.acquireShared();

        // Reset and discard all transactions.
        mTransactions.traverse((entry) -> {
            Latch latch = entry.latch();
            try {
                entry.mTxn.recoveryCleanup(true);
            } finally {
                latch.releaseExclusive();
            }
            return true;
        });

        // Although it might seem like a good time to clean out any lingering trash, concurrent
        // transactions are still active and need the trash to rollback properly.
        //mDatabase.emptyAllFragmentedTrash(false);

        // Only release if no exception.
        opFinishedShared();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean timestamp(long timestamp) {
        return nop();
    }

    @Override
    public boolean shutdown(long timestamp) {
        return nop();
    }

    @Override
    public boolean close(long timestamp) {
        return nop();
    }

    @Override
    public boolean endFile(long timestamp) {
        return nop();
    }

    @Override
    public boolean store(long indexId, byte[] key, byte[] value) throws IOException {
        Index ix = getIndex(indexId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        // Locks must be acquired in their original order to avoid
        // deadlock, so don't allow another task thread to run yet.
        Locker locker = mDatabase.mLockManager.localLocker();
        locker.lockExclusive(indexId, key, INFINITE_TIMEOUT);

        // Allow another task thread to run while operation completes.
        nextTask();

        try {
            while (ix != null) {
                try {
                    ix.store(Transaction.BOGUS, key, value);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    ix = openIndex(indexId, null);
                }
            }
        } finally {
            locker.scopeUnlockAll();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        notifyStore(ix, key, value);

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean storeNoLock(long indexId, byte[] key, byte[] value) throws IOException {
        // A no-lock change is created when using the UNSAFE lock mode. If the
        // application has performed its own locking, consistency can be
        // preserved by locking the index entry. Otherwise, the outcome is
        // unpredictable.

        return store(indexId, key, value);
    }

    @Override
    public boolean renameIndex(long txnId, long indexId, byte[] newName) throws IOException {
        Index ix = getIndex(indexId);
        byte[] oldName = null;

        // Acquire latch before performing operations with side-effects.
        mOpLatch.acquireShared();

        if (ix != null) {
            oldName = ix.getName();
            try {
                mDatabase.renameIndex(ix, newName, txnId);
            } catch (RuntimeException e) {
                EventListener listener = mDatabase.eventListener();
                if (listener != null) {
                    listener.notify(EventType.REPLICATION_WARNING,
                                    "Unable to rename index: %1$s", rootCause(e));
                    // Disable notification.
                    ix = null;
                }
            }
        }

        // Only release if no exception.
        opFinishedShared();

        if (ix != null) {
            try {
                mManager.notifyRename(ix, oldName, newName.clone());
            } catch (Throwable e) {
                uncaught(e);
            }
        }

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean deleteIndex(long txnId, long indexId) throws IOException {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Open the index with the transaction to prevent deadlock
        // when the instance is not cached and has to be loaded.
        Index ix = getIndex(txn, indexId);
        mIndexes.remove(indexId);

        // Acquire latch before performing operations with side-effects.
        mOpLatch.acquireShared();

        // Commit the transaction now and delete the index. See LocalDatabase.moveToTrash for
        // more info.
        Latch latch = te.latch();
        try {
            try {
                txn.commit();
            } finally {
                txn.exit();
            }
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        opFinishedShared();

        if (ix != null) {
            ix.close();
            try {
                mManager.notifyDrop(ix);
            } catch (Throwable e) {
                uncaught(e);
            }
        }

        Runnable task = mDatabase.replicaDeleteTree(indexId);

        if (task != null) {
            try {
                // Allow index deletion to run concurrently. If multiple deletes are received
                // concurrently, then the application is likely doing concurrent deletes.
                Thread deletion = new Thread
                    (task, "IndexDeletion-" + (ix == null ? indexId : ix.getNameString()));
                deletion.setDaemon(true);
                deletion.start();
            } catch (Throwable e) {
                EventListener listener = mDatabase.eventListener();
                if (listener != null) {
                    listener.notify(EventType.REPLICATION_WARNING,
                                    "Unable to immediately delete index: %1$s", rootCause(e));
                }
                // Index will get fully deleted when database is re-opened.
            }
        }

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnEnter(long txnId) throws IOException {
        // Reduce hash collisions.
        long scrambledTxnId = mix(txnId);
        TxnEntry e = mTransactions.get(scrambledTxnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        if (e == null) {
            LocalTransaction txn = newTransaction(txnId);
            mTransactions.insert(scrambledTxnId).init(txn, selectLatch(scrambledTxnId));

            // Only release if no exception.
            opFinishedShared();

            return true;
        }

        Latch latch = e.latch();
        try {
            // Cheap operation, so don't let another task thread run.
            e.mTxn.enter();
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        opFinishedShared();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnRollback(long txnId) throws IOException {
        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        Latch latch = te.latch();
        try {
            // Allow another task thread to run while operation completes.
            nextTask();

            te.mTxn.exit();
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean txnRollbackFinal(long txnId) throws IOException {
        // Acquire latch before performing operations with side-effects.
        mOpLatch.acquireShared();

        TxnEntry te = removeTxnEntry(txnId);

        if (te == null) {
            opFinishedShared();
            return true;
        }

        Latch latch = te.latch();
        try {
            // Allow another task thread to run while operation completes.
            nextTask();

            te.mTxn.reset();
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean txnCommit(long txnId) throws IOException {
        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        Latch latch = te.latch();
        try {
            // Commit is expected to complete quickly, so don't let another
            // task thread run.

            Transaction txn = te.mTxn;
            try {
                txn.commit();
            } finally {
                txn.exit();
            }
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        opFinishedShared();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnCommitFinal(long txnId) throws IOException {
        // Acquire latch before performing operations with side-effects.
        mOpLatch.acquireShared();

        TxnEntry te = removeTxnEntry(txnId);

        if (te != null) {
            Latch latch = te.latch(mCommitSpins);
            try {
                // Commit is expected to complete quickly, so don't let another
                // task thread run.

                te.mTxn.commitAll();
            } finally {
                latch.releaseExclusive();
            }
        }

        // Only release if no exception.
        opFinishedShared();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        Index ix = getIndex(indexId);
        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        Latch latch = te.latch();
        try {
            LocalTransaction txn = te.mTxn;

            // Locks must be acquired in their original order to avoid
            // deadlock, so don't allow another task thread to run yet.
            txn.lockUpgradable(indexId, key, INFINITE_TIMEOUT);

            // Allow another task thread to run while operation completes.
            nextTask();

            while (ix != null) {
                try {
                    ix.store(txn, key, value);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    ix = openIndex(indexId, null);
                }
            }
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        notifyStore(ix, key, value);

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        Index ix = getIndex(indexId);
        TxnEntry te = removeTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        LocalTransaction txn;
        Latch latch;

        if (te == null) {
            // Create the transaction, but don't store it in the transaction table.
            txn = newTransaction(txnId);
            // Latch isn't required because no other operations can access the transaction.
            latch = null;
        } else {
            txn = te.mTxn;
            latch = te.latch();
        }

        try {
            // Locks must be acquired in their original order to avoid
            // deadlock, so don't allow another task thread to run yet.
            txn.lockUpgradable(indexId, key, INFINITE_TIMEOUT);

            // Allow another task thread to run while operation completes.
            nextTask();

            while (ix != null) {
                try {
                    ix.store(txn, key, value);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    ix = openIndex(indexId, null);
                }
            }

            txn.commitAll();
        } finally {
            if (latch != null) {
                latch.releaseExclusive();
            }
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        notifyStore(ix, key, value);

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean txnLockShared(long txnId, long indexId, byte[] key) throws IOException {
        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        Latch latch = te.latch();
        try {
            te.mTxn.lockShared(indexId, key, INFINITE_TIMEOUT);
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnLockUpgradable(long txnId, long indexId, byte[] key) throws IOException {
        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        Latch latch = te.latch();
        try {
            te.mTxn.lockUpgradable(indexId, key, INFINITE_TIMEOUT);
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnLockExclusive(long txnId, long indexId, byte[] key) throws IOException {
        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        Latch latch = te.latch();
        try {
            te.mTxn.lockExclusive(indexId, key, INFINITE_TIMEOUT);
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnCustom(long txnId, byte[] message) throws IOException {
        TransactionHandler handler = mDatabase.mCustomTxnHandler;

        if (handler == null) {
            throw new DatabaseException("Custom transaction handler is not installed");
        }

        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();
        mCustomLatch.acquireExclusive();

        Latch latch = te.latch();
        try {
            handler.redo(mDatabase, te.mTxn, message);
        } finally {
            latch.releaseExclusive();
            mCustomLatch.releaseExclusive();
        }

        // Only release if no exception.
        opFinishedShared();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnCustomLock(long txnId, byte[] message, long indexId, byte[] key)
        throws IOException
    {
        TransactionHandler handler = mDatabase.mCustomTxnHandler;

        if (handler == null) {
            throw new DatabaseException("Custom transaction handler is not installed");
        }

        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        Latch latch = te.latch();
        try {
            LocalTransaction txn = te.mTxn;

            // Locks must be acquired in their original order to avoid
            // deadlock, so don't allow another task thread to run yet.
            txn.lockUpgradable(indexId, key, INFINITE_TIMEOUT);

            // Allow another task thread to run while operation completes.
            nextTask();

            // Wait to acquire exclusive now that another thread is running.
            txn.lockExclusive(indexId, key, INFINITE_TIMEOUT);

            handler.redo(mDatabase, txn, message, indexId, key);
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    /**
     * Called for an operation which is ignored.
     */
    private boolean nop() {
        mOpLatch.acquireShared();
        opFinishedShared();
        return true;
    }

    /**
     * Called after an operation is finished which didn't spawn a task thread. Caller must hold
     * shared op latch, which is released by this method. Decode latch must also be held, which
     * caller must release.
     */
    private void opFinishedShared() {
        doOpFinished();
        mOpLatch.releaseShared();
    }

    /**
     * Called after an operation is finished which didn't spawn a task thread. Caller must hold
     * exclusive op latch, which is released by this method. Decode latch must also be held,
     * which caller must release.
     */
    private void opFinishedExclusive() {
        doOpFinished();
        mOpLatch.releaseExclusive();
    }

    private void doOpFinished() {
        // Capture the position for the next operation. Also capture the last transaction id,
        // before a delta is applied.
        ReplRedoDecoder decoder = mDecoder;
        mDecodePosition = decoder.in().mPos;
        mDecodeTransactionId = decoder.mTxnId;
    }

    /**
     * Launch a task thread to continue processing more redo entries
     * concurrently. Caller must return false from the visitor method, to
     * prevent multiple threads from trying to decode the redo input stream. If
     * thread limit is reached, the remaining task threads continue working.
     *
     * Caller must hold exclusive decode latch, which is released by this method. Shared op
     * latch must also be held, which caller must release.
     */
    private void nextTask() {
        // Capture the position for the next operation. Also capture the last transaction id,
        // before a delta is applied.
        ReplRedoDecoder decoder = mDecoder;
        mDecodePosition = decoder.in().mPos;
        mDecodeTransactionId = decoder.mTxnId;

        if (mIdleThreads.get() == 0) {
            int total = mTotalThreads.get();
            if (total < mMaxThreads && mTotalThreads.compareAndSet(total, total + 1)) {
                DecodeTask task;
                try {
                    task = new DecodeTask();
                    task.start();
                } catch (Throwable e) {
                    mDecodeLatch.releaseExclusive();
                    mTotalThreads.getAndDecrement();
                    throw e;
                }
                mTaskThreadSet.put(task, this);
            }
        }

        // Allow task thread to proceed.
        mDecodeLatch.releaseExclusive();
    }

    /**
     * Waits for all incoming replication operations to finish and prevents new ones from
     * starting.
     */
    void suspend() {
        mOpLatch.acquireExclusive();
    }

    void resume() {
        mOpLatch.releaseExclusive();
    }

    /**
     * @return TxnEntry with scrambled transaction id
     */
    private TxnEntry getTxnEntry(long txnId) throws IOException {
        long scrambledTxnId = mix(txnId);
        TxnEntry e = mTransactions.get(scrambledTxnId);

        if (e == null) {
            // Create transaction on demand if necessary. Startup transaction recovery only
            // applies to those which generated undo log entries.
            LocalTransaction txn = newTransaction(txnId);
            e = mTransactions.insert(scrambledTxnId);
            e.init(txn, selectLatch(scrambledTxnId));
        }

        return e;
    }

    private LocalTransaction newTransaction(long txnId) {
        LocalTransaction txn = new LocalTransaction
            (mDatabase, txnId, LockMode.UPGRADABLE_READ, INFINITE_TIMEOUT);
        txn.attach("replication");
        return txn;
    }

    /**
     * @return TxnEntry with scrambled transaction id; null if not found
     */
    private TxnEntry removeTxnEntry(long txnId) throws IOException {
        long scrambledTxnId = mix(txnId);
        return mTransactions.remove(scrambledTxnId);
    }

    /**
     * Returns the index from the local cache, opening it if necessary.
     *
     * @return null if not found
     */
    private Index getIndex(Transaction txn, long indexId) throws IOException {
        LHashTable.ObjEntry<SoftReference<Index>> entry = mIndexes.get(indexId);
        if (entry != null) {
            Index ix = entry.value.get();
            if (ix != null) {
                return ix;
            }
        }
        return openIndex(txn, indexId, entry);
    }


    /**
     * Returns the index from the local cache, opening it if necessary.
     *
     * @return null if not found
     */
    private Index getIndex(long indexId) throws IOException {
        return getIndex(null, indexId);
    }

    /**
     * Opens the index and puts it into the local cache, replacing the existing entry.
     *
     * @return null if not found
     */
    private Index openIndex(Transaction txn, long indexId,
                            LHashTable.ObjEntry<SoftReference<Index>> entry)
        throws IOException
    {
        Index ix = mDatabase.anyIndexById(txn, indexId);
        if (ix == null) {
            return null;
        }

        SoftReference<Index> ref = new SoftReference<>(ix);
        if (entry == null) {
            mIndexes.insert(indexId).value = ref;
        } else {
            entry.value = ref;
        }

        if (entry != null) {
            // Remove entries for all other cleared references, freeing up memory.
            mIndexes.traverse((e) -> e.value.get() == null);
        }

        return ix;
    }

    /**
     * Opens the index and puts it into the local cache, replacing the existing entry.
     *
     * @return null if not found
     */
    private Index openIndex(long indexId, LHashTable.ObjEntry<SoftReference<Index>> entry)
        throws IOException
    {
        return openIndex(null, indexId, entry);
    }

    private Latch selectLatch(long scrambledTxnId) {
        return mLatches[((int) scrambledTxnId) & mLatchesMask];
    }

    private static final long IDLE_TIMEOUT_NANOS = 5 * 1000000000L;

    /**
     * @return false if thread should exit
     */
    boolean decode() {
        mIdleThreads.getAndIncrement();
        try {
            while (true) {
                try {
                    if (acquireHandoff(mDecodeLatch, IDLE_TIMEOUT_NANOS, mDecodeSpins)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    // Treat as timeout.
                    Thread.interrupted();
                }

                int total = mTotalThreads.get();
                if (total > 1 && mTotalThreads.compareAndSet(total, total - 1)) {
                    return false;
                }
            }
        } finally {
            mIdleThreads.getAndDecrement();
        }

        // At this point, decode latch is held exclusively.

        RedoDecoder decoder = mDecoder;
        if (decoder == null) {
            mTotalThreads.getAndDecrement();
            mDecodeLatch.releaseExclusive();
            return false;
        }

        try {
            if (!decoder.run(this)) {
                return true;
            }
            // End of stream reached, and so local instance is now leader.
            reset();
        } catch (Throwable e) {
            if (!mDatabase.isClosed()) {
                EventListener listener = mDatabase.eventListener();
                if (listener != null) {
                    listener.notify(EventType.REPLICATION_PANIC,
                                    "Unexpected replication exception: %1$s", rootCause(e));
                } else {
                    uncaught(e);
                }
            }
            mTotalThreads.getAndDecrement();
            mDecodeLatch.releaseExclusive();
            // Panic.
            closeQuietly(null, mDatabase, e);
            return false;
        }

        mDecoder = null;
        mTotalThreads.getAndDecrement();
        mDecodeLatch.releaseExclusive();

        try {
            mController.leaderNotify();
        } catch (UnmodifiableReplicaException e) {
            // Should already be receiving again due to this exception.
        } catch (Throwable e) {
            // Could try to switch to receiving mode, but panic seems to be the safe option.
            closeQuietly(null, mDatabase, e);
        }

        return false;
    }

    private void notifyStore(Index ix, byte[] key, byte[] value) {
        if (ix != null && !Tree.isInternal(ix.getId())) {
            try {
                mManager.notifyStore(ix, key, value);
            } catch (Throwable e) {
                uncaught(e);
            }
        }
    }

    UnmodifiableReplicaException unmodifiable() throws DatabaseException {
        mDatabase.checkClosed();
        return new UnmodifiableReplicaException();
    }

    final class DecodeTask extends Thread {
        DecodeTask() {
            setDaemon(true);
        }

        public void run() {
            setName("ReplicationReceiver-" + Long.toUnsignedString(getId()));
            try {
                while (ReplRedoEngine.this.decode());
            } finally {
                mTaskThreadSet.remove(this);
            }
        }
    }

    private static long mix(long txnId) {
        return HASH_SPREAD * txnId;
    }

    private static void acquireHandoff(Latch latch, AtomicInteger spinCfg) {
        try {
            acquireHandoff(latch, INFINITE_TIMEOUT, spinCfg);
        } catch (InterruptedException ie) {
            // Shouldn't happen. The call above only throws
            // InterruptedException for a timed wait.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Acquire the exclusive latch using an adaptive spin strategy. The
     * expectation is that the latch is most often uncontended or held
     * exclusive by another thread that is handing off to an exclusive
     * waiter. Spinning here helps avoid costly thread park/unpark.
     */
    private static boolean acquireHandoff(Latch latch, long nanos, AtomicInteger spinCfg) throws InterruptedException {
        int prevSpins = spinCfg.get();
        int cutoff = prevSpins == 0 ? MAX_SPINS : prevSpins;
        int h = 0, tries;

        for (tries = 0;;) {
            if (latch.tryAcquireExclusive()) {
                break;
            } else if (tries < cutoff) {
                h ^= h << 1; h ^= h >>> 3; h ^= h << 10; // xorshift rng
                if (h == 0) {
                    h = ThreadLocalRandom.current().nextInt();
                } else if (h < 0) {
                    ++tries;
                }
            } else if (nanos < 0) {
                latch.acquireExclusive();
                break;
            } else {
                // Timed acquire.
                if (latch.tryAcquireExclusiveNanos(nanos)) break;
                else return false;
            }
        }

        int target;
        if (tries >= MAX_SPINS) {
            // Spinning was pointless if we hit max spins. Next
            // time spin the minimum.
            target = MIN_SPINS;
        } else {
            // Target up to 2 * tries spins next time.
            target = Math.min(MAX_SPINS, Math.max(MIN_SPINS, tries << 1));
        }

        if (prevSpins == 0) {
            spinCfg.set(target);
        } else {
            // Smooth and update the spin config, ignoring any cas failure.
            spinCfg.weakCompareAndSet(prevSpins, prevSpins + (target - prevSpins) / 8);
        }
        return true;
    }

    static final class TxnEntry extends LHashTable.Entry<TxnEntry> {
        LocalTransaction mTxn;
        Latch mLatch;

        void init(LocalTransaction txn, Latch latch) {
            mTxn = txn;
            mLatch = latch;
        }

        Latch latch() {
            Latch latch = mLatch;
            latch.acquireExclusive();
            return latch;
        }

        Latch latch(AtomicInteger spinCfg) {
            Latch latch = mLatch;
            acquireHandoff(latch, spinCfg);
            return latch;
        }
    }

    static final class TxnTable extends LHashTable<TxnEntry> {
        TxnTable(int capacity) {
            super(capacity);
        }

        protected TxnEntry newEntry() {
            return new TxnEntry();
        }
    }
}
