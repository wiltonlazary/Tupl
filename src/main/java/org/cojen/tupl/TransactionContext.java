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

import org.cojen.tupl.util.Latch;

/**
 * Collection of state which is shared by multiple transactions. Contention is reduced by
 * creating many context instances, and distributing them among the transactions.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class TransactionContext extends Latch {
    private final int mTxnStride;

    private long mInitialTxnId;
    private long mTxnId;
    private UndoLog mTopUndoLog;
    private int mUndoLogCount;

    /**
     * @param txnStride transaction id increment
     */
    TransactionContext(int txnStride) {
        if (txnStride <= 0) {
            throw new IllegalArgumentException();
        }
        mTxnStride = txnStride;
    }

    void addStats(Database.Stats stats) {
        acquireShared();
        stats.txnCount += mUndoLogCount;
        stats.txnsCreated += mTxnId / mTxnStride;
        releaseShared();
    }

    /**
     * Set the previously vended transaction id. A call to nextTransactionId returns a higher one.
     */
    void resetTransactionId(long txnId) {
        if (txnId < 0) {
            throw new IllegalArgumentException();
        }
        acquireExclusive();
        mInitialTxnId = txnId;
        mTxnId = txnId;
        releaseExclusive();
    }

    /**
     * To be called only by transaction instances, and caller must hold commit lock. The commit
     * lock ensures that highest transaction id is persisted correctly by checkpoint.
     *
     * @return positive non-zero transaction id
     */
    long nextTransactionId() {
        acquireExclusive();
        long txnId = mTxnId + mTxnStride;
        mTxnId = txnId;
        releaseExclusive();

        if (txnId <= 0) {
            // Improbably, the transaction identifier has wrapped around. Only vend positive
            // identifiers. Non-replicated transactions always have negative identifiers.
            acquireExclusive();
            if (mTxnId <= 0 && (txnId = mTxnId + mTxnStride) <= 0) {
                txnId = mInitialTxnId % mTxnStride;
            }
            mTxnId = txnId;
            releaseExclusive();
        }

        return txnId;
    }

    /**
     * Caller must hold commit lock.
     */
    void register(UndoLog undo) {
        acquireExclusive();

        UndoLog top = mTopUndoLog;
        if (top != null) {
            undo.mPrev = top;
            top.mNext = undo;
        }
        mTopUndoLog = undo;
        mUndoLogCount++;

        releaseExclusive();
    }

    /**
     * Should only be called after all log entries have been truncated or rolled back. Caller
     * does not need to hold db commit lock.
     */
    void unregister(UndoLog log) {
        acquireExclusive();

        UndoLog prev = log.mPrev;
        UndoLog next = log.mNext;
        if (prev != null) {
            prev.mNext = next;
            log.mPrev = null;
        }
        if (next != null) {
            next.mPrev = prev;
            log.mNext = null;
        } else if (log == mTopUndoLog) {
            mTopUndoLog = prev;
        }
        mUndoLogCount--;

        releaseExclusive();
    }

    /**
     * Returns the current transaction id or the given one, depending on which is higher.
     * Caller must hold latch on this state object.
     */
    long maxTransactionId(long txnId) {
        return Math.max(mTxnId, txnId);
    }

    /**
     * Caller must hold latch on this state object.
     */
    boolean hasUndoLogs() {
        return mTopUndoLog != null;
    }

    /**
     * Write any undo log references to the master undo log. Caller must hold commit lock and
     * latch on this state object.
     *
     * @param workspace temporary buffer, allocated on demand
     * @return new or original workspace instance
     */
    byte[] writeToMaster(UndoLog master, byte[] workspace) throws IOException {
        for (UndoLog log = mTopUndoLog; log != null; log = log.mPrev) {
            workspace = log.writeToMaster(master, workspace);
        }
        return workspace;
    }

    /**
     * Deletes any UndoLog instances, as part of database close sequence. Caller must hold
     * exclusive db commit lock.
     */
    void deleteUndoLogs() {
        acquireExclusive();
        try {
            for (UndoLog log = mTopUndoLog; log != null; log = log.mPrev) {
                log.delete();
            }
            mTopUndoLog = null;
        } finally {
            releaseExclusive();
        }
    }
}
