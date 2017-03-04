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

import java.util.ArrayDeque;
import java.util.Deque;

import static java.lang.System.arraycopy;

import static org.cojen.tupl.PageOps.*;
import static org.cojen.tupl.Utils.*;

import org.cojen.tupl.ext.TransactionHandler;

/**
 * Specialized stack used by UndoLog.
 *
 * @author Brian S O'Neill
 */
final class UndoLog implements DatabaseAccess {
    // Linked list of UndoLogs registered with Database.
    UndoLog mPrev;
    UndoLog mNext;

    /*
      UndoLog is persisted in Nodes. All multibyte types are little endian encoded.

      +----------------------------------------+
      | byte:   node type                      |  header
      | byte:   reserved (must be 0)           |
      | ushort: pointer to top entry           |
      | ulong:  lower node id                  |
      +----------------------------------------+
      | free space                             |
      -                                        -
      |                                        |
      +----------------------------------------+
      | log stack entries                      |
      -                                        -
      |                                        |
      +----------------------------------------+

      Stack entries are encoded from the tail end of the node towards the
      header. Entries without payloads are encoded with an opcode less than 16.
      All other types of entries are composed of three sections:

      +----------------------------------------+
      | byte:   opcode                         |
      | varint: payload length                 |
      | n:      payload                        |
      +----------------------------------------+

      Popping entries off the stack involves reading the opcode and moving
      forwards. Payloads which don't fit into the node spill over into the
      lower node(s).
    */

    static final int I_LOWER_NODE_ID = 4;
    private static final int HEADER_SIZE = 12;

    // Must be power of two.
    private static final int INITIAL_BUFFER_SIZE = 128;

    private static final byte OP_SCOPE_ENTER = (byte) 1;
    private static final byte OP_SCOPE_COMMIT = (byte) 2;

    // Indicates that transaction has been committed.
    static final byte OP_COMMIT = (byte) 4;

    // Indicates that transaction has been committed and log is partially truncated.
    static final byte OP_COMMIT_TRUNCATE = (byte) 5;

    // All ops less than 16 have no payload.
    private static final byte PAYLOAD_OP = (byte) 16;

    // Copy to another log from master log. Payload is transaction id, active
    // index id, buffer size (short type), and serialized buffer.
    private static final byte OP_LOG_COPY = (byte) 16;

    // Reference to another log from master log. Payload is transaction id,
    // active index id, length, node id, and top entry offset.
    private static final byte OP_LOG_REF = (byte) 17;

    // Payload is active index id.
    private static final byte OP_INDEX = (byte) 18;

    // Payload is key to delete to undo an insert.
    static final byte OP_UNINSERT = (byte) 19;

    // Payload is Node-encoded key/value entry to store, to undo an update.
    static final byte OP_UNUPDATE = (byte) 20;

    // Payload is Node-encoded key/value entry to store, to undo a delete.
    static final byte OP_UNDELETE = (byte) 21;

    // Payload is Node-encoded key and trash id, to undo a fragmented value delete.
    static final byte OP_UNDELETE_FRAGMENTED = (byte) 22;

    // Payload is custom message.
    static final byte OP_CUSTOM = (byte) 24;

    private static final int LK_ADJUST = 5;

    // Payload is a (large) key and value to store, to undo an update.
    static final byte OP_UNUPDATE_LK = (byte) (OP_UNUPDATE + LK_ADJUST); //25

    // Payload is a (large) key and value to store, to undo a delete.
    static final byte OP_UNDELETE_LK = (byte) (OP_UNDELETE + LK_ADJUST); //26

    // Payload is a (large) key and trash id, to undo a fragmented value delete.
    static final byte OP_UNDELETE_LK_FRAGMENTED = (byte) (OP_UNDELETE_FRAGMENTED + LK_ADJUST); //27

    private final LocalDatabase mDatabase;
    private final long mTxnId;

    // Number of bytes currently pushed into log.
    private long mLength;

    // Except for mLength, all field modifications during normal usage must be
    // performed while holding shared db commit lock. See writeToMaster method.

    private byte[] mBuffer;
    private int mBufferPos;

    // Top node, if required. Nodes are not used for logs which fit into local buffer.
    private Node mNode;
    private int mNodeTopPos;

    private long mActiveIndexId;

    UndoLog(LocalDatabase db, long txnId) {
        mDatabase = db;
        mTxnId = txnId;
    }

    @Override
    public LocalDatabase getDatabase() {
        return mDatabase;
    }

    /**
     * Ensures all entries are stored in persistable nodes, unless the log is empty. Caller
     * must hold db commit lock.
     *
     * @return top node id or 0 if log is empty
     */
    long persistReady() throws IOException {
        Node node = mNode;

        if (node != null) {
            node.acquireExclusive();
            try {
                mDatabase.markUnmappedDirty(node);
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
        } else if (mLength == 0) {
            return 0;
        } else {
            mNode = node = allocUnevictableNode(0);

            byte[] buffer = mBuffer;
            if (buffer == null) {
                // Set pointer to top entry (none at the moment).
                mNodeTopPos = pageSize(node.mPage);
            } else {
                int pos = mBufferPos;
                int size = buffer.length - pos;
                /*P*/ byte[] page = node.mPage;
                int newPos = pageSize(page) - size;
                p_copyFromArray(buffer, pos, page, newPos, size);
                // Set pointer to top entry.
                mNodeTopPos = newPos;
                mBuffer = null;
                mBufferPos = 0;
            }
        }

        node.undoTop(mNodeTopPos);
        node.releaseExclusive();

        return mNode.mId;
    }

    /**
     * Returns the top node id as returned by the last call to persistReady. Caller must hold
     * db commit lock.
     *
     * @return top node id or 0 if log is empty
     */
    long topNodeId() throws IOException {
        return mNode == null ? 0 : mNode.mId;
    }

    private int pageSize(/*P*/ byte[] page) {
        /*P*/ // [
        return page.length;
        /*P*/ // |
        /*P*/ // return mDatabase.pageSize();
        /*P*/ // ]
    }

    long txnId() {
        return mTxnId;
    }

    /**
     * Deletes just the top node, as part of database close sequence. Caller must hold
     * exclusive db commit lock.
     */
    void delete() {
        Node node = mNode;
        if (node != null) {
            mNode = null;
            node.delete(mDatabase);
        }
    }

    /**
     * Caller must hold db commit lock.
     */
    final void pushUninsert(final long indexId, byte[] key) throws IOException {
        setActiveIndexId(indexId);
        doPush(OP_UNINSERT, key, 0, key.length, calcUnsignedVarIntLength(key.length));
    }

    /**
     * Push an operation with a Node-encoded key and value, which might be fragmented. Caller
     * must hold db commit lock.
     *
     * @param op OP_UNUPDATE, OP_UNDELETE or OP_UNDELETE_FRAGMENTED
     */
    final void pushNodeEncoded(final long indexId, byte op, byte[] payload, int off, int len)
        throws IOException
    {
        setActiveIndexId(indexId);

        if ((payload[off] & 0xc0) == 0xc0) {
            // Key is fragmented and cannot be stored as-is, so expand it fully and switch to
            // using the "LK" op variant.
            /*P*/ byte[] copy = p_transfer(payload);
            try {
                payload = Node.expandKeyAtLoc(this, copy, off, len, op != OP_UNDELETE_FRAGMENTED);
            } finally {
                p_delete(copy);
            }
            off = 0;
            len = payload.length;
            op += LK_ADJUST;
        }

        doPush(op, payload, off, len, calcUnsignedVarIntLength(len));
    }

    /**
     * Push an operation with a Node-encoded key and value, which might be fragmented. Caller
     * must hold db commit lock.
     *
     * @param op OP_UNUPDATE, OP_UNDELETE or OP_UNDELETE_FRAGMENTED
     */
    final void pushNodeEncoded(final long indexId, byte op, long payloadPtr, int off, int len)
        throws IOException
    {
        setActiveIndexId(indexId);

        byte[] payload;
        if ((DirectPageOps.p_byteGet(payloadPtr, off) & 0xc0) == 0xc0) {
            // Key is fragmented and cannot be stored as-is, so expand it fully and switch to
            // using the "LK" op variant.
            /*P*/ // [
            throw new AssertionError(); // shouldn't be using direct page access
            /*P*/ // |
            /*P*/ // payload = Node.expandKeyAtLoc
            /*P*/ //     (this, payloadPtr, off, len, op != OP_UNDELETE_FRAGMENTED);
            /*P*/ // op += LK_ADJUST;
            /*P*/ // ]
        } else {
            payload = new byte[len];
            DirectPageOps.p_copyToArray(payloadPtr, off, payload, 0, len);
        }

        doPush(op, payload, 0, payload.length, calcUnsignedVarIntLength(payload.length));
    }

    private void setActiveIndexId(long indexId) throws IOException {
        long activeIndexId = mActiveIndexId;
        if (indexId != activeIndexId) {
            if (activeIndexId != 0) {
                byte[] payload = new byte[8];
                encodeLongLE(payload, 0, activeIndexId);
                doPush(OP_INDEX, payload, 0, 8, 1);
            }
            mActiveIndexId = indexId;
        }
    }

    /**
     * Caller must hold db commit lock.
     */
    void pushCommit() throws IOException {
        doPush(OP_COMMIT);
    }

    void pushCustom(byte[] message) throws IOException {
        int len = message.length;
        doPush(OP_CUSTOM, message, 0, len, calcUnsignedVarIntLength(len));
    }

    /**
     * Caller must hold db commit lock.
     */
    private void doPush(final byte op) throws IOException {
        doPush(op, EMPTY_BYTES, 0, 0, 0);
    }

    /**
     * Caller must hold db commit lock.
     */
    private void doPush(final byte op, final byte[] payload, final int off, final int len,
                        final int varIntLen)
        throws IOException
    {
        final int encodedLen = 1 + varIntLen + len;

        Node node = mNode;
        if (node != null) {
            // Push into allocated node, which must be marked dirty.
            node.acquireExclusive();
            try {
                mDatabase.markUnmappedDirty(node);
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
        } else quick: {
            // Try to push into a local buffer before allocating a node.
            byte[] buffer = mBuffer;
            int pos;
            if (buffer == null) {
                int newCap = Math.max(INITIAL_BUFFER_SIZE, roundUpPower2(encodedLen));
                int pageSize = mDatabase.pageSize();
                if (newCap <= (pageSize >> 1)) {
                    mBuffer = buffer = new byte[newCap];
                    mBufferPos = pos = newCap;
                } else {
                    // Required capacity is large, so just use a node.
                    mNode = node = allocUnevictableNode(0);
                    // Set pointer to top entry (none at the moment).
                    mNodeTopPos = pageSize;
                    break quick;
                }
            } else {
                pos = mBufferPos;
                if (pos < encodedLen) {
                    final int size = buffer.length - pos;
                    int newCap = Math.max(buffer.length << 1, roundUpPower2(encodedLen + size));
                    if (newCap <= (mDatabase.pageSize() >> 1)) {
                        byte[] newBuf = new byte[newCap];
                        int newPos = newCap - size;
                        arraycopy(buffer, pos, newBuf, newPos, size);
                        mBuffer = buffer = newBuf;
                        mBufferPos = pos = newPos;
                    } else {
                        // Required capacity is large, so just use a node.
                        mNode = node = allocUnevictableNode(0);
                        /*P*/ byte[] page = node.mPage;
                        int newPos = pageSize(page) - size;
                        p_copyFromArray(buffer, pos, page, newPos, size);
                        // Set pointer to top entry.
                        mNodeTopPos = newPos;
                        mBuffer = null;
                        mBufferPos = 0;
                        break quick;
                    }
                }
            }

            writeBufferEntry(buffer, pos -= encodedLen, op, payload, off, len);
            mBufferPos = pos;
            mLength += encodedLen;
            return;
        }

        int pos = mNodeTopPos;
        int available = pos - HEADER_SIZE;
        if (available >= encodedLen) {
            writePageEntry(node.mPage, pos -= encodedLen, op, payload, off, len);
            node.releaseExclusive();
            mNodeTopPos = pos;
            mLength += encodedLen;
            return;
        }

        // Payload doesn't fit into node, so break it up.
        int remaining = len;

        while (true) {
            int amt = Math.min(available, remaining);
            pos -= amt;
            available -= amt;
            remaining -= amt;
            /*P*/ byte[] page = node.mPage;
            p_copyFromArray(payload, off + remaining, page, pos, amt);

            if (remaining <= 0 && available >= (1 + varIntLen)) {
                if (varIntLen > 0) {
                    p_uintPutVar(page, pos -= varIntLen, len);
                }
                p_bytePut(page, --pos, op);
                node.releaseExclusive();
                break;
            }

            Node newNode;
            try {
                newNode = allocUnevictableNode(node.mId);
            } catch (Throwable e) {
                // Undo the damage.
                while (node != mNode) {
                    node = popNode(node, true);
                }
                node.releaseExclusive();
                throw e;
            }

            node.undoTop(pos);
            mDatabase.nodeMapPut(node);
            node.releaseExclusive();
            node.makeEvictable();

            node = newNode;
            pos = pageSize(page);
            available = pos - HEADER_SIZE;
        }

        mNode = node;
        mNodeTopPos = pos;
        mLength += encodedLen;
    }

    /**
     * Caller does not need to hold db commit lock.
     *
     * @return savepoint
     */
    final long scopeEnter() throws IOException {
        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            long savepoint = mLength;
            doScopeEnter();
            return savepoint;
        } finally {
            shared.release();
        }
    }

    /**
     * Caller must hold db commit lock.
     */
    final void doScopeEnter() throws IOException {
        doPush(OP_SCOPE_ENTER);
    }

    /**
     * Caller does not need to hold db commit lock.
     *
     * @return savepoint
     */
    final long scopeCommit() throws IOException {
        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            doPush(OP_SCOPE_COMMIT);
            return mLength;
        } finally {
            shared.release();
        }
    }

    /**
     * Rollback all log entries to the given savepoint. Pass zero to rollback
     * everything. Caller does not need to hold db commit lock.
     */
    final void scopeRollback(long savepoint) throws IOException {
        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            if (savepoint < mLength) {
                // Rollback the entire scope, including the enter op.
                doRollback(savepoint);
            }
        } finally {
            shared.release();
        }
    }

    /**
     * Truncate all log entries. Caller does not need to hold db commit lock.
     *
     * @param commit pass true to indicate that top of stack is a commit op
     */
    final void truncate(boolean commit) throws IOException {
        final CommitLock commitLock = mDatabase.commitLock();
        CommitLock.Shared shared = commitLock.acquireShared();
        try {
            shared = doTruncate(commitLock, shared, commit);
        } finally {
            shared.release();
        }
    }

    /**
     * Truncate all log entries. Caller must hold db commit lock.
     *
     * @param commit pass true to indicate that top of stack is a commit op
     */
    final CommitLock.Shared doTruncate(CommitLock commitLock, CommitLock.Shared shared,
                                       boolean commit)
        throws IOException
    {
        if (mLength > 0) {
            Node node = mNode;
            if (node == null) {
                mBufferPos = mBuffer.length;
            } else {
                node.acquireExclusive();
                while ((node = popNode(node, true)) != null) {
                    if (commit) {
                        // When shared lock is released, log can be checkpointed in an
                        // incomplete state. Although caller must have already pushed the
                        // commit op, any of the remaining nodes might be referenced by an
                        // older master undo log entry. Must call prepareToDelete before
                        // calling redirty, in case node contains data which has been
                        // marked to be written out with the active checkpoint. The state
                        // assigned by redirty is such that the node might be written
                        // by the next checkpoint.
                        mDatabase.prepareToDelete(node);
                        mDatabase.redirty(node);
                        /*P*/ byte[] page = node.mPage;
                        int end = pageSize(page) - 1;
                        node.undoTop(end);
                        p_bytePut(page, end, OP_COMMIT_TRUNCATE);
                    }
                    if (commitLock.hasQueuedThreads()) {
                        // Release and re-acquire, to unblock any threads waiting for
                        // checkpoint to begin.
                        shared.release();
                        shared = commitLock.acquireShared();
                    }
                }
            }
            mLength = 0;
            mActiveIndexId = 0;
        }

        return shared;
    }

    /**
     * Rollback all log entries. Caller does not need to hold db commit lock.
     */
    final void rollback() throws IOException {
        if (mLength == 0) {
            // Nothing to rollback, so return quickly.
            return;
        }

        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            doRollback(0);
        } finally {
            shared.release();
        }
    }

    /**
     * @param savepoint must be less than mLength
     */
    private void doRollback(long savepoint) throws IOException {
        // Implementation could be optimized somewhat, resulting in less
        // temporary arrays and copies. Rollback optimization is generally not
        // necessary, since most transactions are expected to commit.

        byte[] opRef = new byte[1];
        Index activeIndex = null;
        do {
            byte[] entry = pop(opRef, true);
            if (entry == null) {
                break;
            }
            byte op = opRef[0];
            activeIndex = undo(activeIndex, op, entry);
        } while (savepoint < mLength);
    }

    /**
     * Truncate all log entries, and delete any ghosts that were created. Only
     * to be called during recovery.
     */
    final void deleteGhosts() throws IOException {
        if (mLength <= 0) {
            return;
        }

        byte[] opRef = new byte[1];
        Index activeIndex = null;
        do {
            byte[] entry = pop(opRef, true);
            if (entry == null) {
                break;
            }

            byte op = opRef[0];
            switch (op) {
            default:
                throw new DatabaseException("Unknown undo log entry type: " + op);

            case OP_SCOPE_ENTER:
            case OP_SCOPE_COMMIT:
            case OP_COMMIT:
            case OP_COMMIT_TRUNCATE:
            case OP_UNINSERT:
            case OP_UNUPDATE:
            case OP_CUSTOM:
            case OP_UNUPDATE_LK:
                // Ignore.
                break;

            case OP_INDEX:
                mActiveIndexId = decodeLongLE(entry, 0);
                activeIndex = null;
                break;

            case OP_UNDELETE:
            case OP_UNDELETE_FRAGMENTED:
                // Since transaction was committed, don't insert an entry
                // to undo a delete, but instead delete the ghost.
                if ((activeIndex = findIndex(activeIndex)) != null) {
                    byte[] key;
                    /*P*/ byte[] pentry = p_transfer(entry);
                    try {
                        key = Node.retrieveKeyAtLoc(this, pentry, 0);
                    } finally {
                        p_delete(pentry);
                    }

                    do {
                        TreeCursor cursor = new TreeCursor((Tree) activeIndex, null);
                        try {
                            cursor.deleteGhost(key);
                            break;
                        } catch (ClosedIndexException e) {
                            // User closed the shared index reference, so re-open it.
                            activeIndex = findIndex(null);
                        } catch (Throwable e) {
                            throw closeOnFailure(cursor, e);
                        }
                    } while (activeIndex != null);
                }
                break;

            case OP_UNDELETE_LK:
            case OP_UNDELETE_LK_FRAGMENTED:
                // Since transaction was committed, don't insert an entry
                // to undo a delete, but instead delete the ghost.
                if ((activeIndex = findIndex(activeIndex)) != null) {
                    byte[] key = new byte[decodeUnsignedVarInt(entry, 0)];
                    arraycopy(entry, calcUnsignedVarIntLength(key.length), key, 0, key.length);

                    do {
                        TreeCursor cursor = new TreeCursor((Tree) activeIndex, null);
                        try {
                            cursor.deleteGhost(key);
                            break;
                        } catch (ClosedIndexException e) {
                            // User closed the shared index reference, so re-open it.
                            activeIndex = findIndex(null);
                        } catch (Throwable e) {
                            throw closeOnFailure(cursor, e);
                        }
                    } while (activeIndex != null);
                }
                break;
            }
        } while (mLength > 0);
    }

    /**
     * @param activeIndex active index, possibly null
     * @param op undo op
     * @return new active index, possibly null
     */
    private Index undo(Index activeIndex, byte op, byte[] entry) throws IOException {
        switch (op) {
        default:
            throw new DatabaseException("Unknown undo log entry type: " + op);

        case OP_SCOPE_ENTER:
        case OP_SCOPE_COMMIT:
        case OP_COMMIT:
        case OP_COMMIT_TRUNCATE:
            // Only needed by recovery.
            break;

        case OP_INDEX:
            mActiveIndexId = decodeLongLE(entry, 0);
            activeIndex = null;
            break;

        case OP_UNINSERT:
            while ((activeIndex = findIndex(activeIndex)) != null) {
                try {
                    activeIndex.delete(Transaction.BOGUS, entry);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    activeIndex = null;
                }
            }
            break;

        case OP_UNUPDATE:
        case OP_UNDELETE: {
            if ((activeIndex = findIndex(activeIndex)) != null) {
                byte[][] pair;
                /*P*/ byte[] pentry = p_transfer(entry);
                try {
                    pair = Node.retrieveKeyValueAtLoc(this, pentry, 0);
                } finally {
                    p_delete(pentry);
                }

                do {
                    try {
                        activeIndex.store(Transaction.BOGUS, pair[0], pair[1]);
                        break;
                    } catch (ClosedIndexException e) {
                        // User closed the shared index reference, so re-open it.
                        activeIndex = findIndex(null);
                    }
                } while (activeIndex != null);
            }
            break;
        }

        case OP_UNUPDATE_LK:
        case OP_UNDELETE_LK:
            if ((activeIndex = findIndex(activeIndex)) != null) {
                byte[] key = new byte[decodeUnsignedVarInt(entry, 0)];
                int keyLoc = calcUnsignedVarIntLength(key.length);
                arraycopy(entry, keyLoc, key, 0, key.length);

                int valueLoc = keyLoc + key.length;
                byte[] value = new byte[entry.length - valueLoc];
                arraycopy(entry, valueLoc, value, 0, value.length);

                do {
                    try {
                        activeIndex.store(Transaction.BOGUS, key, value);
                        break;
                    } catch (ClosedIndexException e) {
                        // User closed the shared index reference, so re-open it.
                        activeIndex = findIndex(null);
                    }
                } while (activeIndex != null);
            }
            break;

        case OP_UNDELETE_FRAGMENTED:
            while (true) {
                try {
                    activeIndex = findIndex(activeIndex);
                    mDatabase.fragmentedTrash().remove(mTxnId, (Tree) activeIndex, entry);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    activeIndex = null;
                }
            }
            break;

        case OP_UNDELETE_LK_FRAGMENTED:
            if ((activeIndex = findIndex(activeIndex)) != null) {
                byte[] key = new byte[decodeUnsignedVarInt(entry, 0)];
                int keyLoc = calcUnsignedVarIntLength(key.length);
                arraycopy(entry, keyLoc, key, 0, key.length);

                int tidLoc = keyLoc + key.length;
                int tidLen = entry.length - tidLoc;
                byte[] trashKey = new byte[8 + tidLen];
                encodeLongBE(trashKey, 0, mTxnId);
                arraycopy(entry, tidLoc, trashKey, 8, tidLen);

                do {
                    try {
                        activeIndex = findIndex(activeIndex);
                        mDatabase.fragmentedTrash().remove((Tree) activeIndex, key, trashKey);
                        break;
                    } catch (ClosedIndexException e) {
                        // User closed the shared index reference, so re-open it.
                        activeIndex = findIndex(null);
                    }
                } while (activeIndex != null);
            }
            break;

        case OP_CUSTOM:
            LocalDatabase db = mDatabase;
            TransactionHandler handler = db.mCustomTxnHandler;
            if (handler == null) {
                throw new DatabaseException("Custom transaction handler is not installed");
            }
            handler.undo(db, entry);
            break;
        }

        return activeIndex;
    }

    /**
     * @return null if index was deleted
     */
    private Index findIndex(Index activeIndex) throws IOException {
        if (activeIndex == null || activeIndex.isClosed()) {
            activeIndex = mDatabase.anyIndexById(mActiveIndexId);
        }
        return activeIndex;
    }

    /**
     * @param delete true to delete empty nodes
     * @return last pushed op, or 0 if empty
     */
    final byte peek(boolean delete) throws IOException {
        Node node = mNode;
        if (node == null) {
            return (mBuffer == null || mBufferPos >= mBuffer.length) ? 0 : mBuffer[mBufferPos];
        }

        node.acquireExclusive();
        while (true) {
            /*P*/ byte[] page = node.mPage;
            if (mNodeTopPos < pageSize(page)) {
                byte op = p_byteGet(page, mNodeTopPos);
                node.releaseExclusive();
                return op;
            }
            if ((node = popNode(node, delete)) == null) {
                return 0;
            }
        }
    }

    /**
     * Caller must hold db commit lock.
     *
     * @param opRef element zero is filled in with the opcode
     * @param delete true to delete nodes
     * @return null if nothing left
     */
    private final byte[] pop(byte[] opRef, boolean delete) throws IOException {
        Node node = mNode;
        if (node == null) {
            byte[] buffer = mBuffer;
            if (buffer == null) {
                opRef[0] = 0;
                mLength = 0;
                return null;
            }
            int pos = mBufferPos;
            if (pos >= buffer.length) {
                opRef[0] = 0;
                mLength = 0;
                return null;
            }
            if ((opRef[0] = buffer[pos++]) < PAYLOAD_OP) {
                mBufferPos = pos;
                mLength -= 1;
                return EMPTY_BYTES;
            }
            int payloadLen = decodeUnsignedVarInt(buffer, pos);
            int varIntLen = calcUnsignedVarIntLength(payloadLen);
            pos += varIntLen;
            byte[] entry = new byte[payloadLen];
            arraycopy(buffer, pos, entry, 0, payloadLen);
            mBufferPos = pos += payloadLen;
            mLength -= 1 + varIntLen + payloadLen;
            return entry;
        }

        node.acquireExclusive();
        /*P*/ byte[] page;
        while (true) {
            page = node.mPage;
            if (mNodeTopPos < pageSize(page)) {
                break;
            }
            if ((node = popNode(node, delete)) == null) {
                mLength = 0;
                return null;
            }
        }

        if ((opRef[0] = p_byteGet(page, mNodeTopPos++)) < PAYLOAD_OP) {
            mLength -= 1;
            if (mNodeTopPos >= pageSize(page)) {
                node = popNode(node, delete);
            }
            if (node != null) {
                node.releaseExclusive();
            }
            return EMPTY_BYTES;
        }

        int payloadLen;
        {
            payloadLen = p_uintGetVar(page, mNodeTopPos);
            int varIntLen = p_uintVarSize(payloadLen);
            mNodeTopPos += varIntLen;
            mLength -= 1 + varIntLen + payloadLen;
        }

        byte[] entry = new byte[payloadLen];
        int entryPos = 0;

        while (true) {
            int avail = Math.min(payloadLen, pageSize(page) - mNodeTopPos);
            p_copyToArray(page, mNodeTopPos, entry, entryPos, avail);
            payloadLen -= avail;
            mNodeTopPos += avail;

            if (mNodeTopPos >= pageSize(page)) {
                node = popNode(node, delete);
            }

            if (payloadLen <= 0) {
                if (node != null) {
                    node.releaseExclusive();
                }
                return entry;
            }

            if (node == null) {
                throw new CorruptDatabaseException("Remainder of undo log is missing");
            }

            page = node.mPage;

            // Payloads which spill over should always continue into a node which is full. If
            // the top position is actually at the end, then it likely references a
            // OP_COMMIT_TRUNCATE operation, in which case the transaction has actully
            // committed, and full decoding of the undo log is unnecessary or impossible.
            if (mNodeTopPos == pageSize(page) - 1 &&
                p_byteGet(page, mNodeTopPos) == OP_COMMIT_TRUNCATE)
            {
                node.releaseExclusive();
                return entry;
            }

            entryPos += avail;
        }
    }

    /**
     * @param parent latched parent node
     * @param delete true to delete the parent node too
     * @return current (latched) mNode; null if none left
     */
    private Node popNode(Node parent, boolean delete) throws IOException {
        Node lowerNode = null;
        long lowerNodeId = p_longGetLE(parent.mPage, I_LOWER_NODE_ID);
        if (lowerNodeId != 0) {
            lowerNode = mDatabase.nodeMapGetAndRemove(lowerNodeId);
            if (lowerNode != null) {
                lowerNode.makeUnevictable();
            } else {
                // Node was evicted, so reload it.
                try {
                    lowerNode = readUndoLogNode(mDatabase, lowerNodeId);
                } catch (Throwable e) {
                    parent.releaseExclusive();
                    throw e;
                }
            }
        }

        parent.makeEvictable();

        if (delete) {
            LocalDatabase db = mDatabase;
            db.prepareToDelete(parent);
            // Safer to never recycle undo log nodes. Keep them until the next checkpoint, when
            // there's a guarantee that the master undo log will not reference them anymore.
            db.deleteNode(parent, false);
        } else {
            parent.releaseExclusive();
        }

        mNode = lowerNode;
        mNodeTopPos = lowerNode == null ? 0 : lowerNode.undoTop();

        return lowerNode;
    }

    private static void writeBufferEntry(byte[] dest, int destPos,
                                         byte op, byte[] payload, int off, int len)
    {
        dest[destPos] = op;
        if (op >= PAYLOAD_OP) {
            int payloadPos = encodeUnsignedVarInt(dest, destPos + 1, len);
            arraycopy(payload, off, dest, payloadPos, len);
        }
    }

    private static void writePageEntry(/*P*/ byte[] page, int pagePos,
                                       byte op, byte[] payload, int off, int len)
    {
        p_bytePut(page, pagePos, op);
        if (op >= PAYLOAD_OP) {
            int payloadPos = p_uintPutVar(page, pagePos + 1, len);
            p_copyFromArray(payload, off, page, payloadPos, len);
        }
    }

    /**
     * Caller must hold db commit lock.
     */
    private Node allocUnevictableNode(long lowerNodeId) throws IOException {
        Node node = mDatabase.allocDirtyNode(NodeUsageList.MODE_UNEVICTABLE);
        node.type(Node.TYPE_UNDO_LOG);
        p_longPutLE(node.mPage, I_LOWER_NODE_ID, lowerNodeId);
        return node;
    }

    /**
     * Caller must hold exclusive db commit lock.
     *
     * @param workspace temporary buffer, allocated on demand
     * @return new or original workspace instance
     */
    final byte[] writeToMaster(UndoLog master, byte[] workspace) throws IOException {
        Node node = mNode;
        if (node == null) {
            byte[] buffer = mBuffer;
            if (buffer == null) {
                return workspace;
            }
            int pos = mBufferPos;
            int bsize = buffer.length - pos;
            if (bsize == 0) {
                return workspace;
            }
            // TODO: Consider calling persistReady if UndoLog is still in a buffer next time.
            final int psize = (8 + 8 + 2) + bsize;
            if (workspace == null || workspace.length < psize) {
                workspace = new byte[Math.max(INITIAL_BUFFER_SIZE, roundUpPower2(psize))];
            }
            writeHeaderToMaster(workspace);
            encodeShortLE(workspace, (8 + 8), bsize);
            arraycopy(buffer, pos, workspace, (8 + 8 + 2), bsize);
            master.doPush(OP_LOG_COPY, workspace, 0, psize,
                          calcUnsignedVarIntLength(psize));
        } else {
            if (workspace == null) {
                workspace = new byte[INITIAL_BUFFER_SIZE];
            }
            writeHeaderToMaster(workspace);
            encodeLongLE(workspace, (8 + 8), mLength);
            encodeLongLE(workspace, (8 + 8 + 8), node.mId);
            encodeShortLE(workspace, (8 + 8 + 8 + 8), mNodeTopPos);
            master.doPush(OP_LOG_REF, workspace, 0, (8 + 8 + 8 + 8 + 2), 1);
        }
        return workspace;
    }

    private void writeHeaderToMaster(byte[] workspace) {
        encodeLongLE(workspace, 0, mTxnId);
        encodeLongLE(workspace, 8, mActiveIndexId);
    }

    static UndoLog recoverMasterUndoLog(LocalDatabase db, long nodeId) throws IOException {
        UndoLog log = new UndoLog(db, 0);
        // Length is not recoverable.
        log.mLength = Long.MAX_VALUE;
        log.mNode = readUndoLogNode(db, nodeId);
        log.mNodeTopPos = log.mNode.undoTop();
        log.mNode.releaseExclusive();
        return log;
    }

    /**
     * Recover transactions which were recorded by this master log, keyed by
     * transaction id. Recovered transactions have a NO_REDO durability mode.
     * All transactions are registered, and so they must be reset after
     * recovery is complete. Master log is truncated as a side effect of
     * calling this method.
     */
    void recoverTransactions(LHashTable.Obj<LocalTransaction> txns,
                             LockMode lockMode, long timeoutNanos)
        throws IOException
    {
        byte[] opRef = new byte[1];
        byte[] entry;
        while ((entry = pop(opRef, true)) != null) {
            UndoLog log = recoverUndoLog(opRef[0], entry);
            LocalTransaction txn = log.recoverTransaction(lockMode, timeoutNanos);

            // Reload the UndoLog, since recoverTransaction consumes it all.
            txn.recoveredUndoLog(recoverUndoLog(opRef[0], entry));

            txns.insert(log.mTxnId).value = txn;
        }
    }

    /**
     * Method consumes entire log as a side-effect.
     */
    private final LocalTransaction recoverTransaction(LockMode lockMode, long timeoutNanos)
        throws IOException
    {
        byte[] opRef = new byte[1];
        Scope scope = new Scope();

        // Scopes are recovered in the opposite order in which they were
        // created. Gather them in a stack to reverse the order.
        Deque<Scope> scopes = new ArrayDeque<>();
        scopes.addFirst(scope);

        boolean acquireLocks = true;
        int depth = 1;

        loop: while (mLength > 0) {
            byte[] entry = pop(opRef, false);
            if (entry == null) {
                break;
            }

            byte op = opRef[0];
            switch (op) {
            default:
                throw new DatabaseException("Unknown undo log entry type: " + op);

            case OP_COMMIT:
                // Handled by Transaction.recoveryCleanup, but don't acquire
                // locks. This avoids deadlocks with later transactions.
                acquireLocks = false;
                break;

            case OP_COMMIT_TRUNCATE:
                // Skip examining the rest of the log. It will likely appear to be corrupt
                // anyhow due to the OP_COMMIT_TRUNCATE having overwritten existing data.
                if (mNode != null) {
                    mNode.makeEvictable();
                    mNode.releaseExclusive();
                    mNode = null;
                    mNodeTopPos = 0;
                }
                break loop;

            case OP_SCOPE_ENTER:
                depth++;
                if (depth > scopes.size()) {
                    scope.mSavepoint = mLength;
                    scope = new Scope();
                    scopes.addFirst(scope);
                }
                break;

            case OP_SCOPE_COMMIT:
                depth--;
                break;

            case OP_INDEX:
                mActiveIndexId = decodeLongLE(entry, 0);
                break;

            case OP_UNINSERT:
                if (lockMode != LockMode.UNSAFE) {
                    scope.addLock(mActiveIndexId, entry);
                }
                break;

            case OP_UNUPDATE:
            case OP_UNDELETE:
            case OP_UNDELETE_FRAGMENTED:
                if (lockMode != LockMode.UNSAFE) {
                    byte[] key;
                    /*P*/ byte[] pentry = p_transfer(entry);
                    try {
                        key = Node.retrieveKeyAtLoc(this, pentry, 0);
                    } finally {
                        p_delete(pentry);
                    }

                    scope.addLock(mActiveIndexId, key)
                        // Indicate that a ghost must be deleted when the transaction is
                        // committed. When the frame is uninitialized, the Node.deleteGhost
                        // method uses the slow path and searches for the entry.
                        .setGhostFrame(new CursorFrame.Ghost());
                }
                break;

            case OP_UNUPDATE_LK:
            case OP_UNDELETE_LK:
            case OP_UNDELETE_LK_FRAGMENTED:
                if (lockMode != LockMode.UNSAFE) {
                    byte[] key = new byte[decodeUnsignedVarInt(entry, 0)];
                    arraycopy(entry, calcUnsignedVarIntLength(key.length), key, 0, key.length);

                    scope.addLock(mActiveIndexId, key)
                        // Indicate that a ghost must be deleted when the transaction is
                        // committed. When the frame is uninitialized, the Node.deleteGhost
                        // method uses the slow path and searches for the entry.
                        .setGhostFrame(new CursorFrame.Ghost());
                }
                break;

            case OP_CUSTOM:
                break;
            }
        }

        LocalTransaction txn = new LocalTransaction
            (mDatabase, mTxnId, lockMode, timeoutNanos,
             // Blindly assume trash must be deleted. No harm if none exists.
             LocalTransaction.HAS_TRASH);

        scope = scopes.pollFirst();
        if (acquireLocks) {
            scope.acquireLocks(txn);
        }

        while ((scope = scopes.pollFirst()) != null) {
            txn.recoveredScope(scope.mSavepoint, LocalTransaction.HAS_TRASH);
            if (acquireLocks) {
                scope.acquireLocks(txn);
            }
        }

        return txn;
    }

    /**
     * Recovered undo scope.
     */
    static class Scope {
        long mSavepoint;

        // Locks are recovered in the opposite order in which they were acquired. Gather them
        // in a stack to reverse the order. Re-use the LockManager collision chain field and
        // form a linked list.
        Lock mTopLock;

        Scope() {
        }

        Lock addLock(long indexId, byte[] key) {
            Lock lock = new Lock();
            lock.mIndexId = indexId;
            lock.mKey = key;
            lock.mHashCode = LockManager.hash(indexId, key);
            lock.mLockManagerNext = mTopLock;
            mTopLock = lock;
            return lock;
        }

        void acquireLocks(LocalTransaction txn) throws LockFailureException {
            Lock lock = mTopLock;
            if (lock != null) while (true) {
                // Copy next before the field is overwritten.
                Lock next = lock.mLockManagerNext;
                txn.lockExclusive(lock);
                if (next == null) {
                    break;
                }
                mTopLock = lock = next;
            }
        }
    }

    /**
     * @param masterLogOp OP_LOG_COPY or OP_LOG_REF
     */
    private UndoLog recoverUndoLog(byte masterLogOp, byte[] masterLogEntry)
        throws IOException
    {
        if (masterLogOp != OP_LOG_COPY && masterLogOp != OP_LOG_REF) {
            throw new DatabaseException("Unknown undo log entry type: " + masterLogOp);
        }

        long txnId = decodeLongLE(masterLogEntry, 0);
        UndoLog log = new UndoLog(mDatabase, txnId);
        log.mActiveIndexId = decodeLongLE(masterLogEntry, 8);

        if (masterLogOp == OP_LOG_COPY) {
            int bsize = decodeUnsignedShortLE(masterLogEntry, (8 + 8));
            log.mLength = bsize;
            byte[] buffer = new byte[bsize];
            arraycopy(masterLogEntry, (8 + 8 + 2), buffer, 0, bsize);
            log.mBuffer = buffer;
            log.mBufferPos = 0;
        } else {
            log.mLength = decodeLongLE(masterLogEntry, (8 + 8));
            long nodeId = decodeLongLE(masterLogEntry, (8 + 8 + 8));
            int topEntry = decodeUnsignedShortLE(masterLogEntry, (8 + 8 + 8 + 8));
            log.mNode = readUndoLogNode(mDatabase, nodeId);
            log.mNodeTopPos = topEntry;

            // If node contains OP_COMMIT_TRUNCATE at the end, then the corresponding transaction
            // was committed and the undo log nodes don't need to be fully examined.
            if (log.mNode.undoTop() == pageSize(log.mNode.mPage) - 1 &&
                p_byteGet(log.mNode.mPage, log.mNode.undoTop()) == OP_COMMIT_TRUNCATE)
            {
                log.mNodeTopPos = log.mNode.undoTop();
            }

            log.mNode.releaseExclusive();
        }

        return log;
    }

    /**
     * @return latched, unevictable node
     */
    private static Node readUndoLogNode(LocalDatabase db, long nodeId) throws IOException {
        Node node = db.allocLatchedNode(nodeId, NodeUsageList.MODE_UNEVICTABLE);
        try {
            node.read(db, nodeId);
            if (node.type() != Node.TYPE_UNDO_LOG) {
                throw new CorruptDatabaseException
                    ("Not an undo log node type: " + node.type() + ", id: " + nodeId);
            }
            return node;
        } catch (Throwable e) {
            node.makeEvictableNow();
            node.releaseExclusive();
            throw e;
        }
    }
}
