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

import java.util.function.LongConsumer;

import org.cojen.tupl.io.CauseCloseable;

/**
 * @author Brian S O'Neill
 * @see DurablePageDb
 * @see NonPageDb
 */
abstract class PageDb implements CauseCloseable {
    final CommitLock mCommitLock;

    PageDb() {
        mCommitLock = new CommitLock();
    }

    /**
     * Must be called when object is no longer referenced.
     */
    abstract void delete();

    public abstract boolean isDurable();

    /**
     * @return 0 or NodeUsageList.MODE_NO_EVICT
     */
    public abstract int allocMode();

    /**
     * @param mode NodeUsageList.MODE_UNEVICTABLE | MODE_NO_EVICT
     * @return node with id assigned
     */
    public abstract Node allocLatchedNode(LocalDatabase db, int mode) throws IOException;

    /**
     * Returns the fixed size of all pages in the store, in bytes.
     */
    public abstract int pageSize();

    public abstract long pageCount() throws IOException;

    public abstract void pageLimit(long limit);

    public abstract long pageLimit();

    public abstract void pageLimitOverride(long limit);

    /**
     * Returns a snapshot of additional store stats.
     */
    public abstract Stats stats();

    public static final class Stats {
        public long totalPages;
        public long freePages;

        public String toString() {
            return "PageDb.Stats {totalPages=" + totalPages + ", freePages=" + freePages + '}';
        }
    }

    /**
     * Reads a page without locking. Caller must ensure that a deleted page
     * is not read during or after a commit.
     *
     * @param id page id to read
     * @param page receives read data
     */
    public abstract void readPage(long id, /*P*/ byte[] page) throws IOException;

    /**
     * Allocates a page to be written to.
     *
     * @return page id; never zero or one
     */
    public abstract long allocPage() throws IOException;

    /**
     * Writes to an allocated page, but doesn't commit it. A written page is
     * immediately readable even if not committed. An uncommitted page can be
     * deleted, but it remains readable until after a commit.
     *
     * @param id previously allocated page id
     * @param page data to write
     */
    public abstract void writePage(long id, /*P*/ byte[] page) throws IOException;

    /**
     * Same as writePage, except that the given buffer might be altered and a replacement might
     * be returned. Caller must not alter the original buffer if a replacement was provided,
     * and the contents of the replacement are undefined.
     *
     * @param id previously allocated page id
     * @param page data to write; implementation might alter the contents
     * @return replacement buffer, or same instance if replacement was not performed
     */
    public abstract /*P*/ byte[] evictPage(long id, /*P*/ byte[] page) throws IOException;

    /**
     * If supported, copies a page into the cache, but does not write it. Cached copy is
     * removed when read again, unless evicted sooner.
     *
     * @param id previously allocated page id
     */
    public abstract void cachePage(long id, /*P*/ byte[] page) throws IOException;

    /**
     * If supported, removes a page from the cache.
     *
     * @param id previously allocated page id
     */
    public abstract void uncachePage(long id) throws IOException;

    /**
     * Deletes a page, but doesn't commit it. Deleted pages are not used for
     * new writes, and they are still readable until after a commit. Caller
     * must ensure that a page is deleted at most once between commits.
     */
    public abstract void deletePage(long id) throws IOException;

    /**
     * Recycles a page for immediate re-use.
     */
    public abstract void recyclePage(long id) throws IOException;

    /**
     * Allocates pages for immediate use.
     *
     * @return actual amount allocated
     */
    public abstract long allocatePages(long pageCount) throws IOException;

    public long directPagePointer(long id) throws IOException {
        throw new UnsupportedOperationException();
    }

    public long dirtyPage(long id) throws IOException {
        throw new UnsupportedOperationException();
    }

    public long copyPage(long srcId, long dstId) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Access the commit lock, which prevents commits while held shared.
     */
    public CommitLock commitLock() {
        return mCommitLock;
    }

    /**
     * Scan all durable free pages, passing them to the given consumer.
     *
     * @param dst destination for scanned page ids
     */
    public abstract void scanFreeList(LongConsumer dst) throws IOException;

    /**
     * Caller must ensure that at most one compaction is in progress and that no checkpoints
     * occur when compaction starts and ends. Only one thread may control the compaction
     * sequence:
     *
     * 1. start
     * 2. scan free list (finds and moves pages in the compaction zone)
     * 3. checkpoint (ensures dirty nodes are flushed out)
     * 4. trace indexes and re-allocate pages which are in the compaction zone
     * 5. scan free list (finds and moves additional pages)
     * 6. forced checkpoint (ensures previous scan is applied)
     * 7. verify; if not, scan and checkpoint again
     * 8. end (must always be called if start returned true)
     * 8. truncate
     *
     * @return false if target cannot be met or compaction is not supported
     * @throws IllegalStateException if compaction is already in progress
     */
    public abstract boolean compactionStart(long targetPageCount) throws IOException;

    /**
     * Moves as many free pages as possible from the compaction zone into the reserve
     * list. Other threads may concurrently allocate pages and might be stalled if they are
     * required to do this work.
     *
     * @return false if aborted
     */
    public abstract boolean compactionScanFreeList() throws IOException;

    /**
     * @return false if verification failed
     */
    public abstract boolean compactionVerify() throws IOException;

    /**
     * @return false if aborted
     */
    public abstract boolean compactionEnd() throws IOException;

    /**
     * Must be called after compactionEnd, and after running a checkpoint, to reclaim pages in
     * the reserve list. Otherwise, the pages are reclaimed when restarting the database.
     */
    public abstract void compactionReclaim() throws IOException;

    /**
     * Called after compaction, to actually shrink the file.
     *
     * @return false if nothing was truncated
     */
    public abstract boolean truncatePages() throws IOException;

    /**
     * Returns the header offset for writing extra commit data into, up to 256 bytes.
     */
    public abstract int extraCommitDataOffset();

    /**
     * Durably commits all writes and deletes to the underlying device. Caller must hold commit
     * lock.
     *
     * @param resume true if resuming an aborted commit
     * @param header must be page size
     * @param callback optional callback to run during commit, which can release the exclusive
     * lock at any time
     */
    public abstract void commit(boolean resume, /*P*/ byte[] header, CommitCallback callback)
        throws IOException;

    @FunctionalInterface
    public static interface CommitCallback {
        /**
         * Write all allocated pages which should be committed. Extra header data provided is
         * stored in the PageDb header.
         *
         * @param resume true if resuming an aborted commit
         * @param header header to write extra data into, up to 256 bytes
         */
        public void prepare(boolean resume, /*P*/ byte[] header) throws IOException;
    }

    /**
     * Reads extra data that was stored with the last commit.
     *
     * @param extra optional extra data which was committed, up to 256 bytes
     */
    public abstract void readExtraCommitData(byte[] extra) throws IOException;
}
