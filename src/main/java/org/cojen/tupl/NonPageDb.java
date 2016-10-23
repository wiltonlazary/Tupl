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

import java.util.Arrays;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import java.util.function.LongConsumer;

import static org.cojen.tupl.PageOps.*;

/**
 * PageDb implementation which doesn't actually work. Used for non-durable
 * databases.
 *
 * @author Brian S O'Neill
 */
final class NonPageDb extends PageDb {
    private final int mPageSize;
    private final PageCache mCache;

    private final AtomicLong mAllocId;
    private final LongAdder mFreePageCount;

    /**
     * @param cache optional
     */
    NonPageDb(int pageSize, PageCache cache) {
        mPageSize = pageSize;
        mCache = cache;
        // Next assigned id is 2, the first legal identifier.
        mAllocId = new AtomicLong(1);
        mFreePageCount = new LongAdder();
    }

    @Override
    void delete() {
    }

    @Override
    public boolean isDurable() {
        return false;
    }

    @Override
    public int allocMode() {
        return NodeUsageList.MODE_NO_EVICT;
    }

    @Override
    public Node allocLatchedNode(LocalDatabase db, int mode) throws IOException {
        Node node = db.allocLatchedNode(Utils.cheapRandom(), mode);
        long nodeId = node.mId;
        if (nodeId < 0) {
            // Recycle the id.
            nodeId = -nodeId;
            mFreePageCount.decrement();
        } else {
            nodeId = allocPage();
        }
        node.mId = nodeId;
        return node;
    }

    @Override
    public int pageSize() {
        return mPageSize;
    }

    @Override
    public long pageCount() {
        return 0;
    }

    @Override
    public void pageLimit(long limit) {
        // Ignored.
    }

    @Override
    public long pageLimit() {
        // No explicit limit.
        return -1;
    }

    @Override
    public void pageLimitOverride(long limit) {
        // Ignored.
    }

    @Override
    public Stats stats() {
        Stats stats = new Stats();
        stats.freePages = Math.max(0, mFreePageCount.sum());
        stats.totalPages = Math.max(stats.freePages, mAllocId.get());
        return stats;
    }

    @Override
    public void readPage(long id, /*P*/ byte[] page) throws IOException {
        PageCache cache = mCache;
        if (cache == null || !cache.remove(id, page, 0, pageSize())) {
            fail(false);
        }
    }

    @Override
    public long allocPage() throws IOException {
        // Cached nodes and fragmented values always require unique identifiers.
        long id = mAllocId.incrementAndGet();
        if (id > 0x0000_ffff_ffff_ffffL) {
            // Identifier is limited to 48-bit range.
            mAllocId.decrementAndGet();
            throw new DatabaseFullException();
        }
        return id;
    }

    @Override
    public void writePage(long id, /*P*/ byte[] page) throws IOException {
        PageCache cache = mCache;
        if (cache == null || !cache.add(id, page, 0, false)) {
            fail(true);
        }
    }

    @Override
    public /*P*/ byte[] evictPage(long id, /*P*/ byte[] page) throws IOException {
        writePage(id, page);
        return page;
    }

    @Override
    public void cachePage(long id, /*P*/ byte[] page) throws IOException {
        PageCache cache = mCache;
        if (cache != null && !cache.add(id, page, 0, false)) {
            fail(false);
        }
    }

    @Override
    public void uncachePage(long id) throws IOException {
        PageCache cache = mCache;
        if (cache != null) {
            cache.remove(id, p_null(), 0, 0);
        }
    }

    @Override
    public void deletePage(long id) throws IOException {
        uncachePage(id);
        mFreePageCount.increment();
    }

    @Override
    public void recyclePage(long id) throws IOException {
        deletePage(id);
    }

    @Override
    public long allocatePages(long pageCount) throws IOException {
        // Do nothing.
        return 0;
    }

    @Override
    public void scanFreeList(LongConsumer dst) throws IOException {
        // No durable pages to scan.
        return;
    }

    @Override
    public boolean compactionStart(long targetPageCount) throws IOException {
        return false;
    }

    @Override
    public boolean compactionScanFreeList() throws IOException {
        return false;
    }

    @Override
    public boolean compactionVerify() throws IOException {
        return false;
    }

    @Override
    public boolean compactionEnd() throws IOException {
        return false;
    }

    @Override
    public void compactionReclaim() throws IOException {
    }

    @Override
    public boolean truncatePages() throws IOException {
        return false;
    }

    @Override
    public int extraCommitDataOffset() {
        return 0;
    }

    @Override
    public void commit(boolean resume, /*P*/ byte[] header, CommitCallback callback)
        throws IOException
    {
        // This is more of an assertion failure.
        throw new DatabaseException("Cannot commit to a non-durable database");
    }

    @Override
    public void readExtraCommitData(byte[] extra) throws IOException {
        Arrays.fill(extra, (byte) 0);
    }

    @Override
    public void close() {
        if (mCache != null) {
            mCache.close();
        }
    }

    @Override
    public void close(Throwable cause) {
        close();
    }

    private static void fail(boolean forWrite) throws DatabaseException {
        if (forWrite) {
            throw new DatabaseFullException();
        } else {
            // This is more of an assertion failure.
            throw new DatabaseException("Cannot read from a non-durable database");
        }
    }
}
