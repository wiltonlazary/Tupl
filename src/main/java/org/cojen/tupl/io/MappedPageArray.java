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

package org.cojen.tupl.io;

import java.io.File;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.ClosedChannelException;

import java.util.EnumSet;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.sun.jna.Platform;

import org.cojen.tupl.DatabaseFullException;

/**
 * {@link PageArray} implementation which accesses a fixed sized file, fully mapped to memory.
 *
 * @author Brian S O'Neill
 */
public abstract class MappedPageArray extends PageArray {
    private static AtomicLongFieldUpdater<MappedPageArray> cMappingPtrUpdater =
        AtomicLongFieldUpdater.newUpdater(MappedPageArray.class, "mMappingPtr");

    private static AtomicReferenceFieldUpdater<MappedPageArray, Throwable> cCauseUpdater =
        AtomicReferenceFieldUpdater.newUpdater(MappedPageArray.class, Throwable.class, "mCause");

    private final long mPageCount;
    private final boolean mReadOnly;

    private volatile long mMappingPtr;
    private volatile Throwable mCause;

    /**
     * @param file file to store pages, or null if anonymous
     */
    public static MappedPageArray open(int pageSize, long pageCount,
                                       File file, EnumSet<OpenOption> options)
        throws IOException
    {
        if (pageSize < 1 || pageCount < 0 || pageCount > Long.MAX_VALUE / pageSize) {
            throw new IllegalArgumentException();
        }

        if (options == null) {
            options = EnumSet.noneOf(OpenOption.class);
        }

        if (Platform.isWindows()) {
            return new WindowsMappedPageArray(pageSize, pageCount, file, options);
        } else {
            return new PosixMappedPageArray(pageSize, pageCount, file, options);
        }
    }

    MappedPageArray(int pageSize, long pageCount, EnumSet<OpenOption> options) {
        super(pageSize);
        mPageCount = pageCount;
        mReadOnly = options.contains(OpenOption.READ_ONLY);
    }

    @Override
    public final boolean isReadOnly() {
        return mReadOnly;
    }

    @Override
    public boolean isEmpty() {
        return getPageCount() == 0;
    }

    @Override
    public long getPageCount() {
        return mPageCount;
    }

    @Override
    public void setPageCount(long count) {
    }

    @Override
    public long getPageCountLimit() {
        return mPageCount;
    }

    public void readPage(long index, byte[] buf, int offset, int length)
        throws IOException
    {
        readCheck(index);
        DirectAccess.ref(mappingPtr() + index * mPageSize, length).get(buf, offset, length);
    }

    public void readPage(long index, long ptr, int offset, int length)
        throws IOException
    {
        readCheck(index);

        ptr += offset;
        int pageSize = mPageSize;

        long srcPtr = mappingPtr() + index * pageSize;
        if (srcPtr != ptr) {
            ByteBuffer src = DirectAccess.ref(srcPtr, length);
            ByteBuffer dst = DirectAccess.ref2(ptr, length);
            dst.put(src);
        }
    }

    public void writePage(long index, byte[] buf, int offset) throws IOException {
        writeCheck(index);
        int pageSize = mPageSize;
        DirectAccess.ref(mappingPtr() + index * pageSize, pageSize).put(buf, offset, pageSize);
    }

    public void writePage(long index, long ptr, int offset) throws IOException {
        writeCheck(index);

        ptr += offset;
        int pageSize = mPageSize;

        long dstPtr = mappingPtr() + index * pageSize;
        if (dstPtr != ptr) {
            ByteBuffer dst = DirectAccess.ref(dstPtr, pageSize);
            ByteBuffer src = DirectAccess.ref2(ptr, pageSize);
            dst.put(src);
        }
    }

    @Override
    public long directPagePointer(long index) throws IOException {
        readCheck(index);
        return mappingPtr() + index * mPageSize;
    }

    /**
     * Indicate that the contents of the given page will be modified. Permits the
     * implementation to make a copy of the existing page contents, if it supports
     * snapshotting.
     *
     * @return direct pointer to destination
     */
    @Override
    public long dirtyPage(long index) throws IOException {
        return directPagePointer(index);
    }
 
    /**
     * @return direct pointer to destination
     */
    @Override
    public long copyPage(long srcIndex, long dstIndex) throws IOException {
        readCheck(srcIndex);
        writeCheck(dstIndex);

        int pageSize = mPageSize;
        long ptr = mappingPtr();
        long dstPtr = ptr + dstIndex * pageSize;
        ByteBuffer dst = DirectAccess.ref(dstPtr, pageSize);
        ByteBuffer src = DirectAccess.ref2(ptr + srcIndex * pageSize, pageSize);
        dst.put(src);

        return dstPtr;
    }

    /**
     * @return direct pointer to destination
     */
    @Override
    public long copyPageFromPointer(long srcPointer, long dstIndex) throws IOException {
        writeCheck(dstIndex);

        int pageSize = mPageSize;
        long dstPtr = mappingPtr() + dstIndex * pageSize;
        ByteBuffer dst = DirectAccess.ref(dstPtr, pageSize);
        ByteBuffer src = DirectAccess.ref2(srcPointer, pageSize);
        dst.put(src);

        return dstPtr;
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        doSync(mappingPtr(), metadata);
    }

    @Override
    public void syncPage(long index) throws IOException {
        writeCheck(index);
        doSyncPage(mappingPtr(), index);
    }

    @Override
    public final void close(Throwable cause) throws IOException {
        while (true) {
            long ptr = mMappingPtr;
            if (ptr == 0) {
                return;
            }
            cCauseUpdater.compareAndSet(this, null, cause);
            if (cMappingPtrUpdater.compareAndSet(this, ptr, 0)) {
                mCause = cause;
                doClose(ptr);
                return;
            }
        }
    }

    @Override
    public MappedPageArray open() throws IOException {
        return mMappingPtr == 0 ? doOpen() : this;
    }

    void setMappingPtr(long ptr) throws IOException {
        while (!cMappingPtrUpdater.compareAndSet(this, 0, ptr)) {
            if (mMappingPtr != 0) {
                throw new IllegalStateException();
            }
        }
    }

    abstract MappedPageArray doOpen() throws IOException;

    abstract void doSync(long mappingPtr, boolean metadata) throws IOException;

    abstract void doSyncPage(long mappingPtr, long index) throws IOException;

    abstract void doClose(long mappingPtr) throws IOException;

    long mappingPtr() throws IOException {
        long mappingPtr = mMappingPtr;
        if (mappingPtr == 0) {
            ClosedChannelException cce = new ClosedChannelException();
            cce.initCause(mCause);
            throw cce;
        }
        return mappingPtr;
    }

    private void readCheck(long index) throws IOException {
        if (index < 0) {
            throw new IOException("Negative page index: " + index);
        }
        if (index >= mPageCount) {
            throw new IOException("Page index too high: " + index + " > " + mPageCount);
        }
    }

    private void writeCheck(long index) throws IOException {
        if (index < 0) {
            throw new IOException("Negative page index: " + index);
        }
        if (index >= mPageCount) {
            throw new DatabaseFullException
                ("Mapped file length limit reached: " + (mPageCount * mPageSize));
        }
    }
}
