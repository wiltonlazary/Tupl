/*
 *  Copyright 2013 Brian S O'Neill
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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4Factory;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CompressedPageArray extends PageArray {
    public static void main(String[] args) throws Exception {
        String base = args[0];

        PageArray cpa = new CompressedPageArray(4096, new File(base));

        Database db = Database.open(new DatabaseConfig()
                                    .baseFilePath(base)
                                    .dataPageArray(cpa)
                                    .durabilityMode(DurabilityMode.NO_FLUSH)
                                    .minCacheSize(10000000));

        Index ix = db.openIndex("foo");
        for (int i=0; i<100000000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            ix.store(null, key, value);
            //ix.load(null, key);
        }

        db.checkpoint();
    }

    private final Database mDataDb;
    private final Index mPages;
    private final LZ4Compressor mCompressor;
    private final int mMaxCompressedLength;
    private final LZ4FastDecompressor mDecompressor;

    public CompressedPageArray(int pageSize, File baseFile) throws IOException {
        this(pageSize, Database.open(new DatabaseConfig()
                                     .baseFile(new File(baseFile.getPath() + ".data"))
                                     .pageSize(selectDataPageSize(pageSize))
                                     .checkpointSizeThreshold(0)
                                     .checkpointDelayThreshold(0, null)
                                     .durabilityMode(DurabilityMode.NO_FLUSH)
                                     .minCacheSize(10000000))); // FIXME: configurable
    }

    public CompressedPageArray(int pageSize, Database dataDb) throws IOException {
        super(pageSize);

        mDataDb = dataDb;
        mPages = dataDb.openIndex("pages");

        LZ4Factory factory = LZ4Factory.fastestJavaInstance();

        mCompressor = factory.fastCompressor();
        mMaxCompressedLength = mCompressor.maxCompressedLength(pageSize());
        mDecompressor = factory.fastDecompressor();
    }

    private static int selectDataPageSize(int topPageSize) {
        return topPageSize >= 4096 ? 512 : (topPageSize * 8);
    }

    @Override
    public boolean isReadOnly() {
        // TODO
        return false;
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mPages.load(Transaction.BOGUS, keyFor(0)) == null;
    }

    @Override
    public long getPageCount() throws IOException {
        Cursor c = mPages.newCursor(Transaction.BOGUS);
        c.last();
        byte[] key = c.key();
        c.reset();
        return key == null ? 0 : (indexFor(key) + 1);
    }

    @Override
    public void setPageCount(long count) throws IOException {
        Cursor c = mPages.newCursor(null);
        try {
            c.autoload(false);
            for (c.findGe(keyFor(count)); c.key() != null; c.next()) {
                c.store(null);
            }
        } finally {
            c.reset();
        }
    }

    @Override
    public void readPage(long index, byte[] buf, int offset) throws IOException {
        // TODO: Use stream API and buffer pool.
        byte[] value = mPages.load(Transaction.BOGUS, keyFor(index));
        mDecompressor.decompress(value, 0, buf, offset, pageSize());
    }

    @Override
    public int readPartial(long index, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        // TODO: Use stream API and buffer pool.
        byte[] page = new byte[pageSize()];
        readPage(index, page, 0);
        System.arraycopy(page, start, buf, offset, length);
        return length;
    }

    @Override
    public void writePage(long index, byte[] buf) throws IOException {
        // TODO: Use stream API and buffer pool.
        byte[] comp = new byte[mMaxCompressedLength];
        int len = mCompressor.compress(buf, 0, pageSize(), comp, 0);
        byte[] value = new byte[len];
        System.arraycopy(comp, 0, value, 0, len);
        mPages.store(Transaction.BOGUS, keyFor(index), value);
    }

    @Override
    public void writePage(long index, byte[] buf, int offset) throws IOException {
        // TODO: Use stream API and buffer pool.
        byte[] comp = new byte[mMaxCompressedLength];
        int len = mCompressor.compress(buf, offset, pageSize(), comp, 0);
        byte[] value = new byte[len];
        System.arraycopy(comp, 0, value, 0, len);
        mPages.store(Transaction.BOGUS, keyFor(index), value);
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        // Pages are written non-transactionally, and so checkpoint must be performed.
        mDataDb.checkpoint();
    }

    @Override
    public void close(Throwable cause) throws IOException {
        mDataDb.close(cause);
    }

    private static byte[] keyFor(long index) {
        byte[] key = new byte[6];
        Utils.encodeInt48BE(key, 0, index);
        return key;
    }

    private static long indexFor(byte[] key) {
        return Utils.decodeUnsignedInt48BE(key, 0);
    }
}
