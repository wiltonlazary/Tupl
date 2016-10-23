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

import java.io.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SnapshotMappedDirectTest extends SnapshotMappedTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SnapshotMappedDirectTest.class.getName());
    }

    @After
    public void cleanup() throws Exception {
        deleteTempDatabases();
    }

    @Override
    public void decorate(DatabaseConfig config) throws Exception {
        super.decorate(config);
        config.directPageAccess(true);
    }

    @Test
    public void fullyMapped() throws Exception {
        Database db = newTempDatabase(50_000_000, TestUtils.OpenMode.DIRECT_MAPPED, 0);

        Index ix = db.openIndex("test");
        
        for (int i=0; i<800_000; i++) {
            ix.store(null, key(i), value(i));
        }

        db.checkpoint();

        // Create an undo log which spans a checkpoint and a snapshot.

        Transaction txn = db.newTransaction();
        for (int i=0; i<10_000; i++) {
            ix.delete(txn, key(i));
            if (i % 1000 == 0) {
                db.checkpoint();
            }
        }

        db.checkpoint();

        Snapshot s = db.beginSnapshot();
        long fullLength = s.length();
        assertTrue(fullLength > 20_000_000);

        File snapshotBase = newTempBaseFile();
        File snapshot = new File(snapshotBase.getParentFile(), snapshotBase.getName() + ".db");

        final FileOutputStream out = new FileOutputStream(snapshot);

        class Suspendable extends OutputStream {
            private long mLimit;
            private long mTotal;
            private boolean mSuspended;

            synchronized void setLimit(long limit) {
                mLimit = limit;
                notifyAll();
            }

            synchronized void waitUntilSuspended() throws Exception {
                while (!mSuspended) {
                    wait();
                }
            }

            @Override
            public void write(int b) throws IOException {
                throw new IOException();
            }

            @Override
            public synchronized void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);

                mTotal += len;

                try {
                    while (mTotal >= mLimit) {
                        mSuspended = true;
                        notifyAll();
                        wait();
                    }
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
                mSuspended = false;
                notifyAll();
            }

            @Override
            public synchronized void close() throws IOException {
                setLimit(Long.MAX_VALUE);
                out.close();
            }
        };

        // Copy half the snapshot for now.
        Suspendable sout = new Suspendable();
        sout.setLimit(fullLength / 2);

        Thread writer = new Thread(() -> {
            try {
                s.writeTo(sout);
                sout.close();
            } catch (IOException e) {
            }
        });

        writer.start();

        sout.waitUntilSuspended();

        // Commit the long transaction.
        txn.commit();
        db.checkpoint();

        // Delete everything.
        db.deleteIndex(ix).run();
        db.checkpoint();

        // Finish the snapshot.
        sout.setLimit(Long.MAX_VALUE);
        writer.join();

        DatabaseConfig restoredConfig = new DatabaseConfig()
            .directPageAccess(false)
            .baseFile(snapshotBase)
            .minCacheSize(50_000_000)
            .checkpointRate(0, null)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        Database restored = Database.open(restoredConfig);

        assertTrue(restored.verify(null));

        ix = restored.findIndex("test");
        assertTrue(ix != null);
        
        for (int i=0; i<800_000; i++) {
            fastAssertArrayEquals(value(i), ix.load(null, key(i)));
        }

        restored.close();
        db.close();
    }

    private static byte[] key(int i) {
        return ("key-" + i).getBytes();
    }

    private static byte[] value(int i) {
        return ("value-" + i).getBytes();
    }
}
