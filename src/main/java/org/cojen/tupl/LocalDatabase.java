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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import java.util.concurrent.locks.ReentrantLock;

import static java.lang.System.arraycopy;

import static java.util.Arrays.fill;

import org.cojen.tupl.ext.ReplicationManager;
import org.cojen.tupl.ext.TransactionHandler;

import org.cojen.tupl.io.CauseCloseable;
import org.cojen.tupl.io.FileFactory;
import org.cojen.tupl.io.MappedPageArray;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;

import org.cojen.tupl.util.Latch;

import static org.cojen.tupl.Node.*;
import static org.cojen.tupl.PageOps.*;
import static org.cojen.tupl.Utils.*;

/**
 * Standard database implementation.
 *
 * @author Brian S O'Neill
 */
final class LocalDatabase extends AbstractDatabase {
    private static final int DEFAULT_CACHED_NODES = 1000;
    // +2 for registry and key map root nodes, +1 for one user index, and +2 for at least one
    // usage list to function correctly.
    private static final int MIN_CACHED_NODES = 5;

    private static final long PRIMER_MAGIC_NUMBER = 4943712973215968399L;

    private static final String INFO_FILE_SUFFIX = ".info";
    private static final String LOCK_FILE_SUFFIX = ".lock";
    static final String PRIMER_FILE_SUFFIX = ".primer";
    static final String REDO_FILE_SUFFIX = ".redo.";

    private static int nodeCountFromBytes(long bytes, int pageSize) {
        if (bytes <= 0) {
            return 0;
        }
        pageSize += NODE_OVERHEAD;
        bytes += pageSize - 1;
        if (bytes <= 0) {
            // Overflow.
            return Integer.MAX_VALUE;
        }
        long count = bytes / pageSize;
        return count <= Integer.MAX_VALUE ? (int) count : Integer.MAX_VALUE;
    }

    private static long byteCountFromNodes(int nodes, int pageSize) {
        return nodes * (long) (pageSize + NODE_OVERHEAD);
    }

    private static final int ENCODING_VERSION = 20130112;

    private static final int I_ENCODING_VERSION        = 0;
    private static final int I_ROOT_PAGE_ID            = I_ENCODING_VERSION + 4;
    private static final int I_MASTER_UNDO_LOG_PAGE_ID = I_ROOT_PAGE_ID + 8;
    private static final int I_TRANSACTION_ID          = I_MASTER_UNDO_LOG_PAGE_ID + 8;
    private static final int I_CHECKPOINT_NUMBER       = I_TRANSACTION_ID + 8;
    private static final int I_REDO_TXN_ID             = I_CHECKPOINT_NUMBER + 8;
    private static final int I_REDO_POSITION           = I_REDO_TXN_ID + 8;
    private static final int I_REPL_ENCODING           = I_REDO_POSITION + 8;
    private static final int HEADER_SIZE               = I_REPL_ENCODING + 8;

    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static final int MINIMUM_PAGE_SIZE = 512;
    private static final int MAXIMUM_PAGE_SIZE = 65536;

    private static final int OPEN_REGULAR = 0, OPEN_DESTROY = 1, OPEN_TEMP = 2;

    final EventListener mEventListener;

    final TransactionHandler mCustomTxnHandler;

    private final File mBaseFile;
    private final LockedFile mLockFile;

    final DurabilityMode mDurabilityMode;
    final long mDefaultLockTimeoutNanos;
    final LockManager mLockManager;
    final RedoWriter mRedoWriter;
    final PageDb mPageDb;
    final int mPageSize;

    private final PagePool mSparePagePool;

    private final Object mArena;
    private final NodeUsageList[] mUsageLists;

    private final CommitLock mCommitLock;

    // Is either CACHED_DIRTY_0 or CACHED_DIRTY_1. Access is guarded by commit lock.
    private byte mCommitState;

    // State to apply to nodes which have just been read. Is CACHED_DIRTY_0 for empty databases
    // which have never checkpointed, but is CACHED_CLEAN otherwise.
    private volatile byte mInitialReadState = CACHED_CLEAN;

    // Set during checkpoint after commit state has switched. If checkpoint aborts, next
    // checkpoint will resume with this commit header and master undo log.
    private /*P*/ byte[] mCommitHeader = p_null();
    private UndoLog mCommitMasterUndoLog;

    // Typically opposite of mCommitState, or negative if checkpoint is not in
    // progress. Indicates which nodes are being flushed by the checkpoint.
    private volatile int mCheckpointFlushState = CHECKPOINT_NOT_FLUSHING;

    private static final int CHECKPOINT_FLUSH_PREPARE = -2, CHECKPOINT_NOT_FLUSHING = -1;

    // The root tree, which maps tree ids to other tree root node ids.
    private final Tree mRegistry;

    static final byte KEY_TYPE_INDEX_NAME   = 0; // prefix for name to id mapping
    static final byte KEY_TYPE_INDEX_ID     = 1; // prefix for id to name mapping
    static final byte KEY_TYPE_TREE_ID_MASK = 2; // full key for random tree id mask
    static final byte KEY_TYPE_NEXT_TREE_ID = 3; // full key for tree id sequence
    static final byte KEY_TYPE_TRASH_ID     = 4; // prefix for id to name mapping of trash

    // Various mappings, defined by KEY_TYPE_ fields.
    private final Tree mRegistryKeyMap;

    private final Latch mOpenTreesLatch;
    // Maps tree names to open trees.
    // Must be a concurrent map because we rely on concurrent iteration.
    private final Map<byte[], TreeRef> mOpenTrees;
    private final LHashTable.Obj<TreeRef> mOpenTreesById;
    private final ReferenceQueue<Tree> mOpenTreesRefQueue;

    private final NodeDirtyList mDirtyList;

    // Map of all loaded nodes.
    private final Node[] mNodeMapTable;
    private final Latch[] mNodeMapLatches;

    final int mMaxKeySize;
    final int mMaxEntrySize;
    final int mMaxFragmentedEntrySize;

    // Fragmented values which are transactionally deleted go here.
    private volatile FragmentedTrash mFragmentedTrash;

    // Pre-calculated maximum capacities for inode levels.
    private final long[] mFragmentInodeLevelCaps;

    // Stripe the transaction contexts, for improved concurrency.
    private final TransactionContext[] mTxnContexts;

    // Checkpoint lock is fair, to ensure that user checkpoint requests are not stalled for too
    // long by checkpoint thread.
    private final ReentrantLock mCheckpointLock = new ReentrantLock(true);

    private long mLastCheckpointNanos;

    private volatile Checkpointer mCheckpointer;

    final TempFileManager mTempFileManager;

    /*P*/ // [|
    /*P*/ // final boolean mFullyMapped;
    /*P*/ // ]

    volatile boolean mClosed;
    volatile Throwable mClosedCause;

    private static final AtomicReferenceFieldUpdater<LocalDatabase, Throwable>
        cClosedCauseUpdater = AtomicReferenceFieldUpdater.newUpdater
        (LocalDatabase.class, Throwable.class, "mClosedCause");

    /**
     * Open a database, creating it if necessary.
     */
    static LocalDatabase open(DatabaseConfig config) throws IOException {
        config = config.clone();
        LocalDatabase db = new LocalDatabase(config, OPEN_REGULAR);
        db.finishInit(config);
        return db;
    }

    /**
     * Delete the contents of an existing database, and replace it with an
     * empty one. When using a raw block device for the data file, this method
     * must be used to format it.
     */
    static LocalDatabase destroy(DatabaseConfig config) throws IOException {
        config = config.clone();
        if (config.mReadOnly) {
            throw new IllegalArgumentException("Cannot destroy read-only database");
        }
        LocalDatabase db = new LocalDatabase(config, OPEN_DESTROY);
        db.finishInit(config);
        return db;
    }

    /**
     * @param config base file is set as a side-effect
     */
    static Tree openTemp(TempFileManager tfm, DatabaseConfig config) throws IOException {
        File file = tfm.createTempFile();
        config.baseFile(file);
        config.dataFile(file);
        config.createFilePath(false);
        config.durabilityMode(DurabilityMode.NO_FLUSH);
        LocalDatabase db = new LocalDatabase(config, OPEN_TEMP);
        tfm.register(file, db);
        db.mCheckpointer = new Checkpointer(db, config);
        db.mCheckpointer.start(false);
        return db.mRegistry;
    }

    /**
     * @param config unshared config
     */
    private LocalDatabase(DatabaseConfig config, int openMode) throws IOException {
        config.mEventListener = mEventListener = SafeEventListener.makeSafe(config.mEventListener);

        mCustomTxnHandler = config.mTxnHandler;

        mBaseFile = config.mBaseFile;
        final File[] dataFiles = config.dataFiles();

        int pageSize = config.mPageSize;
        boolean explicitPageSize = true;
        if (pageSize <= 0) {
            config.pageSize(pageSize = DEFAULT_PAGE_SIZE);
            explicitPageSize = false;
        } else if (pageSize < MINIMUM_PAGE_SIZE) {
            throw new IllegalArgumentException
                ("Page size is too small: " + pageSize + " < " + MINIMUM_PAGE_SIZE);
        } else if (pageSize > MAXIMUM_PAGE_SIZE) {
            throw new IllegalArgumentException
                ("Page size is too large: " + pageSize + " > " + MAXIMUM_PAGE_SIZE);
        } else if ((pageSize & 1) != 0) {
            throw new IllegalArgumentException
                ("Page size must be even: " + pageSize);
        }

        int minCache, maxCache;
        cacheSize: {
            long minCachedBytes = Math.max(0, config.mMinCachedBytes);
            long maxCachedBytes = Math.max(0, config.mMaxCachedBytes);

            if (maxCachedBytes == 0) {
                maxCachedBytes = minCachedBytes;
                if (maxCachedBytes == 0) {
                    minCache = maxCache = DEFAULT_CACHED_NODES;
                    break cacheSize;
                }
            }

            if (minCachedBytes > maxCachedBytes) {
                throw new IllegalArgumentException
                    ("Minimum cache size exceeds maximum: " +
                     minCachedBytes + " > " + maxCachedBytes);
            }

            minCache = nodeCountFromBytes(minCachedBytes, pageSize);
            maxCache = nodeCountFromBytes(maxCachedBytes, pageSize);

            minCache = Math.max(MIN_CACHED_NODES, minCache);
            maxCache = Math.max(MIN_CACHED_NODES, maxCache);
        }

        // Update config such that info file is correct.
        config.mMinCachedBytes = byteCountFromNodes(minCache, pageSize);
        config.mMaxCachedBytes = byteCountFromNodes(maxCache, pageSize);

        mDurabilityMode = config.mDurabilityMode;
        mDefaultLockTimeoutNanos = config.mLockTimeoutNanos;
        mLockManager = new LockManager(this, config.mLockUpgradeRule, mDefaultLockTimeoutNanos);

        // Initialize NodeMap, the primary cache of Nodes.
        final int procCount = Runtime.getRuntime().availableProcessors();
        {
            int latches = Utils.roundUpPower2(procCount * 16);
            int capacity = Utils.roundUpPower2(maxCache);
            if (capacity < 0) {
                capacity = 0x40000000;
            }
            mNodeMapTable = new Node[capacity];
            mNodeMapLatches = new Latch[latches];
            for (int i=0; i<latches; i++) {
                mNodeMapLatches[i] = new Latch();
            }
        }

        if (mBaseFile != null && !config.mReadOnly && config.mMkdirs) {
            FileFactory factory = config.mFileFactory;

            final boolean baseDirectoriesCreated;
            File baseDir = mBaseFile.getParentFile();
            if (factory == null) {
                baseDirectoriesCreated = baseDir.mkdirs();
            } else {
                baseDirectoriesCreated = factory.createDirectories(baseDir);
            }

            if (!baseDirectoriesCreated && !baseDir.exists()) {
                throw new FileNotFoundException("Could not create directory: " + baseDir);
            }

            if (dataFiles != null) {
                for (File f : dataFiles) {
                    final boolean dataDirectoriesCreated;
                    File dataDir = f.getParentFile();
                    if (factory == null) {
                        dataDirectoriesCreated = dataDir.mkdirs();
                    } else {
                        dataDirectoriesCreated = factory.createDirectories(dataDir);
                    }

                    if (!dataDirectoriesCreated && !dataDir.exists()) {
                        throw new FileNotFoundException("Could not create directory: " + dataDir);
                    }
                }
            }
        }

        try {
            // Create lock file, preventing database from being opened multiple times.
            if (mBaseFile == null || openMode == OPEN_TEMP) {
                mLockFile = null;
            } else {
                File lockFile = new File(mBaseFile.getPath() + LOCK_FILE_SUFFIX);

                FileFactory factory = config.mFileFactory;
                if (factory != null && !config.mReadOnly) {
                    factory.createFile(lockFile);
                }

                mLockFile = new LockedFile(lockFile, config.mReadOnly);
            }

            if (openMode == OPEN_DESTROY) {
                deleteRedoLogFiles();
            }

            final long cacheInitStart = System.nanoTime();

            // Create or retrieve optional page cache.
            PageCache cache = config.pageCache(mEventListener);

            if (cache != null) {
                // Update config such that info file is correct.
                config.mSecondaryCacheSize = cache.capacity();
            }

            /*P*/ // [|
            /*P*/ // boolean fullyMapped = false;
            /*P*/ // ]

            if (dataFiles == null) {
                PageArray dataPageArray = config.mDataPageArray;
                if (dataPageArray == null) {
                    mPageDb = new NonPageDb(pageSize, cache);
                } else {
                    dataPageArray = dataPageArray.open();
                    Crypto crypto = config.mCrypto;
                    mPageDb = DurablePageDb.open
                        (dataPageArray, cache, crypto, openMode == OPEN_DESTROY);
                    /*P*/ // [|
                    /*P*/ // fullyMapped = crypto == null && cache == null
                    /*P*/ //               && dataPageArray instanceof MappedPageArray;
                    /*P*/ // ]
                }
            } else {
                EnumSet<OpenOption> options = config.createOpenOptions();
                mPageDb = DurablePageDb.open
                    (explicitPageSize, pageSize,
                     dataFiles, config.mFileFactory, options,
                     cache, config.mCrypto, openMode == OPEN_DESTROY);
            }

            /*P*/ // [|
            /*P*/ // mFullyMapped = fullyMapped;
            /*P*/ // ]

            // Actual page size might differ from configured size.
            config.pageSize(pageSize = mPageSize = mPageDb.pageSize());

            /*P*/ // [
            config.mDirectPageAccess = false;
            /*P*/ // |
            /*P*/ // config.mDirectPageAccess = true;
            /*P*/ // ]

            // Write info file of properties, after database has been opened and after page
            // size is truly known.
            if (mBaseFile != null && openMode != OPEN_TEMP && !config.mReadOnly) {
                File infoFile = new File(mBaseFile.getPath() + INFO_FILE_SUFFIX);

                FileFactory factory = config.mFileFactory;
                if (factory != null) {
                    factory.createFile(infoFile);
                }

                BufferedWriter w = new BufferedWriter
                    (new OutputStreamWriter(new FileOutputStream(infoFile), "UTF-8"));

                try {
                    config.writeInfo(w);
                } finally {
                    w.close();
                }
            }

            mCommitLock = mPageDb.commitLock();

            // Pre-allocate nodes. They are automatically added to the usage lists, and so
            // nothing special needs to be done to allow them to get used. Since the initial
            // state is clean, evicting these nodes does nothing.

            if (mEventListener != null) {
                mEventListener.notify(EventType.CACHE_INIT_BEGIN,
                                      "Initializing %1$d cached nodes", minCache);
            }

            NodeUsageList[] usageLists;
            try {
                // Try to allocate the minimum cache size into an arena, which has lower memory
                // overhead, is page aligned, and takes less time to zero-fill.
                arenaAlloc: {
                    // If database is fully mapped, then no cached pages are allocated at all.
                    // Nodes point directly to a mapped region of memory.
                    /*P*/ // [|
                    /*P*/ // if (mFullyMapped) {
                    /*P*/ //     mArena = null;
                    /*P*/ //     break arenaAlloc;
                    /*P*/ // }
                    /*P*/ // ]

                    try {
                        mArena = p_arenaAlloc(pageSize, minCache); 
                    } catch (IOException e) {
                        OutOfMemoryError oom = new OutOfMemoryError();
                        oom.initCause(e);
                        throw oom;
                    }
                }

                int stripes = roundUpPower2(procCount * 4);

                int stripeSize;
                while (true) {
                    stripeSize = maxCache / stripes;
                    if (stripes <= 1 || stripeSize >= 100) {
                        break;
                    }
                    stripes >>= 1;
                }

                int rem = maxCache % stripes;

                usageLists = new NodeUsageList[stripes];
  
                for (int i=0; i<stripes; i++) {
                    int size = stripeSize;
                    if (rem > 0) {
                        size++;
                        rem--;
                    }
                    usageLists[i] = new NodeUsageList(this, size);
                }

                stripeSize = minCache / stripes;
                rem = minCache % stripes;

                for (NodeUsageList usageList : usageLists) {
                    int size = stripeSize;
                    if (rem > 0) {
                        size++;
                        rem--;
                    }
                    usageList.initialize(mArena, size);
                }
            } catch (OutOfMemoryError e) {
                usageLists = null;
                OutOfMemoryError oom = new OutOfMemoryError
                    ("Unable to allocate the minimum required number of cached nodes: " +
                     minCache + " (" + (minCache * (long) (pageSize + NODE_OVERHEAD)) + " bytes)");
                oom.initCause(e.getCause());
                throw oom;
            }

            mUsageLists = usageLists;

            if (mEventListener != null) {
                double duration = (System.nanoTime() - cacheInitStart) / 1_000_000_000.0;
                mEventListener.notify(EventType.CACHE_INIT_COMPLETE,
                                      "Cache initialization completed in %1$1.3f seconds",
                                      duration, TimeUnit.SECONDS);
            }

            mTxnContexts = new TransactionContext[procCount * 4];
            for (int i=0; i<mTxnContexts.length; i++) {
                mTxnContexts[i] = new TransactionContext(procCount);
            };

            mSparePagePool = new PagePool(mPageSize, procCount);

            mCommitLock.acquireExclusive();
            try {
                mCommitState = CACHED_DIRTY_0;
            } finally {
                mCommitLock.releaseExclusive();
            }

            byte[] header = new byte[HEADER_SIZE];
            mPageDb.readExtraCommitData(header);

            // Also verifies the database and replication encodings.
            Node rootNode = loadRegistryRoot(header, config.mReplManager);

            // Cannot call newTreeInstance because mRedoWriter isn't set yet.
            if (config.mReplManager != null) {
                mRegistry = new TxnTree(this, Tree.REGISTRY_ID, null, rootNode);
            } else {
                mRegistry = new Tree(this, Tree.REGISTRY_ID, null, rootNode);
            }

            mOpenTreesLatch = new Latch();
            if (openMode == OPEN_TEMP) {
                mOpenTrees = Collections.emptyMap();
                mOpenTreesById = new LHashTable.Obj<>(0);
                mOpenTreesRefQueue = null;
            } else {
                mOpenTrees = new ConcurrentSkipListMap<>(KeyComparator.THE);
                mOpenTreesById = new LHashTable.Obj<>(16);
                mOpenTreesRefQueue = new ReferenceQueue<>();
            }

            long txnId = decodeLongLE(header, I_TRANSACTION_ID);

            long redoNum = decodeLongLE(header, I_CHECKPOINT_NUMBER);
            long redoPos = decodeLongLE(header, I_REDO_POSITION);
            long redoTxnId = decodeLongLE(header, I_REDO_TXN_ID);

            if (openMode == OPEN_TEMP) {
                mRegistryKeyMap = null;
            } else {
                mRegistryKeyMap = openInternalTree(Tree.REGISTRY_KEY_MAP_ID, true, config);
            }

            mDirtyList = new NodeDirtyList();

            if (openMode != OPEN_TEMP) {
                Tree tree = openInternalTree(Tree.FRAGMENTED_TRASH_ID, false, config);
                if (tree != null) {
                    mFragmentedTrash = new FragmentedTrash(tree);
                }
            }

            // Key size is limited to ensure that internal nodes can hold at least two keys.
            // Absolute maximum is dictated by key encoding, as described in Node class.
            mMaxKeySize = Math.min(16383, (pageSize >> 1) - 22);

            // Limit maximum non-fragmented entry size to 0.75 of usable node size.
            mMaxEntrySize = ((pageSize - Node.TN_HEADER_SIZE) * 3) >> 2;

            // Limit maximum fragmented entry size to guarantee that 2 entries fit. Each also
            // requires 2 bytes for pointer and up to 3 bytes for value length field.
            mMaxFragmentedEntrySize = (pageSize - Node.TN_HEADER_SIZE - (2 + 3 + 2 + 3)) >> 1;

            mFragmentInodeLevelCaps = calculateInodeLevelCaps(mPageSize);

            long recoveryStart = 0;
            if (mBaseFile == null || openMode == OPEN_TEMP) {
                mRedoWriter = null;
            } else {
                // Perform recovery by examining redo and undo logs.

                if (mEventListener != null) {
                    mEventListener.notify(EventType.RECOVERY_BEGIN, "Database recovery begin");
                    recoveryStart = System.nanoTime();
                }

                LHashTable.Obj<LocalTransaction> txns = new LHashTable.Obj<>(16);
                {
                    long masterNodeId = decodeLongLE(header, I_MASTER_UNDO_LOG_PAGE_ID);
                    if (masterNodeId != 0) {
                        if (mEventListener != null) {
                            mEventListener.notify
                                (EventType.RECOVERY_LOAD_UNDO_LOGS, "Loading undo logs");
                        }
                        UndoLog.recoverMasterUndoLog(this, masterNodeId)
                            .recoverTransactions(txns, LockMode.UPGRADABLE_READ, 0L);
                    }
                }

                if (mCustomTxnHandler != null) {
                    // Although handler shouldn't access the database yet, be safe and call
                    // this method at the point that the database is mostly functional. All
                    // other custom methods will be called soon as well.
                    mCustomTxnHandler.setCheckpointLock(this, mCommitLock);
                }

                ReplicationManager rm = config.mReplManager;
                if (rm != null) {
                    rm.start(redoPos);
                    ReplRedoEngine engine = new ReplRedoEngine
                        (rm, config.mMaxReplicaThreads, this, txns);
                    mRedoWriter = engine.initWriter(redoNum);

                    // Cannot start recovery until constructor is finished and final field
                    // values are visible to other threads. Pass the state to the caller
                    // through the config object.
                    config.mReplRecoveryStartNanos = recoveryStart;
                    config.mReplInitialTxnId = redoTxnId;
                } else {
                    // Apply cache primer before applying redo logs.
                    applyCachePrimer(config);

                    long logId = redoNum;

                    // Make sure old redo logs are deleted. Process might have exited
                    // before last checkpoint could delete them.
                    for (int i=1; i<=2; i++) {
                        RedoLog.deleteOldFile(config.mBaseFile, logId - i);
                    }

                    RedoLogApplier applier = new RedoLogApplier(this, txns);
                    RedoLog replayLog = new RedoLog(config, logId, redoPos);

                    // As a side-effect, log id is set one higher than last file scanned.
                    Set<File> redoFiles = replayLog.replay
                        (applier, mEventListener, EventType.RECOVERY_APPLY_REDO_LOG,
                         "Applying redo log: %1$d");

                    boolean doCheckpoint = !redoFiles.isEmpty();

                    // Avoid re-using transaction ids used by recovery.
                    redoTxnId = applier.mHighestTxnId;
                    if (redoTxnId != 0) {
                        // Subtract for modulo comparison.
                        if (txnId == 0 || (redoTxnId - txnId) > 0) {
                            txnId = redoTxnId;
                        }
                    }

                    if (txns.size() > 0) {
                        // Rollback or truncate all remaining transactions. They were never
                        // explicitly rolled back, or they were committed but not cleaned up.

                        if (mEventListener != null) {
                            mEventListener.notify
                                (EventType.RECOVERY_PROCESS_REMAINING,
                                 "Processing remaining transactions");
                        }

                        txns.traverse((entry) -> {
                            entry.value.recoveryCleanup(true);
                            return false;
                        });

                        doCheckpoint = true;
                    }

                    // New redo logs begin with identifiers one higher than last scanned.
                    mRedoWriter = new RedoLog(config, replayLog);

                    // TODO: If any exception is thrown before checkpoint is complete, delete
                    // the newly created redo log file.

                    if (doCheckpoint) {
                        checkpoint(true, 0, 0);
                        // Only cleanup after successful checkpoint.
                        for (File file : redoFiles) {
                            file.delete();
                        }
                    }

                    // Delete lingering fragmented values after undo logs have been processed,
                    // ensuring deletes were committed.
                    emptyAllFragmentedTrash(true);

                    recoveryComplete(recoveryStart);
                }
            }

            for (TransactionContext txnContext : mTxnContexts) {
                txnContext.resetTransactionId(txnId++);
            }

            if (mBaseFile == null || openMode == OPEN_TEMP) {
                mTempFileManager = null;
            } else {
                mTempFileManager = new TempFileManager(mBaseFile, config.mFileFactory);
            }
        } catch (Throwable e) {
            // Close, but don't double report the exception since construction never finished.
            closeQuietly(null, this);
            throw e;
        }
    }

    /**
     * Post construction, allow additional threads access to the database.
     */
    private void finishInit(DatabaseConfig config) throws IOException {
        if (mRedoWriter == null && mTempFileManager == null) {
            // Nothing is durable and nothing to ever clean up 
            return;
        }

        Checkpointer c = new Checkpointer(this, config);
        mCheckpointer = c;

        // Register objects to automatically shutdown.
        c.register(mRedoWriter);
        c.register(mTempFileManager);

        if (mRedoWriter instanceof ReplRedoWriter) {
            // Need to do this after mRedoWriter is assigned, ensuring that trees are opened as
            // TxnTree instances.
            applyCachePrimer(config);
        }

        if (config.mCachePriming && mPageDb.isDurable()) {
            c.register(new ShutdownPrimer(this));
        }

        // Must tag the trashed trees before starting replication and recovery. Otherwise,
        // trees recently deleted might get double deleted.
        Tree trashed = openNextTrashedTree(null);

        if (trashed != null) {
            Thread deletion = new Thread
                (new Deletion(trashed, true, mEventListener), "IndexDeletion");
            deletion.setDaemon(true);
            deletion.start();
        }

        boolean initialCheckpoint = false;

        if (mRedoWriter instanceof ReplRedoController) {
            // Start replication and recovery.
            ReplRedoController controller = (ReplRedoController) mRedoWriter;
            try {
                // Pass the original listener, in case it has been specialized.
                controller.recover(config.mReplInitialTxnId, config.mEventListener);
            } catch (Throwable e) {
                closeQuietly(null, this, e);
                throw e;
            }
            recoveryComplete(config.mReplRecoveryStartNanos);
            initialCheckpoint = true;
        }

        c.start(initialCheckpoint);
    }

    private void applyCachePrimer(DatabaseConfig config) {
        if (mPageDb.isDurable()) {
            File primer = primerFile();
            try {
                if (config.mCachePriming && primer.exists()) {
                    if (mEventListener != null) {
                        mEventListener.notify(EventType.RECOVERY_CACHE_PRIMING,
                                              "Cache priming");
                    }
                    FileInputStream fin;
                    try {
                        fin = new FileInputStream(primer);
                        try (InputStream bin = new BufferedInputStream(fin)) {
                            applyCachePrimer(bin);
                        } catch (IOException e) {
                            fin.close();
                            primer.delete();
                        }
                    } catch (IOException e) {
                    }
                }
            } finally {
                primer.delete();
            }
        }
    }

    static class ShutdownPrimer implements ShutdownHook {
        private final WeakReference<LocalDatabase> mDatabaseRef;

        ShutdownPrimer(LocalDatabase db) {
            mDatabaseRef = new WeakReference<>(db);
        }

        @Override
        public void shutdown() {
            LocalDatabase db = mDatabaseRef.get();
            if (db == null) {
                return;
            }

            File primer = db.primerFile();

            FileOutputStream fout;
            try {
                fout = new FileOutputStream(primer);
                try {
                    try (OutputStream bout = new BufferedOutputStream(fout)) {
                        db.createCachePrimer(bout);
                    }
                } catch (IOException e) {
                    fout.close();
                    primer.delete();
                }
            } catch (IOException e) {
            }
        }
    }

    File primerFile() {
        return new File(mBaseFile.getPath() + PRIMER_FILE_SUFFIX);
    }

    private void recoveryComplete(long recoveryStart) {
        if (mRedoWriter != null && mEventListener != null) {
            double duration = (System.nanoTime() - recoveryStart) / 1_000_000_000.0;
            mEventListener.notify(EventType.RECOVERY_COMPLETE,
                                  "Recovery completed in %1$1.3f seconds",
                                  duration, TimeUnit.SECONDS);
        }
    }

    private void deleteRedoLogFiles() throws IOException {
        if (mBaseFile != null) {
            deleteNumberedFiles(mBaseFile, REDO_FILE_SUFFIX);
        }
    }

    @Override
    public Index findIndex(byte[] name) throws IOException {
        return openTree(name.clone(), false);
    }

    @Override
    public Index openIndex(byte[] name) throws IOException {
        return openTree(name.clone(), true);
    }

    @Override
    public Index indexById(long id) throws IOException {
        return indexById(null, id);
    }

    Index indexById(Transaction txn, long id) throws IOException {
        if (Tree.isInternal(id)) {
            throw new IllegalArgumentException("Invalid id: " + id);
        }

        Index index;

        mCommitLock.lock();
        try {
            if ((index = lookupIndexById(id)) != null) {
                return index;
            }

            byte[] idKey = new byte[9];
            idKey[0] = KEY_TYPE_INDEX_ID;
            encodeLongBE(idKey, 1, id);

            byte[] name = mRegistryKeyMap.load(txn, idKey);

            if (name == null) {
                checkClosed();
                return null;
            }

            index = openTree(txn, name, false);
        } catch (Throwable e) {
            DatabaseException.rethrowIfRecoverable(e);
            throw closeOnFailure(this, e);
        } finally {
            mCommitLock.unlock();
        }

        if (index == null) {
            // Registry needs to be repaired to fix this.
            throw new DatabaseException("Unable to find index in registry");
        }

        return index;
    }

    /**
     * @return null if index is not open
     */
    private Tree lookupIndexById(long id) {
        mOpenTreesLatch.acquireShared();
        try {
            LHashTable.ObjEntry<TreeRef> entry = mOpenTreesById.get(id);
            return entry == null ? null : entry.value.get();
        } finally {
            mOpenTreesLatch.releaseShared();
        }
    }

    /**
     * Allows access to internal indexes which can use the redo log.
     */
    Index anyIndexById(long id) throws IOException {
        return anyIndexById(null, id);
    }

    /**
     * Allows access to internal indexes which can use the redo log.
     */
    Index anyIndexById(Transaction txn, long id) throws IOException {
        if (id == Tree.REGISTRY_KEY_MAP_ID) {
            return mRegistryKeyMap;
        } else if (id == Tree.FRAGMENTED_TRASH_ID) {
            return fragmentedTrash().mTrash;
        }
        return indexById(txn, id);
    }

    @Override
    public void renameIndex(Index index, byte[] newName) throws IOException {
        renameIndex(index, newName.clone(), 0);
    }

    /**
     * @param newName not cloned
     * @param redoTxnId non-zero if rename is performed by recovery
     */
    void renameIndex(final Index index, final byte[] newName, final long redoTxnId)
        throws IOException
    {
        // Design note: Rename is a Database method instead of an Index method because it
        // offers an extra degree of safety. It's too easy to call rename and pass a byte[] by
        // an accident when something like remove was desired instead. Requiring access to the
        // Database instance makes this operation a bit more of a hassle to use, which is
        // desirable. Rename is not expected to be a common operation.

        final Tree tree = accessTree(index);

        final byte[] idKey, trashIdKey;
        final byte[] oldName, oldNameKey;
        final byte[] newNameKey;

        final LocalTransaction txn;

        final Node root = tree.mRoot;
        root.acquireExclusive();
        try {
            if (root.mPage == p_closedTreePage()) {
                throw new ClosedIndexException();
            }

            if (Tree.isInternal(tree.mId)) {
                throw new IllegalStateException("Cannot rename an internal index");
            }

            oldName = tree.mName;

            if (oldName == null) {
                throw new IllegalStateException("Cannot rename a temporary index");
            }

            if (Arrays.equals(oldName, newName)) {
                return;
            }

            idKey = newKey(KEY_TYPE_INDEX_ID, tree.mIdBytes);
            trashIdKey = newKey(KEY_TYPE_TRASH_ID, tree.mIdBytes);
            oldNameKey = newKey(KEY_TYPE_INDEX_NAME, oldName);
            newNameKey = newKey(KEY_TYPE_INDEX_NAME, newName);

            txn = newNoRedoTransaction(redoTxnId);
            try {
                txn.lockExclusive(mRegistryKeyMap.mId, idKey);
                txn.lockExclusive(mRegistryKeyMap.mId, trashIdKey);
                // Lock in a consistent order, avoiding deadlocks.
                if (compareUnsigned(oldNameKey, newNameKey) <= 0) {
                    txn.lockExclusive(mRegistryKeyMap.mId, oldNameKey);
                    txn.lockExclusive(mRegistryKeyMap.mId, newNameKey);
                } else {
                    txn.lockExclusive(mRegistryKeyMap.mId, newNameKey);
                    txn.lockExclusive(mRegistryKeyMap.mId, oldNameKey);
                }
            } catch (Throwable e) {
                txn.reset();
                throw e;
            }
        } finally {
            // Can release now that registry entries are locked. Those locks will prevent
            // concurrent renames of the same index.
            root.releaseExclusive();
        }

        try {
            Cursor c = mRegistryKeyMap.newCursor(txn);
            try {
                c.autoload(false);

                c.find(trashIdKey);
                if (c.value() != null) {
                    throw new IllegalStateException("Index is deleted");
                }

                c.find(newNameKey);
                if (c.value() != null) {
                    throw new IllegalStateException("New name is used by another index");
                }

                c.store(tree.mIdBytes);
            } finally {
                c.reset();
            }

            RedoWriter redo;
            if (redoTxnId == 0 && (redo = txnRedoWriter()) != null) {
                long commitPos;

                mCommitLock.lock();
                try {
                    commitPos = redo.renameIndex
                        (txn.txnId(), tree.mId, newName, mDurabilityMode.alwaysRedo());
                } finally {
                    mCommitLock.unlock();
                }

                if (commitPos != 0) {
                    // Must wait for durability confirmation before performing actions below
                    // which cannot be easily rolled back. No global latches or locks are held
                    // while waiting.
                    redo.txnCommitSync(txn, commitPos);
                }
            }

            mRegistryKeyMap.delete(txn, oldNameKey);
            mRegistryKeyMap.store(txn, idKey, newName);

            mOpenTreesLatch.acquireExclusive();
            try {
                txn.commit();

                tree.mName = newName;
                mOpenTrees.put(newName, mOpenTrees.remove(oldName));
            } finally {
                mOpenTreesLatch.releaseExclusive();
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Throwable e) {
            DatabaseException.rethrowIfRecoverable(e);
            throw closeOnFailure(this, e);
        } finally {
            txn.reset();
        }
    }

    private Tree accessTree(Index index) {
        try {
            Tree tree;
            if ((tree = ((Tree) index)).mDatabase == this) {
                return tree;
            }
        } catch (ClassCastException e) {
            // Cast and catch an exception instead of calling instanceof to cause a
            // NullPointerException to be thrown if index is null.
        }
        throw new IllegalArgumentException("Index belongs to a different database");
    }

    @Override
    public Runnable deleteIndex(Index index) throws IOException {
        // Design note: This is a Database method instead of an Index method because it offers
        // an extra degree of safety. See notes in renameIndex.
        return accessTree(index).drop(false);
    }

    /**
     * Returns a deletion task for a tree which just moved to the trash.
     */
    Runnable replicaDeleteTree(long treeId) throws IOException {
        byte[] treeIdBytes = new byte[8];
        encodeLongBE(treeIdBytes, 0, treeId);

        Tree trashed = openTrashedTree(treeIdBytes, false);

        return new Deletion(trashed, false, null);
    }

    /**
     * Called by Tree.drop with root node latch held exclusively.
     */
    Runnable deleteTree(Tree tree) throws IOException {
        Node root;
        if ((!(tree instanceof TempTree) && !moveToTrash(tree.mId, tree.mIdBytes))
            || (root = tree.close(true, true)) == null)
        {
            // Handle concurrent delete attempt.
            throw new ClosedIndexException();
        }

        Tree trashed = newTreeInstance(tree.mId, tree.mIdBytes, tree.mName, root);

        return new Deletion(trashed, false, null);
    }

    /**
     * @param lastIdBytes null to start with first
     * @return null if none available
     */
    private Tree openNextTrashedTree(byte[] lastIdBytes) throws IOException {
        return openTrashedTree(lastIdBytes, true);
    }

    /**
     * @param idBytes null to start with first
     * @param next true to find tree with next higher id
     * @return null if not found
     */
    private Tree openTrashedTree(byte[] idBytes, boolean next) throws IOException {
        View view = mRegistryKeyMap.viewPrefix(new byte[] {KEY_TYPE_TRASH_ID}, 1);

        if (idBytes == null) {
            // Tag all the entries that should be deleted automatically. Entries created later
            // will have a different prefix, and so they'll be ignored.
            Cursor c = view.newCursor(Transaction.BOGUS);
            try {
                for (c.first(); c.key() != null; c.next()) {
                    byte[] name = c.value();
                    if (name.length != 0) {
                        name[0] |= 0x80;
                        c.store(name);
                    }
                }
            } finally {
                c.reset();
            }
        }

        byte[] treeIdBytes, name, rootIdBytes;

        Cursor c = view.newCursor(Transaction.BOGUS);
        try {
            if (idBytes == null) {
                c.first();
            } else if (next) {
                c.findGt(idBytes);
            } else {
                c.find(idBytes);
            }

            while (true) {
                treeIdBytes = c.key();

                if (treeIdBytes == null) {
                    return null;
                }

                rootIdBytes = mRegistry.load(Transaction.BOGUS, treeIdBytes);

                if (rootIdBytes == null) {
                    // Clear out bogus entry in the trash.
                    c.store(null);
                } else {
                    name = c.value();
                    if (name[0] < 0) {
                        // Found a tagged entry.
                        break;
                    }
                }

                if (next) {
                    c.next();
                } else {
                    return null;
                }
            }
        } finally {
            c.reset();
        }

        long rootId = rootIdBytes.length == 0 ? 0 : decodeLongLE(rootIdBytes, 0);

        if ((name[0] & 0x7f) == 0) {
            name = null;
        } else {
            // Trim off the tag byte.
            byte[] actual = new byte[name.length - 1];
            System.arraycopy(name, 1, actual, 0, actual.length);
            name = actual;
        }

        long treeId = decodeLongBE(treeIdBytes, 0);

        return newTreeInstance(treeId, treeIdBytes, name, loadTreeRoot(treeId, rootId));
    }

    private class Deletion implements Runnable {
        private Tree mTrashed;
        private final boolean mResumed;
        private final EventListener mListener;

        Deletion(Tree trashed, boolean resumed, EventListener listener) {
            mTrashed = trashed;
            mResumed = resumed;
            mListener = listener;
        }

        @Override
        public synchronized void run() {
            while (mTrashed != null) {
                delete();
            }
        }

        private void delete() {
            if (mListener != null) {
                mListener.notify(EventType.DELETION_BEGIN,
                                 "Index deletion " + (mResumed ? "resumed" : "begin") +
                                 ": %1$d, name: %2$s",
                                 mTrashed.getId(), mTrashed.getNameString());
            }

            final byte[] idBytes = mTrashed.mIdBytes;

            try {
                long start = System.nanoTime();

                mTrashed.deleteAll();
                Node root = mTrashed.close(true, false);
                removeFromTrash(mTrashed, root);

                if (mListener != null) {
                    double duration = (System.nanoTime() - start) / 1_000_000_000.0;
                    mListener.notify(EventType.DELETION_COMPLETE,
                                     "Index deletion complete: %1$d, name: %2$s, " +
                                     "duration: %3$1.3f seconds",
                                     mTrashed.getId(), mTrashed.getNameString(), duration);
                }

                mTrashed = null;
            } catch (IOException e) {
                if ((!mClosed || mClosedCause != null) && mListener != null) {
                    mListener.notify
                        (EventType.DELETION_FAILED,
                         "Index deletion failed: %1$d, name: %2$s, exception: %3$s",
                         mTrashed.getId(), mTrashed.getNameString(), rootCause(e));
                }
                closeQuietly(null, mTrashed);
                return;
            }

            if (mResumed) {
                try {
                    mTrashed = openNextTrashedTree(idBytes);
                } catch (IOException e) {
                    if ((!mClosed || mClosedCause != null) && mListener != null) {
                        mListener.notify
                            (EventType.DELETION_FAILED,
                             "Unable to resume deletion: %1$s", rootCause(e));
                    }
                    return;
                }
            }
        }
    }

    @Override
    public Tree newTemporaryIndex() throws IOException {
        mCommitLock.lock();
        try {
            return newTemporaryTree(null);
        } finally {
            mCommitLock.unlock();
        }
    }

    /**
     * Caller must hold commit lock.
     *
     * @param root pass null to create an empty index; pass an unevictable node otherwise
     */
    Tree newTemporaryTree(Node root) throws IOException {
        checkClosed();

        // Cleanup before opening more trees.
        cleanupUnreferencedTrees();

        byte[] rootIdBytes;
        if (root == null) {
            rootIdBytes = EMPTY_BYTES;
        } else {
            rootIdBytes = new byte[8];
            encodeLongLE(rootIdBytes, 0, root.mId);
        }

        long treeId;
        byte[] treeIdBytes = new byte[8];

        try {
            do {
                treeId = nextTreeId(true);
                encodeLongBE(treeIdBytes, 0, treeId);
            } while (!mRegistry.insert(Transaction.BOGUS, treeIdBytes, rootIdBytes));

            byte[] idKey = newKey(KEY_TYPE_INDEX_ID, treeIdBytes);

            // Register temporary index as trash, unreplicated.
            Transaction createTxn = newNoRedoTransaction();
            try {
                byte[] trashIdKey = newKey(KEY_TYPE_TRASH_ID, treeIdBytes);
                if (!mRegistryKeyMap.insert(createTxn, trashIdKey, new byte[1])) {
                    throw new DatabaseException("Unable to register temporary index");
                }
                createTxn.commit();
            } finally {
                createTxn.reset();
            }
        } catch (Throwable e) {
            try {
                mRegistry.delete(Transaction.BOGUS, treeIdBytes);
            } catch (Throwable e2) {
                // Panic.
                throw closeOnFailure(this, e);
            }
            throw e;
        }

        if (root == null) {
            root = loadTreeRoot(treeId, 0);
        }

        Tree tree = new TempTree(this, treeId, treeIdBytes, root);
        TreeRef treeRef = new TreeRef(tree, mOpenTreesRefQueue);

        mOpenTreesLatch.acquireExclusive();
        try {
            mOpenTreesById.insert(treeId).value = treeRef;
        } finally {
            mOpenTreesLatch.releaseExclusive();
        }

        return tree;
    }

    @Override
    public View indexRegistryByName() throws IOException {
        return mRegistryKeyMap.viewPrefix(new byte[] {KEY_TYPE_INDEX_NAME}, 1).viewUnmodifiable();
    }

    @Override
    public View indexRegistryById() throws IOException {
        return mRegistryKeyMap.viewPrefix(new byte[] {KEY_TYPE_INDEX_ID}, 1).viewUnmodifiable();
    }

    @Override
    public Transaction newTransaction() {
        return doNewTransaction(mDurabilityMode);
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return doNewTransaction(durabilityMode == null ? mDurabilityMode : durabilityMode);
    }

    private LocalTransaction doNewTransaction(DurabilityMode durabilityMode) {
        RedoWriter redo = txnRedoWriter();
        return new LocalTransaction
            (this, redo, durabilityMode, LockMode.UPGRADABLE_READ, mDefaultLockTimeoutNanos);
    }

    LocalTransaction newAlwaysRedoTransaction() {
        return doNewTransaction(mDurabilityMode.alwaysRedo());
    }

    /**
     * Convenience method which returns a transaction intended for locking and undo. Caller can
     * make modifications, but they won't go to the redo log.
     */
    LocalTransaction newNoRedoTransaction() {
        RedoWriter redo = txnRedoWriter();
        return new LocalTransaction
            (this, redo, DurabilityMode.NO_REDO, LockMode.UPGRADABLE_READ, -1);
    }

    /**
     * Convenience method which returns a transaction intended for locking and undo. Caller can
     * make modifications, but they won't go to the redo log.
     *
     * @param redoTxnId non-zero if operation is performed by recovery
     */
    LocalTransaction newNoRedoTransaction(long redoTxnId) {
        return redoTxnId == 0 ? newNoRedoTransaction() :
            new LocalTransaction(this, redoTxnId, LockMode.UPGRADABLE_READ, -1);
    }

    /**
     * Returns a RedoWriter suitable for transactions to write into.
     */
    private RedoWriter txnRedoWriter() {
        RedoWriter redo = mRedoWriter;
        if (redo != null) {
            redo = redo.txnRedoWriter();
        }
        return redo;
    }

    /**
     * Called by transaction constructor after hash code has been assigned.
     */
    TransactionContext selectTransactionContext(LocalTransaction txn) {
        return mTxnContexts[(txn.hashCode() & 0x7fffffff) % mTxnContexts.length];
    }

    @Override
    public long preallocate(long bytes) throws IOException {
        if (!mClosed && mPageDb.isDurable()) {
            int pageSize = mPageSize;
            long pageCount = (bytes + pageSize - 1) / pageSize;
            if (pageCount > 0) {
                pageCount = mPageDb.allocatePages(pageCount);
                if (pageCount > 0) {
                    try {
                        checkpoint(true, 0, 0);
                    } catch (Throwable e) {
                        DatabaseException.rethrowIfRecoverable(e);
                        closeQuietly(null, this, e);
                        throw e;
                    }
                }
                return pageCount * pageSize;
            }
        }
        return 0;
    }

    @Override
    public void capacityLimit(long bytes) {
        mPageDb.pageLimit(bytes < 0 ? -1 : (bytes / mPageSize));
    }

    @Override
    public long capacityLimit() {
        long pageLimit = mPageDb.pageLimit();
        return pageLimit < 0 ? -1 : (pageLimit * mPageSize);
    }

    @Override
    public void capacityLimitOverride(long bytes) {
        mPageDb.pageLimitOverride(bytes < 0 ? -1 : (bytes / mPageSize));
    }

    @Override
    public Snapshot beginSnapshot() throws IOException {
        if (!(mPageDb.isDurable())) {
            throw new UnsupportedOperationException("Snapshot only allowed for durable databases");
        }
        checkClosed();
        DurablePageDb pageDb = (DurablePageDb) mPageDb;
        return pageDb.beginSnapshot(this);
    }

    /**
     * Restore from a {@link #beginSnapshot snapshot}, into the data files defined by the given
     * configuration. All existing data and redo log files at the snapshot destination are
     * deleted before the restore begins.
     *
     * @param in snapshot source; does not require extra buffering; auto-closed
     */
    static Database restoreFromSnapshot(DatabaseConfig config, InputStream in) throws IOException {
        config = config.clone();
        PageDb restored;

        File[] dataFiles = config.dataFiles();
        if (dataFiles == null) {
            PageArray dataPageArray = config.mDataPageArray;

            if (dataPageArray == null) {
                throw new UnsupportedOperationException
                    ("Restore only allowed for durable databases");
            }

            dataPageArray = dataPageArray.open();
            dataPageArray.setPageCount(0);

            // Delete old redo log files.
            deleteNumberedFiles(config.mBaseFile, REDO_FILE_SUFFIX);

            restored = DurablePageDb.restoreFromSnapshot(dataPageArray, null, config.mCrypto, in);
        } else {
            if (!config.mReadOnly) {
                for (File f : dataFiles) {
                    // Delete old data file.
                    f.delete();
                    if (config.mMkdirs) {
                        f.getParentFile().mkdirs();
                    }
                }
            }

            FileFactory factory = config.mFileFactory;
            EnumSet<OpenOption> options = config.createOpenOptions();

            // Delete old redo log files.
            deleteNumberedFiles(config.mBaseFile, REDO_FILE_SUFFIX);

            int pageSize = config.mPageSize;
            if (pageSize <= 0) {
                pageSize = DEFAULT_PAGE_SIZE;
            }

            restored = DurablePageDb.restoreFromSnapshot
                (pageSize, dataFiles, factory, options, null, config.mCrypto, in);
        }

        try {
            restored.close();
        } finally {
            restored.delete();
        }

        return Database.open(config);
    }

    @Override
    public void createCachePrimer(OutputStream out) throws IOException {
        if (!(mPageDb.isDurable())) {
            throw new UnsupportedOperationException
                ("Cache priming only allowed for durable databases");
        }

        out = ((DurablePageDb) mPageDb).encrypt(out);

        DataOutputStream dout = new DataOutputStream(out);

        dout.writeLong(PRIMER_MAGIC_NUMBER);

        for (TreeRef treeRef : mOpenTrees.values()) {
            Tree tree = treeRef.get();
            if (tree != null && !Tree.isInternal(tree.mId)) {
                // Encode name instead of identifier, to support priming set portability
                // between databases. The identifiers won't match, but the names might.
                byte[] name = tree.mName;
                dout.writeInt(name.length);
                dout.write(name);
                tree.writeCachePrimer(dout);
            }
        }

        // Terminator.
        dout.writeInt(-1);
    }

    @Override
    public void applyCachePrimer(InputStream in) throws IOException {
        if (!(mPageDb.isDurable())) {
            throw new UnsupportedOperationException
                ("Cache priming only allowed for durable databases");
        }

        in = ((DurablePageDb) mPageDb).decrypt(in);

        DataInput din;
        if (in instanceof DataInput) {
            din = (DataInput) in;
        } else {
            din = new DataInputStream(in);
        }

        long magic = din.readLong();
        if (magic != PRIMER_MAGIC_NUMBER) {
            throw new DatabaseException("Wrong cache primer magic number: " + magic);
        }

        while (true) {
            int len = din.readInt();
            if (len < 0) {
                break;
            }
            byte[] name = new byte[len];
            din.readFully(name);
            Tree tree = openTree(name, false);
            if (tree != null) {
                tree.applyCachePrimer(din);
            } else {
                Tree.skipCachePrimer(din);
            }
        }
    }

    @Override
    public Stats stats() {
        Stats stats = new Stats();

        stats.pageSize = mPageSize;

        mCommitLock.lock();
        try {
            long cursorCount = 0;
            int openTreesCount = 0;
            for (TreeRef treeRef : mOpenTrees.values()) {
                Tree tree = treeRef.get();
                if (tree != null) {
                    openTreesCount++;
                    cursorCount += tree.mRoot.countCursors();
                }
            }
            stats.openIndexes = openTreesCount;
            stats.cursorCount = cursorCount;

            PageDb.Stats pstats = mPageDb.stats();
            stats.freePages = pstats.freePages;
            stats.totalPages = pstats.totalPages;

            stats.lockCount = mLockManager.numLocksHeld();

            for (TransactionContext txnContext : mTxnContexts) {
                txnContext.addStats(stats);
            }
        } finally {
            mCommitLock.unlock();
        }

        for (NodeUsageList usageList : mUsageLists) {
            stats.cachedPages += usageList.size();
        }

        return stats;
    }

    @Override
    public void flush() throws IOException {
        if (!mClosed && mRedoWriter != null) {
            mRedoWriter.flush();
        }
    }

    @Override
    public void sync() throws IOException {
        if (!mClosed && mRedoWriter != null) {
            mRedoWriter.flushSync(false);
        }
    }

    @Override
    public void checkpoint() throws IOException {
        if (!mClosed && mPageDb.isDurable()) {
            try {
                checkpoint(false, 0, 0);
            } catch (Throwable e) {
                DatabaseException.rethrowIfRecoverable(e);
                closeQuietly(null, this, e);
                throw e;
            }
        }
    }

    @Override
    public void suspendCheckpoints() {
        Checkpointer c = mCheckpointer;
        if (c != null) {
            c.suspend();
        }
    }

    @Override
    public void resumeCheckpoints() {
        Checkpointer c = mCheckpointer;
        if (c != null) {
            c.resume();
        }
    }

    @Override
    public boolean compactFile(CompactionObserver observer, double target) throws IOException {
        if (target < 0 || target > 1) {
            throw new IllegalArgumentException("Illegal compaction target: " + target);
        }

        if (target == 0) {
            // No compaction to do at all, but not aborted.
            return true;
        }

        long targetPageCount;
        mCheckpointLock.lock();
        try {
            PageDb.Stats stats = mPageDb.stats();
            long usedPages = stats.totalPages - stats.freePages;
            targetPageCount = Math.max(usedPages, (long) (usedPages / target));

            // Determine the maximum amount of space required to store the reserve list nodes
            // and ensure the target includes them.
            long reserve;
            {
                // Total pages freed.
                long freed = stats.totalPages - targetPageCount;

                // Scale by the maximum size for encoding page identifers, assuming no savings
                // from delta encoding.
                freed *= calcUnsignedVarLongLength(stats.totalPages << 1);

                // Divide by the node size, excluding the header (see PageQueue).
                reserve = freed / (mPageSize - (8 + 8));

                // A minimum is required because the regular and free lists need to allocate
                // one extra node at checkpoint. Up to three checkpoints may be issued, so pad
                // by 2 * 3 = 6.
                reserve += 6;
            }

            targetPageCount += reserve;

            if (targetPageCount >= stats.totalPages && targetPageCount >= mPageDb.pageCount()) {
                return true;
            }

            if (!mPageDb.compactionStart(targetPageCount)) {
                return false;
            }
        } finally {
            mCheckpointLock.unlock();
        }

        boolean completed = mPageDb.compactionScanFreeList();

        if (completed) {
            // Issue a checkpoint to ensure all dirty nodes are flushed out. This ensures that
            // nodes can be moved out of the compaction zone by simply marking them dirty. If
            // already dirty, they'll not be in the compaction zone unless compaction aborted.
            checkpoint();

            if (observer == null) {
                observer = new CompactionObserver();
            }

            final long highestNodeId = targetPageCount - 1;
            final CompactionObserver fobserver = observer;

            completed = scanAllIndexes((tree) -> {
                return tree.compactTree(tree.observableView(), highestNodeId, fobserver);
            });

            checkpoint(true, 0, 0);

            if (completed && mPageDb.compactionScanFreeList()) {
                if (!mPageDb.compactionVerify() && mPageDb.compactionScanFreeList()) {
                    checkpoint(true, 0, 0);
                }
            }
        }

        mCheckpointLock.lock();
        try {
            completed &= mPageDb.compactionEnd();

            // Reclaim reserved pages, but only after a checkpoint has been performed.
            checkpoint(true, 0, 0);
            mPageDb.compactionReclaim();
            // Checkpoint again in order for reclaimed pages to be immediately available.
            checkpoint(true, 0, 0);

            if (completed) {
                // And now, attempt to actually shrink the file.
                return mPageDb.truncatePages();
            }
        } finally {
            mCheckpointLock.unlock();
        }

        return false;
    }

    @Override
    public boolean verify(VerificationObserver observer) throws IOException {
        // TODO: Verify free lists.
        if (false) {
            mPageDb.scanFreeList(id -> System.out.println(id));
        }

        if (observer == null) {
            observer = new VerificationObserver();
        }

        final boolean[] passedRef = {true};
        final VerificationObserver fobserver = observer;

        scanAllIndexes((tree) -> {
            Index view = tree.observableView();
            fobserver.failed = false;
            boolean keepGoing = tree.verifyTree(view, fobserver);
            passedRef[0] &= !fobserver.failed;
            if (keepGoing) {
                keepGoing = fobserver.indexComplete(view, !fobserver.failed, null);
            }
            return keepGoing;
        });

        return passedRef[0];
    }

    @FunctionalInterface
    static interface ScanVisitor {
        /**
         * @return false if should stop
         */
        boolean apply(Tree tree) throws IOException;
    }

    /**
     * @return false if stopped
     */
    private boolean scanAllIndexes(ScanVisitor visitor) throws IOException {
        if (!visitor.apply(mRegistry)) {
            return false;
        }
        if (!visitor.apply(mRegistryKeyMap)) {
            return false;
        }

        FragmentedTrash trash = mFragmentedTrash;
        if (trash != null) {
            if (!visitor.apply(trash.mTrash)) {
                return false;
            }
        }

        Cursor all = indexRegistryByName().newCursor(null);
        try {
            for (all.first(); all.key() != null; all.next()) {
                long id = decodeLongBE(all.value(), 0);

                Index index = indexById(id);
                if (index instanceof Tree && !visitor.apply((Tree) index)) {
                    return false;
                }
            }
        } finally {
            all.reset();
        }

        return true;
    }

    @Override
    public void close(Throwable cause) throws IOException {
        close(cause, false);
    }

    @Override
    public void shutdown() throws IOException {
        close(null, mPageDb.isDurable());
    }

    @Override
    protected void finalize() throws IOException {
        close();
    }

    private void close(Throwable cause, boolean shutdown) throws IOException {
        if (mClosed) {
            return;
        }

        if (cause != null) {
            if (cClosedCauseUpdater.compareAndSet(this, null, cause)) {
                Throwable rootCause = rootCause(cause);
                if (mEventListener == null) {
                    uncaught(rootCause);
                } else {
                    mEventListener.notify(EventType.PANIC_UNHANDLED_EXCEPTION,
                                          "Closing database due to unhandled exception: %1$s",
                                          rootCause);
                }
            }
        }

        Thread ct = null;
        boolean lockedCheckpointer = false;

        try {
            Checkpointer c = mCheckpointer;

            if (shutdown) {
                mCheckpointLock.lock();
                lockedCheckpointer = true;

                if (!mClosed) {
                    checkpoint(true, 0, 0);
                    if (c != null) {
                        ct = c.close();
                    }
                }
            } else {
                if (c != null) {
                    ct = c.close();
                }

                // Wait for any in-progress checkpoint to complete.

                if (mCheckpointLock.tryLock()) {
                    lockedCheckpointer = true;
                } else if (cause == null && !(mRedoWriter instanceof ReplRedoController)) {
                    // Only attempt lock if not panicked and not replicated. If panicked, other
                    // locks might be held and so acquiring checkpoint lock might deadlock.
                    // Replicated databases might stall indefinitely when checkpointing.
                    // Checkpointer should eventually exit after other resources are closed.
                    mCheckpointLock.lock();
                    lockedCheckpointer = true;
                }
            }

            mClosed = true;
        } finally {
            if (ct != null) {
                ct.interrupt();
            }

            if (lockedCheckpointer) {
                mCheckpointLock.unlock();

                if (ct != null) {
                    // Wait for checkpointer thread to finish.
                    try {
                        ct.join();
                    } catch (InterruptedException e) {
                        // Ignore.
                    }
                }
            }
        }

        try {
            mCheckpointer = null;

            CommitLock lock = mCommitLock;

            if (mOpenTrees != null) {
                // Clear out open trees with commit lock held, to prevent any trees from being
                // opened again. Any attempt to open a tree must acquire the commit lock and
                // then check if the database is closed.
                final ArrayList<TreeRef> trees;
                if (lock != null) {
                    lock.acquireExclusive();
                }
                try {
                    mOpenTreesLatch.acquireExclusive();
                    try {
                        trees = new ArrayList<>(mOpenTreesById.size());

                        mOpenTreesById.traverse((entry) -> {
                            trees.add(entry.value);
                            return true;
                        });

                        mOpenTrees.clear();
                    } finally {
                        mOpenTreesLatch.releaseExclusive();
                    }
                } finally {
                    if (lock != null) {
                        lock.releaseExclusive();
                    }
                }

                for (TreeRef ref : trees) {
                    Tree tree = ref.get();
                    if (tree != null) {
                        tree.close();
                    }
                }
            }

            if (lock != null) {
                lock.acquireExclusive();
            }
            try {
                if (mUsageLists != null) {
                    for (NodeUsageList usageList : mUsageLists) {
                        if (usageList != null) {
                            usageList.delete();
                        }
                    }
                }

                if (mDirtyList != null) {
                    mDirtyList.delete(this);
                }

                if (mTxnContexts != null) {
                    for (TransactionContext txnContext : mTxnContexts) {
                        if (txnContext != null) {
                            txnContext.deleteUndoLogs();
                        }
                    }
                }

                nodeMapDeleteAll();

                IOException ex = null;

                ex = closeQuietly(ex, mRedoWriter, cause);
                ex = closeQuietly(ex, mPageDb, cause);
                ex = closeQuietly(ex, mTempFileManager, cause);

                if (shutdown && mBaseFile != null) {
                    deleteRedoLogFiles();
                    new File(mBaseFile.getPath() + INFO_FILE_SUFFIX).delete();
                    ex = closeQuietly(ex, mLockFile, cause);
                    new File(mBaseFile.getPath() + LOCK_FILE_SUFFIX).delete();
                } else {
                    ex = closeQuietly(ex, mLockFile, cause);
                }

                if (mLockManager != null) {
                    mLockManager.close();
                }

                if (ex != null) {
                    throw ex;
                }
            } finally {
                if (lock != null) {
                    lock.releaseExclusive();
                }
            }
        } finally {
            if (mPageDb != null) {
                mPageDb.delete();
            }
            if (mSparePagePool != null) {
                mSparePagePool.delete();
            }
            p_delete(mCommitHeader);
            p_arenaDelete(mArena);
        }
    }

    void checkClosed() throws DatabaseException {
        if (mClosed) {
            String message = "Closed";
            Throwable cause = mClosedCause;
            if (cause != null) {
                message += "; " + rootCause(cause);
            }
            throw new DatabaseException(message, cause);
        }
    }

    void treeClosed(Tree tree) {
        mOpenTreesLatch.acquireExclusive();
        try {
            TreeRef ref = mOpenTreesById.getValue(tree.mId);
            if (ref != null && ref.get() == tree) {
                ref.clear();
                if (tree.mName != null) {
                    mOpenTrees.remove(tree.mName);
                }
                mOpenTreesById.remove(tree.mId);
            }
        } finally {
            mOpenTreesLatch.releaseExclusive();
        }
    }

    /**
     * @return false if already in the trash
     */
    private boolean moveToTrash(long treeId, byte[] treeIdBytes) throws IOException {
        final byte[] idKey = newKey(KEY_TYPE_INDEX_ID, treeIdBytes);
        final byte[] trashIdKey = newKey(KEY_TYPE_TRASH_ID, treeIdBytes);

        final LocalTransaction txn = newAlwaysRedoTransaction();

        try {
            if (mRegistryKeyMap.load(txn, trashIdKey) != null) {
                // Already in the trash.
                return false;
            }

            byte[] treeName = mRegistryKeyMap.exchange(txn, idKey, null);

            if (treeName == null) {
                // A trash entry with just a zero indicates that the name is null.
                mRegistryKeyMap.store(txn, trashIdKey, new byte[1]);
            } else {
                byte[] nameKey = newKey(KEY_TYPE_INDEX_NAME, treeName);
                mRegistryKeyMap.remove(txn, nameKey, treeIdBytes);
                // Tag the trash entry to indicate that name is non-null. Note that nameKey
                // instance is modified directly.
                nameKey[0] = 1;
                mRegistryKeyMap.store(txn, trashIdKey, nameKey);
            }

            RedoWriter redo = txnRedoWriter();
            if (redo != null) {
                long commitPos;

                // Note: No additional operations can appear after OP_DELETE_INDEX. When a
                // replica reads this operation it immediately commits the transaction in order
                // for the deletion task to be started immediately. The redo log still contains
                // a commit operation, which is redundant and harmless.

                mCommitLock.lock();
                try {
                    commitPos = redo.deleteIndex
                        (txn.txnId(), treeId, mDurabilityMode.alwaysRedo());
                } finally {
                    mCommitLock.unlock();
                }

                if (commitPos != 0) {
                    // Must wait for durability confirmation before performing actions below
                    // which cannot be easily rolled back. No global latches or locks are held
                    // while waiting.
                    redo.txnCommitSync(txn, commitPos);
                }
            }

            txn.commit();
        } catch (Throwable e) {
            DatabaseException.rethrowIfRecoverable(e);
            throw closeOnFailure(this, e);
        } finally {
            txn.reset();
        }

        return true;
    }

    /**
     * Must be called after all entries in the tree have been deleted and tree is closed.
     */
    void removeFromTrash(Tree tree, Node root) throws IOException {
        byte[] trashIdKey = newKey(KEY_TYPE_TRASH_ID, tree.mIdBytes);

        mCommitLock.lock();
        try {
            if (root != null) {
                root.acquireExclusive();
                deleteNode(root);
            }
            mRegistryKeyMap.delete(Transaction.BOGUS, trashIdKey);
            mRegistry.delete(Transaction.BOGUS, tree.mIdBytes);
        } catch (Throwable e) {
            throw closeOnFailure(this, e);
        } finally {
            mCommitLock.unlock();
        }
    }

    /**
     * @param treeId pass zero if unknown or not applicable
     * @param rootId pass zero to create
     * @return unlatched and unevictable root node
     */
    private Node loadTreeRoot(final long treeId, final long rootId) throws IOException {
        if (rootId == 0) {
            // Pass tree identifer to spread allocations around.
            Node rootNode = allocLatchedNode(treeId, NodeUsageList.MODE_UNEVICTABLE);

            try {
                /*P*/ // [
                rootNode.asEmptyRoot();
                /*P*/ // |
                /*P*/ // if (mFullyMapped) {
                /*P*/ //     rootNode.mPage = p_nonTreePage(); // always an empty leaf node
                /*P*/ //     rootNode.mId = 0;
                /*P*/ //     rootNode.mCachedState = CACHED_CLEAN;
                /*P*/ // } else {
                /*P*/ //     rootNode.asEmptyRoot();
                /*P*/ // }
                /*P*/ // ]
                return rootNode;
            } finally {
                rootNode.releaseExclusive();
            }
        } else {
            // Check if root node is still around after tree was closed.
            Node rootNode = nodeMapGet(rootId);

            if (rootNode != null) {
                rootNode.acquireShared();
                try {
                    if (rootId == rootNode.mId) {
                        rootNode.makeUnevictable();
                        return rootNode;
                    }
                } finally {
                    rootNode.releaseShared();
                }
            }

            rootNode = allocLatchedNode(rootId, NodeUsageList.MODE_UNEVICTABLE);

            try {
                try {
                    rootNode.read(this, rootId);
                } finally {
                    rootNode.releaseExclusive();
                }
                nodeMapPut(rootNode);
                return rootNode;
            } catch (Throwable e) {
                rootNode.makeEvictableNow();
                throw e;
            }
        }
    }

    /**
     * Loads the root registry node, or creates one if store is new. Root node
     * is not eligible for eviction.
     */
    private Node loadRegistryRoot(byte[] header, ReplicationManager rm) throws IOException {
        int version = decodeIntLE(header, I_ENCODING_VERSION);

        long rootId;
        if (version == 0) {
            rootId = 0;
            // No registry; clearly nothing has been checkpointed.
            mInitialReadState = CACHED_DIRTY_0;
        } else {
            if (version != ENCODING_VERSION) {
                throw new CorruptDatabaseException("Unknown encoding version: " + version);
            }

            long replEncoding = decodeLongLE(header, I_REPL_ENCODING);

            if (rm == null) {
                if (replEncoding != 0) {
                    throw new DatabaseException
                        ("Database must be configured with a replication manager, " +
                         "identified by: " + replEncoding);
                }
            } else {
                if (replEncoding == 0) {
                    throw new DatabaseException
                        ("Database was created initially without a replication manager");
                }
                long expectedReplEncoding = rm.encoding();
                if (replEncoding != expectedReplEncoding) {
                    throw new DatabaseException
                        ("Database was created initially with a different replication manager, " +
                         "identified by: " + replEncoding);
                }
            }

            rootId = decodeLongLE(header, I_ROOT_PAGE_ID);
        }

        return loadTreeRoot(0, rootId);
    }

    private Tree openInternalTree(long treeId, boolean create) throws IOException {
        return openInternalTree(treeId, create, null);
    }

    private Tree openInternalTree(long treeId, boolean create, DatabaseConfig config)
        throws IOException
    {
        mCommitLock.lock();
        try {
            byte[] treeIdBytes = new byte[8];
            encodeLongBE(treeIdBytes, 0, treeId);
            byte[] rootIdBytes = mRegistry.load(Transaction.BOGUS, treeIdBytes);
            long rootId;
            if (rootIdBytes != null) {
                rootId = decodeLongLE(rootIdBytes, 0);
            } else {
                if (!create) {
                    return null;
                }
                rootId = 0;
            }

            Node root = loadTreeRoot(treeId, rootId);

            // Cannot call newTreeInstance because mRedoWriter isn't set yet.
            if (config != null && config.mReplManager != null) {
                return new TxnTree(this, treeId, treeIdBytes, root);
            }

            return newTreeInstance(treeId, treeIdBytes, null, root);
        } finally {
            mCommitLock.unlock();
        }
    }

    /**
     * @param name required (cannot be null)
     */
    private Tree openTree(byte[] name, boolean create) throws IOException {
        return openTree(null, name, create);
    }

    /**
     * @param name required (cannot be null)
     */
    private Tree openTree(Transaction findTxn, byte[] name, boolean create) throws IOException {
        Tree tree = quickFindIndex(name);
        if (tree == null) {
            mCommitLock.lock();
            try {
                tree = doOpenTree(findTxn, name, create);
            } finally {
                mCommitLock.unlock();
            }
        }
        return tree;
    }

    /**
     * Caller must hold commit lock.
     *
     * @param name required (cannot be null)
     */
    private Tree doOpenTree(Transaction findTxn, byte[] name, boolean create) throws IOException {
        checkClosed();

        // Cleanup before opening more trees.
        cleanupUnreferencedTrees();

        byte[] nameKey = newKey(KEY_TYPE_INDEX_NAME, name);
        byte[] treeIdBytes = mRegistryKeyMap.load(findTxn, nameKey);

        long treeId;
        // Is non-null if tree was created.
        byte[] idKey;

        if (treeIdBytes != null) {
            // Tree already exists.
            idKey = null;
            treeId = decodeLongBE(treeIdBytes, 0);
        } else if (!create) {
            return null;
        } else create: {
            // Transactional find supported only for opens that do not create.
            if (findTxn != null) {
                throw new AssertionError();
            }

            Transaction createTxn = null;

            mOpenTreesLatch.acquireExclusive();
            try {
                treeIdBytes = mRegistryKeyMap.load(null, nameKey);
                if (treeIdBytes != null) {
                    // Another thread created it.
                    idKey = null;
                    treeId = decodeLongBE(treeIdBytes, 0);
                    break create;
                }

                treeIdBytes = new byte[8];

                // Non-transactional operations are critical, in that any failure is treated as
                // non-recoverable.
                boolean critical = true;
                try {
                    do {
                        critical = false;
                        treeId = nextTreeId(false);
                        encodeLongBE(treeIdBytes, 0, treeId);
                        critical = true;
                    } while (!mRegistry.insert(Transaction.BOGUS, treeIdBytes, EMPTY_BYTES));

                    critical = false;

                    try {
                        idKey = newKey(KEY_TYPE_INDEX_ID, treeIdBytes);

                        if (mRedoWriter instanceof ReplRedoController) {
                            // Confirmation is required when replicated.
                            createTxn = newTransaction(DurabilityMode.SYNC);
                        } else {
                            createTxn = newAlwaysRedoTransaction();
                        }

                        if (!mRegistryKeyMap.insert(createTxn, idKey, name)) {
                            throw new DatabaseException("Unable to insert index id");
                        }
                        if (!mRegistryKeyMap.insert(createTxn, nameKey, treeIdBytes)) {
                            throw new DatabaseException("Unable to insert index name");
                        }
                    } catch (Throwable e) {
                        critical = true;
                        try {
                            if (createTxn != null) {
                                createTxn.reset();
                            }
                            mRegistry.delete(Transaction.BOGUS, treeIdBytes);
                            critical = false;
                        } catch (Throwable e2) {
                            e.addSuppressed(e2);
                        }
                        throw e;
                    }
                } catch (Throwable e) {
                    if (!critical) {
                        DatabaseException.rethrowIfRecoverable(e);
                    }
                    throw closeOnFailure(this, e);
                }
            } finally {
                // Release to allow opening other indexes while blocked on commit.
                mOpenTreesLatch.releaseExclusive();
            }

            if (createTxn != null) {
                try {
                    createTxn.commit();
                } catch (Throwable e) {
                    try {
                        createTxn.reset();
                        mRegistry.delete(Transaction.BOGUS, treeIdBytes);
                    } catch (Throwable e2) {
                        e.addSuppressed(e2);
                        throw closeOnFailure(this, e);
                    }
                    DatabaseException.rethrowIfRecoverable(e);
                    throw closeOnFailure(this, e);
                }
            }
        }

        // Use a transaction to ensure that only one thread loads the requested tree. Nothing
        // is written into it.
        Transaction txn = newNoRedoTransaction();
        try {
            // Pass the transaction to acquire the lock.
            byte[] rootIdBytes = mRegistry.load(txn, treeIdBytes);

            Tree tree = quickFindIndex(name);
            if (tree != null) {
                // Another thread got the lock first and loaded the tree.
                return tree;
            }

            long rootId = (rootIdBytes == null || rootIdBytes.length == 0) ? 0
                : decodeLongLE(rootIdBytes, 0);

            Node root = loadTreeRoot(treeId, rootId);

            tree = newTreeInstance(treeId, treeIdBytes, name, root);
            TreeRef treeRef = new TreeRef(tree, mOpenTreesRefQueue);

            mOpenTreesLatch.acquireExclusive();
            try {
                mOpenTrees.put(name, treeRef);
                mOpenTreesById.insert(treeId).value = treeRef;
            } finally {
                mOpenTreesLatch.releaseExclusive();
            }

            return tree;
        } catch (Throwable e) {
            if (idKey != null) {
                // Rollback create of new tree.
                try {
                    mRegistryKeyMap.delete(null, idKey);
                    mRegistryKeyMap.delete(null, nameKey);
                    mRegistry.delete(Transaction.BOGUS, treeIdBytes);
                } catch (Throwable e2) {
                    // Ignore.
                }
            }
            throw e;
        } finally {
            txn.reset();
        }
    }

    private Tree newTreeInstance(long id, byte[] idBytes, byte[] name, Node root) {
        Tree tree;
        if (mRedoWriter instanceof ReplRedoWriter) {
            // Always need an explcit transaction when using auto-commit, to ensure that
            // rollback is possible.
            tree = new TxnTree(this, id, idBytes, root);
        } else {
            tree = new Tree(this, id, idBytes, root);
        }
        tree.mName = name;
        return tree;
    }

    private long nextTreeId(boolean temporary) throws IOException {
        // By generating identifiers from a 64-bit sequence, it's effectively
        // impossible for them to get re-used after trees are deleted.

        Transaction txn;
        if (temporary) {
            txn = newNoRedoTransaction();
        } else {
            txn = newAlwaysRedoTransaction();
        }

        try {
            // Tree id mask, to make the identifiers less predictable and
            // non-compatible with other database instances.
            long treeIdMask;
            {
                byte[] key = {KEY_TYPE_TREE_ID_MASK};
                byte[] treeIdMaskBytes = mRegistryKeyMap.load(txn, key);

                if (treeIdMaskBytes == null) {
                    treeIdMaskBytes = new byte[8];
                    new Random().nextBytes(treeIdMaskBytes);
                    mRegistryKeyMap.store(txn, key, treeIdMaskBytes);
                }

                treeIdMask = decodeLongLE(treeIdMaskBytes, 0);
            }

            byte[] key = {KEY_TYPE_NEXT_TREE_ID};
            byte[] nextTreeIdBytes = mRegistryKeyMap.load(txn, key);

            if (nextTreeIdBytes == null) {
                nextTreeIdBytes = new byte[8];
            }
            long nextTreeId = decodeLongLE(nextTreeIdBytes, 0);

            if (temporary) {
                // Apply negative sequence, avoiding collisions.
                treeIdMask = ~treeIdMask;
            }

            long treeId;
            do {
                treeId = scramble((nextTreeId++) ^ treeIdMask);
            } while (Tree.isInternal(treeId));

            encodeLongLE(nextTreeIdBytes, 0, nextTreeId);
            mRegistryKeyMap.store(txn, key, nextTreeIdBytes);
            txn.commit();

            return treeId;
        } finally {
            txn.reset();
        }
    }

    /**
     * @param name required (cannot be null)
     * @return null if not found
     */
    private Tree quickFindIndex(byte[] name) throws IOException {
        TreeRef treeRef;
        mOpenTreesLatch.acquireShared();
        try {
            treeRef = mOpenTrees.get(name);
            if (treeRef == null) {
                return null;
            }
            Tree tree = treeRef.get();
            if (tree != null) {
                return tree;
            }
        } finally {
            mOpenTreesLatch.releaseShared();
        }

        // Ensure that root node of cleared tree reference is available in the node map before
        // potentially replacing it. Weak references are cleared before they are enqueued, and
        // so polling the queue does not guarantee node eviction. Process the tree directly.
        cleanupUnreferencedTree(treeRef);

        return null;
    }

    /**
     * Tree instances retain a reference to an unevictable root node. If tree is no longer in
     * use, allow it to be evicted. Method cannot be called while a checkpoint is in progress.
     */
    private void cleanupUnreferencedTrees() throws IOException {
        final ReferenceQueue<Tree> queue = mOpenTreesRefQueue;
        if (queue == null) {
            return;
        }
        try {
            while (true) {
                Reference<? extends Tree> ref = queue.poll();
                if (ref == null) {
                    break;
                }
                if (ref instanceof TreeRef) {
                    cleanupUnreferencedTree((TreeRef) ref);
                }
            }
        } catch (Exception e) {
            if (!mClosed) {
                throw e;
            }
        }
    }

    private void cleanupUnreferencedTree(TreeRef ref) throws IOException {
        Node root = ref.mRoot;
        root.acquireShared();
        try {
            mOpenTreesLatch.acquireExclusive();
            try {
                LHashTable.ObjEntry<TreeRef> entry = mOpenTreesById.get(ref.mId);
                if (entry == null || entry.value != ref) {
                    return;
                }
                if (ref.mName != null) {
                    mOpenTrees.remove(ref.mName);
                }
                mOpenTreesById.remove(ref.mId);
                root.makeEvictableNow();
                if (root.mId != 0) {
                    nodeMapPut(root);
                }
            } finally {
                mOpenTreesLatch.releaseExclusive();
            }
        } finally {
            root.releaseShared();
        }
    }

    private static byte[] newKey(byte type, byte[] payload) {
        byte[] key = new byte[1 + payload.length];
        key[0] = type;
        arraycopy(payload, 0, key, 1, payload.length);
        return key;
    }

    /**
     * Returns the fixed size of all pages in the store, in bytes.
     */
    int pageSize() {
        return mPageSize;
    }

    private int pageSize(/*P*/ byte[] page) {
        /*P*/ // [
        return page.length;
        /*P*/ // |
        /*P*/ // return mPageSize;
        /*P*/ // ]
    }

    /**
     * Access the commit lock, which prevents commits while held shared. In general, it should
     * be acquired before any node latches, but postponing acquisition reduces the total time
     * held. Checkpoints don't have to wait as long for the exclusive commit lock. Because node
     * latching first isn't the canonical ordering, acquiring the shared commit lock later must
     * be prepared to abort. Try to acquire first, and if it fails, release the node latch and
     * do over.
     */
    CommitLock commitLock() {
        return mCommitLock;
    }

    /**
     * Returns unconfirmed node if found. Caller must latch and confirm that node identifier
     * matches, in case an eviction snuck in.
     */
    Node nodeMapGet(final long nodeId) {
        return nodeMapGet(nodeId, Long.hashCode(nodeId));
    }

    /**
     * Returns unconfirmed node if found. Caller must latch and confirm that node identifier
     * matches, in case an eviction snuck in.
     */
    Node nodeMapGet(final long nodeId, final int hash) {
        // Quick check without acquiring a partition latch.

        final Node[] table = mNodeMapTable;
        Node node = table[hash & (table.length - 1)];
        if (node != null) {
            // Limit scan of collision chain in case a temporary infinite loop is observed.
            int limit = 100;
            do {
                if (node.mId == nodeId) {
                    return node;
                }
            } while ((node = node.mNodeMapNext) != null && --limit != 0);
        }

        // Again with shared partition latch held.

        final Latch[] latches = mNodeMapLatches;
        final Latch latch = latches[hash & (latches.length - 1)];
        latch.acquireShared();

        node = table[hash & (table.length - 1)];
        while (node != null) {
            if (node.mId == nodeId) {
                latch.releaseShared();
                return node;
            }
            node = node.mNodeMapNext;
        }

        latch.releaseShared();
        return null;
    }

    /**
     * Put a node into the map, but caller must confirm that node is not already present.
     */
    void nodeMapPut(final Node node) {
        nodeMapPut(node, Long.hashCode(node.mId));
    }

    /**
     * Put a node into the map, but caller must confirm that node is not already present.
     */
    void nodeMapPut(final Node node, final int hash) {
        final Latch[] latches = mNodeMapLatches;
        final Latch latch = latches[hash & (latches.length - 1)];
        latch.acquireExclusive();

        final Node[] table = mNodeMapTable;
        final int index = hash & (table.length - 1);
        Node e = table[index];
        while (e != null) {
            if (e == node) {
                latch.releaseExclusive();
                return;
            }
            if (e.mId == node.mId) {
                latch.releaseExclusive();
                throw new AssertionError("Already in NodeMap: " + node + ", " + e + ", " + hash);
            }
            e = e.mNodeMapNext;
        }

        node.mNodeMapNext = table[index];
        table[index] = node;

        latch.releaseExclusive();
    }

    /**
     * Returns unconfirmed node if an existing node is found. Caller must latch and confirm
     * that node identifier matches, in case an eviction snuck in.
     *
     * @return null if node was inserted, existing node otherwise
     */
    Node nodeMapPutIfAbsent(final Node node) {
        final int hash = Long.hashCode(node.mId);
        final Latch[] latches = mNodeMapLatches;
        final Latch latch = latches[hash & (latches.length - 1)];
        latch.acquireExclusive();

        final Node[] table = mNodeMapTable;
        final int index = hash & (table.length - 1);
        Node e = table[index];
        while (e != null) {
            if (e.mId == node.mId) {
                latch.releaseExclusive();
                return e;
            }
            e = e.mNodeMapNext;
        }

        node.mNodeMapNext = table[index];
        table[index] = node;

        latch.releaseExclusive();
        return null;
    }

    /**
     * Replace a node which must be in the map already. Old and new node MUST have the same id.
     */
    void nodeMapReplace(final Node oldNode, final Node newNode) {
        final int hash = Long.hashCode(oldNode.mId);
        final Latch[] latches = mNodeMapLatches;
        final Latch latch = latches[hash & (latches.length - 1)];
        latch.acquireExclusive();

        newNode.mNodeMapNext = oldNode.mNodeMapNext;

        final Node[] table = mNodeMapTable;
        final int index = hash & (table.length - 1);
        Node e = table[index];
        if (e == oldNode) {
            table[index] = newNode;
        } else while (e != null) {
            Node next = e.mNodeMapNext;
            if (next == oldNode) {
                e.mNodeMapNext = newNode;
                break;
            }
            e = next;
        }

        oldNode.mNodeMapNext = null;

        latch.releaseExclusive();
    }

    void nodeMapRemove(final Node node) {
        nodeMapRemove(node, Long.hashCode(node.mId));
    }

    void nodeMapRemove(final Node node, final int hash) {
        final Latch[] latches = mNodeMapLatches;
        final Latch latch = latches[hash & (latches.length - 1)];
        latch.acquireExclusive();

        final Node[] table = mNodeMapTable;
        final int index = hash & (table.length - 1);
        Node e = table[index];
        if (e == node) {
            table[index] = e.mNodeMapNext;
        } else while (e != null) {
            Node next = e.mNodeMapNext;
            if (next == node) {
                e.mNodeMapNext = next.mNodeMapNext;
                break;
            }
            e = next;
        }

        node.mNodeMapNext = null;

        latch.releaseExclusive();
    }

    /**
     * Returns or loads the fragment node with the given id. If loaded, node is put in the cache.
     *
     * @return node with shared latch held
     */
    Node nodeMapLoadFragment(long nodeId) throws IOException {
        Node node = nodeMapGet(nodeId);

        if (node != null) {
            node.acquireShared();
            if (nodeId == node.mId) {
                node.used();
                return node;
            }
            node.releaseShared();
        }

        node = allocLatchedNode(nodeId);
        node.mId = nodeId;

        // node is currently exclusively locked. Insert it into the node map so that no other
        // thread tries to read it at the same time. If another thread sees it at this point
        // (before it is actually read), until the node is read, that thread will block trying
        // to get a shared lock.
        while (true) {
            Node existing = nodeMapPutIfAbsent(node);
            if (existing == null) {
                break;
            }

            // Was already loaded, or is currently being loaded.
            existing.acquireShared();
            if (nodeId == existing.mId) {
                // The item is already loaded. Throw away the node this thread was trying to
                // allocate.
                //
                // Even though node is not currently in the node map, it could have been in
                // there then got recycled. Other thread may still have a reference to it from
                // when it was in the node map. So its id needs to be invalidated.
                node.mId = 0;
                // This releases the exclusive latch and makes the node immediately usable for
                // new allocations.
                node.unused();
                return existing;
            }
            existing.releaseShared();
        }

        try {
            /*P*/ // [
            node.type(TYPE_FRAGMENT);
            /*P*/ // ]
            readNode(node, nodeId);
        } catch (Throwable t) {
            // Something went wrong reading the node. Remove the node from the map, now that
            // it definitely won't get read.
            nodeMapRemove(node);
            node.mId = 0;
            node.releaseExclusive();
            throw t;
        }
        node.downgrade();

        return node;
    }

    /**
     * Returns or loads the fragment node with the given id. If loaded, node is put in the
     * cache. Method is intended for obtaining nodes to write into.
     *
     * @param read true if node should be fully read if it needed to be loaded
     * @return node with exclusive latch held
     */
    Node nodeMapLoadFragmentExclusive(long nodeId, boolean read) throws IOException {
        // Very similar to the nodeMapLoadFragment method. It has comments which explains
        // what's going on here. No point in duplicating that as well.

        Node node = nodeMapGet(nodeId);

        if (node != null) {
            node.acquireExclusive();
            if (nodeId == node.mId) {
                node.used();
                return node;
            }
            node.releaseExclusive();
        }

        node = allocLatchedNode(nodeId);
        node.mId = nodeId;

        while (true) {
            Node existing = nodeMapPutIfAbsent(node);
            if (existing == null) {
                break;
            }
            existing.acquireExclusive();
            if (nodeId == existing.mId) {
                node.mId = 0;
                node.unused();
                return existing;
            }
            existing.releaseExclusive();
        }

        try {
            /*P*/ // [
            node.type(TYPE_FRAGMENT);
            /*P*/ // ]
            if (read) {
                readNode(node, nodeId);
            }
        } catch (Throwable t) {
            nodeMapRemove(node);
            node.mId = 0;
            node.releaseExclusive();
            throw t;
        }

        return node;
    }

    /**
     * @return exclusively latched node if found; null if not found
     */
    Node nodeMapGetAndRemove(long nodeId) {
        int hash = Long.hashCode(nodeId);
        Node node = nodeMapGet(nodeId, hash);
        if (node != null) {
            node.acquireExclusive();
            if (nodeId != node.mId) {
                node.releaseExclusive();
                node = null;
            } else {
                nodeMapRemove(node, hash);
            }
        }
        return node;
    }

    /**
     * Remove and delete nodes from map, as part of close sequence.
     */
    void nodeMapDeleteAll() {
        start: while (true) {
            for (Latch latch : mNodeMapLatches) {
                latch.acquireExclusive();
            }

            try {
                for (int i=mNodeMapTable.length; --i>=0; ) {
                    Node e = mNodeMapTable[i];
                    if (e != null) {
                        if (!e.tryAcquireExclusive()) {
                            // Deadlock prevention.
                            continue start;
                        }
                        try {
                            e.doDelete(this);
                        } finally {
                            e.releaseExclusive();
                        }
                        Node next;
                        while ((next = e.mNodeMapNext) != null) {
                            e.mNodeMapNext = null;
                            e = next;
                        }
                        mNodeMapTable[i] = null;
                    }
                }
            } finally {
                for (Latch latch : mNodeMapLatches) {
                    latch.releaseExclusive();
                }
            }

            return;
        }
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, with an undefined id and a
     * clean state.
     *
     * @param anyNodeId id of any node, for spreading allocations around
     */
    Node allocLatchedNode(long anyNodeId) throws IOException {
        return allocLatchedNode(anyNodeId, 0);
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, with an undefined id and a
     * clean state.
     *
     * @param anyNodeId id of any node, for spreading allocations around
     * @param mode MODE_UNEVICTABLE if allocated node cannot be automatically evicted
     */
    Node allocLatchedNode(long anyNodeId, int mode) throws IOException {
        mode |= mPageDb.allocMode();

        NodeUsageList[] usageLists = mUsageLists;
        int listIx = ((int) anyNodeId) & (usageLists.length - 1);
        IOException fail = null;

        for (int trial = 1; trial <= 3; trial++) {
            for (int i=0; i<usageLists.length; i++) {
                try {
                    Node node = usageLists[listIx].tryAllocLatchedNode(trial, mode);
                    if (node != null) {
                        return node;
                    }
                } catch (IOException e) {
                    if (fail == null) {
                        fail = e;
                    }
                }
                if (--listIx < 0) {
                    listIx = usageLists.length - 1;
                }
            }

            checkClosed();

            mCommitLock.lock();
            try {
                // Try to free up nodes from unreferenced trees.
                cleanupUnreferencedTrees();
            } finally {
                mCommitLock.unlock();
            }
        }

        if (fail == null && mPageDb.isDurable()) {
            throw new CacheExhaustedException();
        } else if (fail instanceof DatabaseFullException) {
            throw fail;
        } else {
            throw new DatabaseFullException(fail);
        }
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively and marked
     * dirty. Caller must hold commit lock.
     */
    Node allocDirtyNode() throws IOException {
        return allocDirtyNode(0);
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, marked
     * dirty and unevictable. Caller must hold commit lock.
     *
     * @param mode MODE_UNEVICTABLE if allocated node cannot be automatically evicted
     */
    Node allocDirtyNode(int mode) throws IOException {
        Node node = mPageDb.allocLatchedNode(this, mode);

        /*P*/ // [|
        /*P*/ // if (mFullyMapped) {
        /*P*/ //     node.mPage = mPageDb.dirtyPage(node.mId);
        /*P*/ // }
        /*P*/ // ]

        mDirtyList.add(node, mCommitState);
        return node;
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively and marked
     * dirty. Caller must hold commit lock.
     */
    Node allocDirtyFragmentNode() throws IOException {
        Node node = allocDirtyNode();
        nodeMapPut(node);
        /*P*/ // [
        node.type(TYPE_FRAGMENT);
        /*P*/ // ]
        return node;
    }

    /**
     * Caller must hold commit lock and any latch on node.
     */
    boolean shouldMarkDirty(Node node) {
        return node.mCachedState != mCommitState && node.mId >= 0;
    }

    /**
     * Mark a tree node as dirty. Caller must hold commit lock and exclusive latch on
     * node. Method does nothing if node is already dirty. Latch is never released by this
     * method, even if an exception is thrown.
     *
     * @return true if just made dirty and id changed
     */
    boolean markDirty(Tree tree, Node node) throws IOException {
        if (node.mCachedState == mCommitState || node.mId < 0) {
            return false;
        } else {
            doMarkDirty(tree, node);
            return true;
        }
    }

    /**
     * Mark a fragment node as dirty. Caller must hold commit lock and exclusive latch on
     * node. Method does nothing if node is already dirty. Latch is never released by this
     * method, even if an exception is thrown.
     *
     * @return true if just made dirty and id changed
     */
    boolean markFragmentDirty(Node node) throws IOException {
        if (node.mCachedState == mCommitState) {
            return false;
        } else {
            if (node.mCachedState != CACHED_CLEAN) {
                node.write(mPageDb);
            }

            long newId = mPageDb.allocPage();
            long oldId = node.mId;

            if (oldId != 0) {
                try {
                    mPageDb.deletePage(oldId);
                } catch (Throwable e) {
                    try {
                        mPageDb.recyclePage(newId);
                    } catch (Throwable e2) {
                        // Panic.
                        e.addSuppressed(e2);
                        close(e);
                    }
                    throw e;
                }

                nodeMapRemove(node, Long.hashCode(oldId));
            }

            dirty(node, newId);
            nodeMapPut(node);
            return true;
        }
    }

    /**
     * Mark an unmapped node as dirty. Caller must hold commit lock and exclusive latch on
     * node. Method does nothing if node is already dirty. Latch is never released by this
     * method, even if an exception is thrown.
     */
    void markUnmappedDirty(Node node) throws IOException {
        if (node.mCachedState != mCommitState) {
            node.write(mPageDb);

            long newId = mPageDb.allocPage();
            long oldId = node.mId;

            try {
                mPageDb.deletePage(oldId);
            } catch (Throwable e) {
                try {
                    mPageDb.recyclePage(newId);
                } catch (Throwable e2) {
                    // Panic.
                    e.addSuppressed(e2);
                    close(e);
                }
                throw e;
            }

            dirty(node, newId);
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Method must
     * not be called if node is already dirty. Latch is never released by this
     * method, even if an exception is thrown.
     */
    void doMarkDirty(Tree tree, Node node) throws IOException {
        if (node.mCachedState != CACHED_CLEAN) {
            node.write(mPageDb);
        }

        long newId = mPageDb.allocPage();
        long oldId = node.mId;

        try {
            if (node == tree.mRoot) {
                storeTreeRootId(tree, newId);
            }
        } catch (Throwable e) {
            try {
                mPageDb.recyclePage(newId);
            } catch (Throwable e2) {
                // Panic.
                e.addSuppressed(e2);
                close(e);
            }
            throw e;
        }

        try {
            if (oldId != 0) {
                // TODO: This can hang on I/O; release frame latch if deletePage would block?
                // Then allow thread to block without node latch held.
                mPageDb.deletePage(oldId);
                nodeMapRemove(node, Long.hashCode(oldId));
            }
        } catch (Throwable e) {
            try {
                if (node == tree.mRoot) {
                    storeTreeRootId(tree, oldId);
                }
                mPageDb.recyclePage(newId);
            } catch (Throwable e2) {
                // Panic.
                e.addSuppressed(e2);
                close(e);
            }
            throw e;
        }

        dirty(node, newId);
        nodeMapPut(node);
    }

    private void storeTreeRootId(Tree tree, long id) throws IOException {
        if (tree.mIdBytes != null) {
            byte[] encodedId = new byte[8];
            encodeLongLE(encodedId, 0, id);
            mRegistry.store(Transaction.BOGUS, tree.mIdBytes, encodedId);
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node.
     */
    private void dirty(Node node, long newId) throws IOException {
        /*P*/ // [|
        /*P*/ // if (mFullyMapped) {
        /*P*/ //     if (node.mPage == p_nonTreePage()) {
        /*P*/ //         node.mPage = mPageDb.dirtyPage(newId);
        /*P*/ //         node.asEmptyRoot();
        /*P*/ //     } else if (node.mPage != p_closedTreePage()) {
        /*P*/ //         node.mPage = mPageDb.copyPage(node.mId, newId); // copy on write
        /*P*/ //     }
        /*P*/ // }
        /*P*/ // ]

        node.mId = newId;
        mDirtyList.add(node, mCommitState);
    }

    /**
     * Remove the old node from the dirty list and swap in the new node. Caller must hold
     * commit lock and latched the old node. The cached state of the nodes is not altered.
     */
    void swapIfDirty(Node oldNode, Node newNode) {
        mDirtyList.swapIfDirty(oldNode, newNode);
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. This method
     * should only be called for nodes whose existing data is not needed.
     */
    void redirty(Node node) throws IOException {
        /*P*/ // [|
        /*P*/ // if (mFullyMapped) {
        /*P*/ //     mPageDb.dirtyPage(node.mId);
        /*P*/ // }
        /*P*/ // ]
        mDirtyList.add(node, mCommitState);
    }

    /**
     * Similar to markDirty method except no new page is reserved, and old page
     * is not immediately deleted. Caller must hold commit lock and exclusive
     * latch on node. Latch is never released by this method, unless an
     * exception is thrown.
     */
    void prepareToDelete(Node node) throws IOException {
        // Hello. My name is Inigo Montoya. You killed my father. Prepare to die. 
        if (node.mCachedState == mCheckpointFlushState) {
            // Node must be committed with the current checkpoint, and so
            // it must be written out before it can be deleted.
            try {
                node.write(mPageDb);
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. The
     * prepareToDelete method must have been called first. Latch is always
     * released by this method, even if an exception is thrown.
     */
    void deleteNode(Node node) throws IOException {
        deleteNode(node, true);
    }

    /**
     * @param canRecycle true if node's page can be immediately re-used
     */
    void deleteNode(Node node, boolean canRecycle) throws IOException {
        try {
            long id = node.mId;

            // Must be removed from map before page is deleted. It could be recycled too soon,
            // creating a NodeMap collision.
            nodeMapRemove(node, Long.hashCode(id));

            try {
                if (id != 0) {
                    if (canRecycle && node.mCachedState == mCommitState) {
                        // Newly reserved page was never used, so recycle it.
                        mPageDb.recyclePage(id);
                    } else {
                        // Old data must survive until after checkpoint.
                        mPageDb.deletePage(id);
                    }
                }
            } catch (Throwable e) {
                // Try to undo things.
                try {
                    nodeMapPut(node);
                } catch (Throwable e2) {
                    e.addSuppressed(e2);
                }
                throw e;
            }

            // When id is <= 1, it won't be moved to a secondary cache. Preserve the original
            // id for non-durable database to recycle it. Durable database relies on free list.
            node.mId = -id;

            // When node is re-allocated, it will be evicted. Ensure that eviction
            // doesn't write anything.
            node.mCachedState = CACHED_CLEAN;
        } catch (Throwable e) {
            node.releaseExclusive();
            throw e;
        }

        // Always releases the node latch.
        node.unused();
    }

    final byte[] fragmentKey(byte[] key) throws IOException {
        return fragment(key, key.length, mMaxKeySize);
    }

    /**
     * Breakup a large value into separate pages, returning a new value which
     * encodes the page references. Caller must hold commit lock.
     *
     * Returned value begins with a one byte header:
     *
     * 0b0000_ffip
     *
     * The leading 4 bits define the encoding type, which must be 0. The 'f' bits define the
     * full value length field size: 2, 4, 6, or 8 bytes. The 'i' bit defines the inline
     * content length field size: 0 or 2 bytes. The 'p' bit is clear if direct pointers are
     * used, and set for indirect pointers. Pointers are always 6 bytes.
     *
     * @param value can be null if value is all zeros
     * @param max maximum allowed size for returned byte array; must not be
     * less than 11 (can be 9 if full value length is < 65536)
     * @return null if max is too small
     */
    byte[] fragment(final byte[] value, final long vlength, int max)
        throws IOException
    {
        final int pageSize = mPageSize;
        long pageCount = vlength / pageSize;
        final int remainder = (int) (vlength % pageSize);

        if (vlength >= 65536) {
            // Subtract header size, full length field size, and size of one pointer.
            max -= (1 + 4 + 6);
        } else if (pageCount == 0 && remainder <= (max - (1 + 2 + 2))) {
            // Entire value fits inline. It didn't really need to be
            // encoded this way, but do as we're told.
            byte[] newValue = new byte[(1 + 2 + 2) + (int) vlength];
            newValue[0] = 0x02; // ff=0, i=1, p=0
            encodeShortLE(newValue, 1, (int) vlength);     // full length
            encodeShortLE(newValue, 1 + 2, (int) vlength); // inline length
            arrayCopyOrFill(value, 0, newValue, (1 + 2 + 2), (int) vlength);
            return newValue;
        } else {
            // Subtract header size, full length field size, and size of one pointer.
            max -= (1 + 2 + 6);
        }

        if (max < 0) {
            return null;
        }

        long pointerSpace = pageCount * 6;

        byte[] newValue;
        if (remainder <= max && remainder < 65536
            && (pointerSpace <= (max + (6 - 2) - remainder)))
        {
            // Remainder fits inline, minimizing internal fragmentation. All
            // extra pages will be full. All pointers fit too; encode direct.

            // Conveniently, 2 is the header bit and the inline length field size.
            final int inline = remainder == 0 ? 0 : 2;

            byte header = (byte) inline;
            final int offset;
            if (vlength < (1L << (2 * 8))) {
                // (2 byte length field)
                offset = 1 + 2;
            } else if (vlength < (1L << (4 * 8))) {
                header |= 0x04; // ff = 1 (4 byte length field)
                offset = 1 + 4;
            } else if (vlength < (1L << (6 * 8))) {
                header |= 0x08; // ff = 2 (6 byte length field)
                offset = 1 + 6;
            } else {
                header |= 0x0c; // ff = 3 (8 byte length field)
                offset = 1 + 8;
            }

            int poffset = offset + inline + remainder;
            newValue = new byte[poffset + (int) pointerSpace];
            if (pageCount > 0) {
                if (value == null) {
                    // Value is sparse, so just fill with null pointers.
                    fill(newValue, poffset, poffset + ((int) pageCount) * 6, (byte) 0);
                } else {
                    try {
                        int voffset = remainder;
                        while (true) {
                            Node node = allocDirtyFragmentNode();
                            try {
                                encodeInt48LE(newValue, poffset, node.mId);
                                p_copyFromArray(value, voffset, node.mPage, 0, pageSize);
                                if (pageCount == 1) {
                                    break;
                                }
                            } finally {
                                node.releaseExclusive();
                            }
                            pageCount--;
                            poffset += 6;
                            voffset += pageSize;
                        }
                    } catch (DatabaseException e) {
                        if (!e.isRecoverable()) {
                            close(e);
                        } else {
                            // Clean up the mess.
                            while ((poffset -= 6) >= (offset + inline + remainder)) {
                                deleteFragment(decodeUnsignedInt48LE(newValue, poffset));
                            }
                        }
                        throw e;
                    }
                }
            }

            newValue[0] = header;

            if (remainder != 0) {
                encodeShortLE(newValue, offset, remainder); // inline length
                arrayCopyOrFill(value, 0, newValue, offset + 2, remainder);
            }
        } else {
            // Remainder doesn't fit inline, so don't encode any inline
            // content. Last extra page will not be full.
            pageCount++;
            pointerSpace += 6;

            byte header;
            final int offset;
            if (vlength < (1L << (2 * 8))) {
                header = 0x00; // ff = 0, i=0
                offset = 1 + 2;
            } else if (vlength < (1L << (4 * 8))) {
                header = 0x04; // ff = 1, i=0
                offset = 1 + 4;
            } else if (vlength < (1L << (6 * 8))) {
                header = 0x08; // ff = 2, i=0
                offset = 1 + 6;
            } else {
                header = 0x0c; // ff = 3, i=0
                offset = 1 + 8;
            }

            if (pointerSpace <= (max + 6)) {
                // All pointers fit, so encode as direct.
                newValue = new byte[offset + (int) pointerSpace];
                if (pageCount > 0) {
                    if (value == null) {
                        // Value is sparse, so just fill with null pointers.
                        fill(newValue, offset, offset + ((int) pageCount) * 6, (byte) 0);
                    } else {
                        int poffset = offset;
                        try {
                            int voffset = 0;
                            while (true) {
                                Node node = allocDirtyFragmentNode();
                                try {
                                    encodeInt48LE(newValue, poffset, node.mId);
                                    /*P*/ byte[] page = node.mPage;
                                    if (pageCount > 1) {
                                        p_copyFromArray(value, voffset, page, 0, pageSize);
                                    } else {
                                        p_copyFromArray(value, voffset, page, 0, remainder);
                                        // Zero fill the rest, making it easier to extend later.
                                        p_clear(page, remainder, pageSize(page));
                                        break;
                                    }
                                } finally {
                                    node.releaseExclusive();
                                }
                                pageCount--;
                                poffset += 6;
                                voffset += pageSize;
                            }
                        } catch (DatabaseException e) {
                            if (!e.isRecoverable()) {
                                close(e);
                            } else {
                                // Clean up the mess.
                                while ((poffset -= 6) >= offset) {
                                    deleteFragment(decodeUnsignedInt48LE(newValue, poffset));
                                }
                            }
                            throw e;
                        }
                    }
                }
            } else {
                // Use indirect pointers.
                header |= 0x01;
                newValue = new byte[offset + 6];
                if (value == null) {
                    // Value is sparse, so just store a null pointer.
                    encodeInt48LE(newValue, offset, 0);
                } else {
                    int levels = calculateInodeLevels(vlength);
                    Node inode = allocDirtyFragmentNode();
                    try {
                        encodeInt48LE(newValue, offset, inode.mId);
                        writeMultilevelFragments(levels, inode, value, 0, vlength);
                    } catch (DatabaseException e) {
                        if (!e.isRecoverable()) {
                            close(e);
                        } else {
                            // Clean up the mess.
                            deleteMultilevelFragments(levels, inode, vlength);
                        }
                        throw e;
                    }
                }
            }

            newValue[0] = header;
        }

        // Encode full length field.
        if (vlength < (1L << (2 * 8))) {
            encodeShortLE(newValue, 1, (int) vlength);
        } else if (vlength < (1L << (4 * 8))) {
            encodeIntLE(newValue, 1, (int) vlength);
        } else if (vlength < (1L << (6 * 8))) {
            encodeInt48LE(newValue, 1, vlength);
        } else {
            encodeLongLE(newValue, 1, vlength);
        }

        return newValue;
    }

    int calculateInodeLevels(long vlength) {
        long[] caps = mFragmentInodeLevelCaps;
        int levels = 0;
        while (levels < caps.length) {
            if (vlength <= caps[levels]) {
                break;
            }
            levels++;
        }
        return levels;
    }

    static long decodeFullFragmentedValueLength(int header, /*P*/ byte[] fragmented, int off) {
        switch ((header >> 2) & 0x03) {
        default:
            return p_ushortGetLE(fragmented, off);
        case 1:
            return p_intGetLE(fragmented, off) & 0xffffffffL;
        case 2:
            return p_uint48GetLE(fragmented, off);
        case 3:
            return p_longGetLE(fragmented, off);
        }
    }

    /**
     * @param level inode level; at least 1
     * @param inode exclusive latched parent inode; always released by this method
     * @param value slice of complete value being fragmented
     */
    private void writeMultilevelFragments(int level, Node inode,
                                          byte[] value, int voffset, long vlength)
        throws IOException
    {
        try {
            /*P*/ byte[] page = inode.mPage;
            level--;
            long levelCap = levelCap(level);

            int childNodeCount = (int) ((vlength + (levelCap - 1)) / levelCap);

            int poffset = 0;
            try {
                for (int i=0; i<childNodeCount; i++) {
                    Node childNode = allocDirtyFragmentNode();
                    p_int48PutLE(page, poffset, childNode.mId);
                    poffset += 6;

                    int len = (int) Math.min(levelCap, vlength);
                    if (level <= 0) {
                        /*P*/ byte[] childPage = childNode.mPage;
                        p_copyFromArray(value, voffset, childPage, 0, len);
                        // Zero fill the rest, making it easier to extend later.
                        p_clear(childPage, len, pageSize(childPage));
                        childNode.releaseExclusive();
                    } else {
                        writeMultilevelFragments(level, childNode, value, voffset, len);
                    }

                    vlength -= len;
                    voffset += len;
                }
            } finally {
                // Zero fill the rest, making it easier to extend later. If an exception was
                // thrown, this simplies cleanup. All of the allocated pages are referenced,
                // but the rest are not.
                p_clear(page, poffset, pageSize(page));
            }
        } finally {
            inode.releaseExclusive();
        }
    }

    /**
     * Reconstruct a fragmented key.
     */
    byte[] reconstructKey(/*P*/ byte[] fragmented, int off, int len) throws IOException {
        try {
            return reconstruct(fragmented, off, len);
        } catch (LargeValueException e) {
            throw new LargeKeyException(e.getLength(), e.getCause());
        }
    }

    /**
     * Reconstruct a fragmented value.
     */
    byte[] reconstruct(/*P*/ byte[] fragmented, int off, int len) throws IOException {
        return reconstruct(fragmented, off, len, null);
    }

    /**
     * Reconstruct a fragmented value.
     *
     * @param stats non-null for stats: [0]: full length, [1]: number of pages (>0 if fragmented)
     * @return null if stats requested
     */
    byte[] reconstruct(/*P*/ byte[] fragmented, int off, int len, long[] stats)
        throws IOException
    {
        int header = p_byteGet(fragmented, off++);
        len--;

        long vLen;
        switch ((header >> 2) & 0x03) {
        default:
            vLen = p_ushortGetLE(fragmented, off);
            break;

        case 1:
            vLen = p_intGetLE(fragmented, off);
            if (vLen < 0) {
                vLen &= 0xffffffffL;
                if (stats == null) {
                    throw new LargeValueException(vLen);
                }
            }
            break;

        case 2:
            vLen = p_uint48GetLE(fragmented, off);
            if (vLen > Integer.MAX_VALUE && stats == null) {
                throw new LargeValueException(vLen);
            }
            break;

        case 3:
            vLen = p_longGetLE(fragmented, off);
            if (vLen < 0 || (vLen > Integer.MAX_VALUE && stats == null)) {
                throw new LargeValueException(vLen);
            }
            break;
        }

        {
            int vLenFieldSize = 2 + ((header >> 1) & 0x06);
            off += vLenFieldSize;
            len -= vLenFieldSize;
        }

        byte[] value;
        if (stats != null) {
            stats[0] = vLen;
            value = null;
        } else {
            try {
                value = new byte[(int) vLen];
            } catch (OutOfMemoryError e) {
                throw new LargeValueException(vLen, e);
            }
        }

        int vOff = 0;
        if ((header & 0x02) != 0) {
            // Inline content.
            int inLen = p_ushortGetLE(fragmented, off);
            off += 2;
            len -= 2;
            if (value != null) {
                p_copyToArray(fragmented, off, value, vOff, inLen);
            }
            off += inLen;
            len -= inLen;
            vOff += inLen;
            vLen -= inLen;
        }

        long pagesRead = 0;

        if ((header & 0x01) == 0) {
            // Direct pointers.
            while (len >= 6) {
                long nodeId = p_uint48GetLE(fragmented, off);
                off += 6;
                len -= 6;
                int pLen;
                if (nodeId == 0) {
                    // Reconstructing a sparse value. Array is already zero-filled.
                    pLen = Math.min((int) vLen, mPageSize);
                } else {
                    Node node = nodeMapLoadFragment(nodeId);
                    pagesRead++;
                    try {
                        /*P*/ byte[] page = node.mPage;
                        pLen = Math.min((int) vLen, pageSize(page));
                        if (value != null) {
                            p_copyToArray(page, 0, value, vOff, pLen);
                        }
                    } finally {
                        node.releaseShared();
                    }
                }
                vOff += pLen;
                vLen -= pLen;
            }
        } else {
            // Indirect pointers.
            long inodeId = p_uint48GetLE(fragmented, off);
            if (inodeId != 0) {
                Node inode = nodeMapLoadFragment(inodeId);
                pagesRead++;
                int levels = calculateInodeLevels(vLen);
                pagesRead += readMultilevelFragments(levels, inode, value, 0, vLen);
            }
        }

        if (stats != null) {
            stats[1] = pagesRead;
        }

        return value;
    }

    /**
     * @param level inode level; at least 1
     * @param inode shared latched parent inode; always released by this method
     * @param value slice of complete value being reconstructed; initially filled with zeros;
     * pass null for stats only
     * @return number of pages read
     */
    private long readMultilevelFragments(int level, Node inode,
                                         byte[] value, int voffset, long vlength)
        throws IOException
    {
        try {
            long pagesRead = 0;

            /*P*/ byte[] page = inode.mPage;
            level--;
            long levelCap = levelCap(level);

            int childNodeCount = (int) ((vlength + (levelCap - 1)) / levelCap);

            for (int poffset = 0, i=0; i<childNodeCount; poffset += 6, i++) {
                long childNodeId = p_uint48GetLE(page, poffset);
                int len = (int) Math.min(levelCap, vlength);

                if (childNodeId != 0) {
                    Node childNode = nodeMapLoadFragment(childNodeId);
                    pagesRead++;
                    if (level <= 0) {
                        if (value != null) {
                            p_copyToArray(childNode.mPage, 0, value, voffset, len);
                        }
                        childNode.releaseShared();
                    } else {
                        pagesRead += readMultilevelFragments
                            (level, childNode, value, voffset, len);
                    }
                }

                vlength -= len;
                voffset += len;
            }

            return pagesRead;
        } finally {
            inode.releaseShared();
        }
    }

    /**
     * Delete the extra pages of a fragmented value. Caller must hold commit lock.
     *
     * @param fragmented page containing fragmented value 
     */
    void deleteFragments(/*P*/ byte[] fragmented, int off, int len)
        throws IOException
    {
        int header = p_byteGet(fragmented, off++);
        len--;

        long vLen;
        if ((header & 0x01) == 0) {
            // Don't need to read the value length when deleting direct pointers.
            vLen = 0;
        } else {
            switch ((header >> 2) & 0x03) {
            default:
                vLen = p_ushortGetLE(fragmented, off);
                break;
            case 1:
                vLen = p_intGetLE(fragmented, off) & 0xffffffffL;
                break;
            case 2:
                vLen = p_uint48GetLE(fragmented, off);
                break;
            case 3:
                vLen = p_longGetLE(fragmented, off);
                break;
            }
        }

        {
            int vLenFieldSize = 2 + ((header >> 1) & 0x06);
            off += vLenFieldSize;
            len -= vLenFieldSize;
        }

        if ((header & 0x02) != 0) {
            // Skip inline content.
            int inLen = 2 + p_ushortGetLE(fragmented, off);
            off += inLen;
            len -= inLen;
        }

        if ((header & 0x01) == 0) {
            // Direct pointers.
            while (len >= 6) {
                long nodeId = p_uint48GetLE(fragmented, off);
                off += 6;
                len -= 6;
                deleteFragment(nodeId);
            }
        } else {
            // Indirect pointers.
            long inodeId = p_uint48GetLE(fragmented, off);
            if (inodeId != 0) {
                Node inode = removeInode(inodeId);
                int levels = calculateInodeLevels(vLen);
                deleteMultilevelFragments(levels, inode, vLen);
            }
        }
    }

    /**
     * @param level inode level; at least 1
     * @param inode exclusive latched parent inode; always released by this method
     */
    private void deleteMultilevelFragments(int level, Node inode, long vlength)
        throws IOException
    {
        /*P*/ byte[] page = inode.mPage;
        level--;
        long levelCap = levelCap(level);

        // Copy all child node ids and release parent latch early.
        int childNodeCount = (int) ((vlength + (levelCap - 1)) / levelCap);
        long[] childNodeIds = new long[childNodeCount];
        for (int poffset = 0, i=0; i<childNodeCount; poffset += 6, i++) {
            childNodeIds[i] = p_uint48GetLE(page, poffset);
        }
        deleteNode(inode);

        if (level <= 0) for (long childNodeId : childNodeIds) {
            deleteFragment(childNodeId);
        } else for (long childNodeId : childNodeIds) {
            long len = Math.min(levelCap, vlength);
            if (childNodeId != 0) {
                Node childNode = removeInode(childNodeId);
                deleteMultilevelFragments(level, childNode, len);
            }
            vlength -= len;
        }
    }

    /**
     * @param nodeId must not be zero
     * @return non-null Node with exclusive latch held
     */
    private Node removeInode(long nodeId) throws IOException {
        Node node = nodeMapGetAndRemove(nodeId);
        if (node == null) {
            node = allocLatchedNode(nodeId, NodeUsageList.MODE_UNEVICTABLE);
            /*P*/ // [
            node.type(TYPE_FRAGMENT);
            /*P*/ // ]
            readNode(node, nodeId);
        }
        return node;
    }

    /**
     * @param nodeId can be zero
     */
    private void deleteFragment(long nodeId) throws IOException {
        if (nodeId != 0) {
            Node node = nodeMapGetAndRemove(nodeId);
            if (node != null) {
                deleteNode(node);
            } else if (mInitialReadState != CACHED_CLEAN) {
                // Page was never used if nothing has ever been checkpointed.
                mPageDb.recyclePage(nodeId);
            } else {
                // Page is clean if not in a Node, and so it must survive until
                // after the next checkpoint.
                mPageDb.deletePage(nodeId);
            }
        }
    }

    private static long[] calculateInodeLevelCaps(int pageSize) {
        long[] caps = new long[10];
        long cap = pageSize;
        long scalar = pageSize / 6; // 6-byte pointers

        int i = 0;
        while (i < caps.length) {
            caps[i++] = cap;
            long next = cap * scalar;
            if (next / scalar != cap) {
                caps[i++] = Long.MAX_VALUE;
                break;
            }
            cap = next;
        }

        if (i < caps.length) {
            long[] newCaps = new long[i];
            arraycopy(caps, 0, newCaps, 0, i);
            caps = newCaps;
        }

        return caps;
    }

    long levelCap(int level) {
        return mFragmentInodeLevelCaps[level];
    }

    /**
     * If fragmented trash exists, non-transactionally delete all fragmented values. Expected
     * to be called only during recovery or replication leader switch.
     */
    void emptyAllFragmentedTrash(boolean checkpoint) throws IOException {
        FragmentedTrash trash = mFragmentedTrash;
        if (trash != null && trash.emptyAllTrash(mEventListener) && checkpoint) {
            checkpoint(false, 0, 0);
        }
    }

    /**
     * Obtain the trash for transactionally deleting fragmented values.
     */
    FragmentedTrash fragmentedTrash() throws IOException {
        FragmentedTrash trash = mFragmentedTrash;
        if (trash != null) {
            return trash;
        }
        mOpenTreesLatch.acquireExclusive();
        try {
            if ((trash = mFragmentedTrash) != null) {
                return trash;
            }
            Tree tree = openInternalTree(Tree.FRAGMENTED_TRASH_ID, true);
            return mFragmentedTrash = new FragmentedTrash(tree);
        } finally {
            mOpenTreesLatch.releaseExclusive();
        }
    }

    /*P*/ byte[] removeSparePage() {
        return mSparePagePool.remove();
    }

    void addSparePage(/*P*/ byte[] page) {
        mSparePagePool.add(page);
    }

    /**
     * Reads the node page, sets the id and cached state. Node must be latched exclusively.
     */
    void readNode(Node node, long id) throws IOException {
        /*P*/ // [
        mPageDb.readPage(id, node.mPage);
        /*P*/ // |
        /*P*/ // if (mFullyMapped) {
        /*P*/ //     node.mPage = mPageDb.directPagePointer(id);
        /*P*/ // } else {
        /*P*/ //     mPageDb.readPage(id, node.mPage);
        /*P*/ // }
        /*P*/ // ]

        node.mId = id;

        // NOTE: If initial state is clean, an optimization is possible, but it's a bit
        // tricky. Too many pages are allocated when evictions are high, write rate is high,
        // and commits are bogged down.  Keep some sort of cache of ids known to be dirty. If
        // reloaded before commit, then they're still dirty.
        //
        // A Bloom filter is not appropriate, because of false positives. A random evicting
        // cache works well -- it has no collision chains. Evict whatever else was there in
        // the slot. An array of longs should suffice.
        //
        // When a child node is loaded with a dirty state, the parent nodes must be updated
        // as well. This might force them to be evicted, and then the optimization is
        // lost. A better approach would avoid the optimization if the parent node is clean
        // or doesn't match the current commit state.

        node.mCachedState = mInitialReadState;
    }

    @Override
    EventListener eventListener() {
        return mEventListener;
    }

    @Override
    void checkpoint(boolean force, long sizeThreshold, long delayThresholdNanos)
        throws IOException
    {
        // Checkpoint lock ensures consistent state between page store and logs.
        mCheckpointLock.lock();
        try {
            if (mClosed) {
                return;
            }

            // Now's a good time to clean things up.
            cleanupUnreferencedTrees();

            final Node root = mRegistry.mRoot;

            long nowNanos = System.nanoTime();

            if (!force) {
                thresholdCheck : {
                    if (delayThresholdNanos == 0) {
                        break thresholdCheck;
                    }

                    if (delayThresholdNanos > 0 &&
                        ((nowNanos - mLastCheckpointNanos) >= delayThresholdNanos))
                    {
                        break thresholdCheck;
                    }

                    if (mRedoWriter == null || mRedoWriter.shouldCheckpoint(sizeThreshold)) {
                        break thresholdCheck;
                    }

                    // Thresholds not met for a full checkpoint, but fully sync the redo log
                    // for durability.
                    mRedoWriter.flushSync(true);

                    return;
                }

                // Thresholds for a full checkpoint are met.
                treeCheck: {
                    root.acquireShared();
                    try {
                        if (root.mCachedState != CACHED_CLEAN) {
                            // Root is dirty, do a full checkpoint.
                            break treeCheck;
                        }
                    } finally {
                        root.releaseShared();
                    }

                    // Root is clean, so no need for full checkpoint,
                    // but fully sync the redo log for durability.
                    if (mRedoWriter != null) {
                        mRedoWriter.flushSync(true);
                    }
                    return;
                }
            }

            mLastCheckpointNanos = nowNanos;

            if (mEventListener != null) {
                // Note: Events should not be delivered when exclusive commit lock is held.
                // The listener implementation might introduce extra blocking.
                mEventListener.notify(EventType.CHECKPOINT_BEGIN, "Checkpoint begin");
            }

            boolean resume = true;

            /*P*/ byte[] header = mCommitHeader;
            UndoLog masterUndoLog = mCommitMasterUndoLog;

            if (header == p_null()) {
                // Not resumed. Allocate new header early, before acquiring locks.
                header = p_calloc(mPageDb.pageSize());
                resume = false;
                if (masterUndoLog != null) {
                    throw new AssertionError();
                }
            }

            final RedoWriter redo = mRedoWriter;

            try {
                int hoff = mPageDb.extraCommitDataOffset();
                p_intPutLE(header, hoff + I_ENCODING_VERSION, ENCODING_VERSION);

                if (redo != null) {
                    // File-based redo log should create a new file, but not write to it yet.
                    redo.checkpointPrepare();
                }

                while (true) {
                    mCommitLock.acquireExclusive();

                    // Registry root is infrequently modified, and so shared latch
                    // is usually available. If not, cause might be a deadlock. To
                    // be safe, always release commit lock and start over.
                    if (root.tryAcquireShared()) {
                        break;
                    }

                    mCommitLock.releaseExclusive();
                }

                mCheckpointFlushState = CHECKPOINT_FLUSH_PREPARE;

                if (!resume) {
                    p_longPutLE(header, hoff + I_ROOT_PAGE_ID, root.mId);
                }

                final long redoNum, redoPos, redoTxnId;
                if (redo == null) {
                    redoNum = 0;
                    redoPos = 0;
                    redoTxnId = 0;
                } else {
                    // Switch and capture state while commit lock is held.
                    redo.checkpointSwitch();
                    redoNum = redo.checkpointNumber();
                    redoPos = redo.checkpointPosition();
                    redoTxnId = redo.checkpointTransactionId();
                }

                p_longPutLE(header, hoff + I_CHECKPOINT_NUMBER, redoNum);
                p_longPutLE(header, hoff + I_REDO_TXN_ID, redoTxnId);
                p_longPutLE(header, hoff + I_REDO_POSITION, redoPos);

                p_longPutLE(header, hoff + I_REPL_ENCODING,
                            mRedoWriter == null ? 0 : mRedoWriter.encoding());

                // TODO: I don't like all this activity with exclusive commit
                // lock held. UndoLog can be refactored to store into a special
                // Tree, but this requires more features to be added to Tree
                // first. Specifically, large values and appending to them.

                long txnId = 0;
                final long masterUndoLogId;

                if (resume) {
                    masterUndoLogId = masterUndoLog == null ? 0 : masterUndoLog.topNodeId();
                } else {
                    byte[] workspace = null;

                    for (TransactionContext txnContext : mTxnContexts) {
                        txnContext.acquireShared();
                        try {
                            txnId = txnContext.maxTransactionId(txnId);

                            if (txnContext.hasUndoLogs()) {
                                if (masterUndoLog == null) {
                                    masterUndoLog = new UndoLog(this, 0);
                                }
                                workspace = txnContext.writeToMaster(masterUndoLog, workspace);
                            }
                        } finally {
                            txnContext.releaseShared();
                        }
                    }

                    if (masterUndoLog == null) {
                        masterUndoLogId = 0;
                    } else {
                        masterUndoLogId = masterUndoLog.topNodeId();
                        if (masterUndoLogId == 0) {
                            // Nothing was actually written to the log.
                            masterUndoLog = null;
                        }
                    }

                    // Stash it to resume after an aborted checkpoint.
                    mCommitMasterUndoLog = masterUndoLog;
                }

                p_longPutLE(header, hoff + I_TRANSACTION_ID, txnId);
                p_longPutLE(header, hoff + I_MASTER_UNDO_LOG_PAGE_ID, masterUndoLogId);

                mPageDb.commit(resume, header, (boolean resume_, /*P*/ byte[] header_) -> {
                    flush(resume_, header_);
                });
            } catch (Throwable e) {
                if (mCommitHeader != header) {
                    p_delete(header);
                }

                if (mCheckpointFlushState == CHECKPOINT_FLUSH_PREPARE) {
                    // Exception was thrown with locks still held.
                    mCheckpointFlushState = CHECKPOINT_NOT_FLUSHING;
                    root.releaseShared();
                    mCommitLock.releaseExclusive();
                    if (redo != null) {
                        redo.checkpointAborted();
                    }
                }

                throw e;
            }

            // Reset for next checkpoint.
            p_delete(mCommitHeader);
            mCommitHeader = p_null();
            mCommitMasterUndoLog = null;

            if (masterUndoLog != null) {
                // Delete the master undo log, which won't take effect until
                // the next checkpoint.
                mCommitLock.lock();
                try {
                    if (!mClosed) {
                        masterUndoLog.doTruncate(mCommitLock, false);
                    }
                } finally {
                    mCommitLock.unlock();
                }
            }

            // Note: This step is intended to discard old redo data, but it can
            // get skipped if process exits at this point. Data is discarded
            // again when database is re-opened.
            if (mRedoWriter != null) {
                mRedoWriter.checkpointFinished();
            }

            if (mEventListener != null) {
                double duration = (System.nanoTime() - mLastCheckpointNanos) / 1_000_000_000.0;
                mEventListener.notify(EventType.CHECKPOINT_COMPLETE,
                                      "Checkpoint completed in %1$1.3f seconds",
                                      duration, TimeUnit.SECONDS);
            }
        } finally {
            mCheckpointLock.unlock();
        }
    }

    /**
     * Method is invoked with exclusive commit lock and shared root node latch
     * held. Both are released by this method.
     */
    private void flush(final boolean resume, final /*P*/ byte[] header) throws IOException {
        Object custom = mCustomTxnHandler;
        if (custom != null) {
            custom = mCustomTxnHandler.checkpointStart(this);
        }

        int stateToFlush = mCommitState;

        if (resume) {
            // Resume after an aborted checkpoint.
            if (header != mCommitHeader) {
                throw new AssertionError();
            }
            stateToFlush ^= 1;
        } else {
            if (mInitialReadState != CACHED_CLEAN) {
                mInitialReadState = CACHED_CLEAN; // Must be set before switching commit state.
            }
            mCommitState = (byte) (stateToFlush ^ 1);
            mCommitHeader = header;
        }

        mCheckpointFlushState = stateToFlush;

        mRegistry.mRoot.releaseShared();
        mCommitLock.releaseExclusive();

        if (mRedoWriter != null) {
            mRedoWriter.checkpointStarted();
        }

        if (mEventListener != null) {
            mEventListener.notify(EventType.CHECKPOINT_FLUSH, "Flushing all dirty nodes");
        }

        try {
            mDirtyList.flush(mPageDb, stateToFlush);

            if (mRedoWriter != null) {
                mRedoWriter.checkpointFlushed();
            }

            if (mCustomTxnHandler != null) {
                mCustomTxnHandler.checkpointFinish(this, custom);
            }
        } finally {
            mCheckpointFlushState = CHECKPOINT_NOT_FLUSHING;
        }
    }

    // Called by DurablePageDb with header latch held.
    static long readRedoPosition(/*P*/ byte[] header, int offset) {
        return p_longGetLE(header, offset + I_REDO_POSITION);
    }
}
