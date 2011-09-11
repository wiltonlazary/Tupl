/*
 *  Copyright 2011 Brian S O'Neill
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

import java.io.Closeable;
import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import java.util.concurrent.locks.Lock;

import static org.cojen.tupl.TreeNode.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class TreeNodeStore implements Closeable {
    private static final int ENCODING_VERSION = 20110514;

    final PageStore mPageStore;

    private final BufferPool mSpareBufferPool;

    private final Latch mCacheLatch;
    private final int mMaxCachedNodeCount;
    private int mCachedNodeCount;
    private TreeNode mMostRecentlyUsed;
    private TreeNode mLeastRecentlyUsed;

    private final Lock mSharedCommitLock;

    // Is either CACHED_DIRTY_0 or CACHED_DIRTY_1. Access is guarded by commit lock.
    private byte mCommitState;

    // The root tree, which maps names to other trees.
    private final Tree mRegistry;

    private final Map<byte[], Tree> mOpenTrees;

    TreeNodeStore(PageStore store, int minCachedNodeCount, int maxCachedNodeCount)
        throws IOException
    {
        this(store, minCachedNodeCount, maxCachedNodeCount,
             Runtime.getRuntime().availableProcessors());
    }

    TreeNodeStore(PageStore store, int minCachedNodeCount, int maxCachedNodeCount,
                  int spareBufferCount)
        throws IOException
    {
        if (minCachedNodeCount > maxCachedNodeCount) {
            throw new IllegalArgumentException
                ("Minimum cached node count exceeds maximum count: " +
                 minCachedNodeCount + " > " + maxCachedNodeCount);
        }

        if (maxCachedNodeCount < 3) {
            // One is needed for the root node, and at least two nodes are
            // required for eviction code to function correctly. It always
            // assumes that the least recently used node points to a valid,
            // more recently used node.
            throw new IllegalArgumentException
                ("Maximum cached node count is too small: " + maxCachedNodeCount);
        }

        mPageStore = store;

        mSpareBufferPool = new BufferPool(store.pageSize(), spareBufferCount);

        mCacheLatch = new Latch();
        mMaxCachedNodeCount = maxCachedNodeCount - 1; // less one for root

        mSharedCommitLock = store.sharedCommitLock();
        mSharedCommitLock.lock();
        try {
            mCommitState = CACHED_DIRTY_0;
        } finally {
            mSharedCommitLock.unlock();
        }

        mRegistry = new Tree(this, null, loadRegistryRoot());
        mOpenTrees = new TreeMap<byte[], Tree>(KeyComparator.THE);

        // Pre-allocate nodes. They are automatically added to the usage list,
        // and so nothing special needs to be done to allow them to get used. Since
        // the initial state is clean, evicting these nodes does nothing.
        try {
            for (int i=minCachedNodeCount; --i>0; ) { // less one for root
                allocLatchedNode().releaseExclusive();
            }
        } catch (OutOfMemoryError e) {
            mMostRecentlyUsed = null;
            mLeastRecentlyUsed = null;

            try {
                mPageStore.close();
            } catch (IOException e2) {
                // Ignore.
            }

            throw new OutOfMemoryError
                ("Unable to allocate the minimum required number of cached nodes: " +
                 minCachedNodeCount);
        }
    }

    /**
     * Loads the root registry node, or creates one if store is new. Root node
     * is not eligible for eviction.
     */
    private TreeNode loadRegistryRoot() throws IOException {
        byte[] header = new byte[12];
        mPageStore.readExtraCommitData(header);
        int version = DataIO.readInt(header, 0);

        if (version == 0) {
            // Assume store is new and return a new empty leaf node.
            return new TreeNode(pageSize(), true);
        }

        if (version != ENCODING_VERSION) {
            throw new CorruptPageStoreException("Unknown encoding version: " + version);
        }

        long rootId = DataIO.readLong(header, 4);

        TreeNode root = new TreeNode(pageSize(), false);
        root.read(this, rootId);
        return root;
    }

    /**
     * Returns a full view into the given named sub database, creating it if
     * necessary.
     */
    View openView(byte[] nameKey) throws IOException {
        final Lock commitLock = sharedCommitLock();
        commitLock.lock();
        try {
            synchronized (mOpenTrees) {
                Tree tree = mOpenTrees.get(nameKey);
                if (tree != null) {
                    return tree;
                }
            }

            byte[] encodedRootId = mRegistry.get(nameKey);

            TreeNode rootNode;
            if (encodedRootId == null) {
                // Create a new empty leaf node.
                rootNode = new TreeNode(pageSize(), true);
            } else {
                rootNode = new TreeNode(pageSize(), false);
                rootNode.read(this, DataIO.readLong(encodedRootId, 0));
            }

            synchronized (mOpenTrees) {
                Tree tree = mOpenTrees.get(nameKey);
                if (tree == null) {
                    tree = new Tree(this, nameKey, rootNode);
                    mOpenTrees.put(nameKey, tree);
                }
                return tree;
            }

        } finally {
            commitLock.unlock();
        }
    }

    /**
     * Returns the fixed size of all pages in the store, in bytes.
     */
    int pageSize() {
        return mPageStore.pageSize();
    }

    /**
     * Access the shared commit lock, which prevents commits while held.
     */
    Lock sharedCommitLock() {
        return mSharedCommitLock;
    }

    /**
     * Returns a new or recycled TreeNode instance, latched exclusively, with an id
     * of zero and a clean state.
     */
    TreeNode allocLatchedNode() throws IOException {
        mCacheLatch.acquireExclusiveUnfair();
        try {
            int max = mMaxCachedNodeCount;
            if (mCachedNodeCount < max) {
                TreeNode node = new TreeNode(pageSize(), false);
                node.acquireExclusiveUnfair();

                mCachedNodeCount++;
                if ((node.mLessUsed = mMostRecentlyUsed) == null) {
                    mLeastRecentlyUsed = node;
                } else {
                    mMostRecentlyUsed.mMoreUsed = node;
                }
                mMostRecentlyUsed = node;

                return node;
            }

            do {
                TreeNode node = mLeastRecentlyUsed;
                (mLeastRecentlyUsed = node.mMoreUsed).mLessUsed = null;
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;

                if (node.tryAcquireExclusiveUnfair()) {
                    if (node.evict(this)) {
                        // Return with latch still held.
                        return node;
                    } else {
                        node.releaseExclusive();
                    }
                }
            } while (--max > 0);
        } finally {
            mCacheLatch.releaseExclusive();
        }

        // FIXME: Throw a better exception. Also, try all nodes again, but with
        // stronger latch request before giving up.
        throw new IllegalStateException("Cache is full");
    }

    /**
     * Returns a new reserved node, latched exclusively and marked dirty. Caller
     * must hold commit lock.
     */
    TreeNode newNodeForSplit() throws IOException {
        TreeNode node = allocLatchedNode();
        node.mId = mPageStore.reservePage();
        node.mCachedState = mCommitState;
        return node;
    }

    /**
     * Caller must hold commit lock and any latch on node.
     */
    boolean shouldMarkDirty(TreeNode node) {
        return node.mCachedState != mCommitState && node.mId != TreeNode.STUB_ID;
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Method does
     * nothing if node is already dirty. Latch is never released by this method,
     * even if an exception is thrown.
     *
     * @return true if just made dirty and id changed
     */
    boolean markDirty(Tree tree, TreeNode node) throws IOException {
        byte state = node.mCachedState;
        if (state == mCommitState || node.mId == TreeNode.STUB_ID) {
            return false;
        } else {
            doMarkDirty(tree, node);
            return true;
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Method must
     * not be called if node is already dirty. Latch is never released by this
     * method, even if an exception is thrown.
     */
    void doMarkDirty(Tree tree, TreeNode node) throws IOException {
        long oldId = node.mId;
        long newId = mPageStore.reservePage();

        if (oldId != 0) {
            mPageStore.deletePage(oldId);
        }

        if (node.mCachedState != CACHED_CLEAN) {
            node.write(this);
        }

        if (node == tree.mRoot && tree.mNameKey != null) {
            byte[] newEncodedId = new byte[8];
            DataIO.writeLong(newEncodedId, 0, newId);
            mRegistry.store(tree.mNameKey, newEncodedId);
        }

        node.mId = newId;
        node.mCachedState = mCommitState;
    }

    /**
     * Similar to markDirty method except no new page is reserved, and old page
     * is not immediately deleted. Caller must hold commit lock and exclusive
     * latch on node. Latch is never released by this method, even if an
     * exception is thrown.
     */
    void prepareToDelete(TreeNode node) throws IOException {
        // Hello. My name is ��igo Montoya. You killed my father. Prepare to die. 
        byte state = node.mCachedState;
        if (state != CACHED_CLEAN && state != mCommitState) {
            node.write(this);
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Latch is
     * never released by this method, even if an exception is thrown.
     */
    void deleteNode(TreeNode node) throws IOException {
        deletePage(node.mId);

        node.mId = 0;
        // FIXME: child node array should be recycled
        node.mChildNodes = null;

        // When node is re-allocated, it will be evicted. Ensure that eviction
        // doesn't write anything.
        node.mCachedState = CACHED_CLEAN;

        // Indicate that node is least recently used, allowing it to be
        // re-allocated immediately without evicting another node.
        mCacheLatch.acquireExclusiveUnfair();
        try {
            TreeNode lessUsed = node.mLessUsed;
            if (lessUsed != null) {
                TreeNode moreUsed = node.mMoreUsed;
                if ((lessUsed.mMoreUsed = moreUsed) == null) {
                    mMostRecentlyUsed = lessUsed;
                } else {
                    moreUsed.mLessUsed = lessUsed;
                }
                node.mLessUsed = null;
                (node.mMoreUsed = mLeastRecentlyUsed).mLessUsed = node;
                mLeastRecentlyUsed = node;
            }
        } finally {
            mCacheLatch.releaseExclusive();
        }
    }

    /**
     * Caller must hold commit lock.
     */
    void deletePage(long id) throws IOException {
        if (id != 0) {
            // TODO: Id can immediately be re-used, depending on the cached
            // state. Also see notes in the TreeNode.evict method.
            mPageStore.deletePage(id);
        }
    }

    /**
     * Indicate that non-root node is most recently used. Root node is not
     * managed in usage list and cannot be evicted.
     */
    void used(TreeNode node) {
        // Because this method can be a bottleneck, don't wait for exclusive
        // latch. If node is popular, it will get more chances to be identified
        // as most recently used. This strategy works well enough because cache
        // eviction is always a best-guess approach.
        if (mCacheLatch.tryAcquireExclusiveUnfair()) {
            TreeNode moreUsed = node.mMoreUsed;
            if (moreUsed != null) {
                TreeNode lessUsed = node.mLessUsed;
                if ((moreUsed.mLessUsed = lessUsed) == null) {
                    mLeastRecentlyUsed = moreUsed;
                } else {
                    lessUsed.mMoreUsed = moreUsed;
                }
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;
            }
            mCacheLatch.releaseExclusive();
        }
    }

    byte[] removeSpareBuffer() throws InterruptedIOException {
        return mSpareBufferPool.remove();
    }

    void addSpareBuffer(byte[] buffer) {
        mSpareBufferPool.add(buffer);
    }

    void readPage(long id, byte[] page) throws IOException {
        mPageStore.readPage(id, page);
    }

    void writeReservedPage(long id, byte[] page) throws IOException {
        mPageStore.writeReservedPage(id, page);
    }

    @Override
    public void close() throws IOException {
        mPageStore.close();
    }

    /**
     * Durably commit all changes to the database, while still allowing
     * concurrent access. Commit can be called by any thread, although only one
     * is permitted in at a time.
     */
    void commit() throws IOException {
        final TreeNode root = mRegistry.mRoot;

        // Commit lock must be acquired first, to prevent deadlock.
        mPageStore.exclusiveCommitLock().lock();
        root.acquireSharedUnfair();
        if (root.mCachedState == CACHED_CLEAN) {
            // Root is clean, so nothing to do.
            root.releaseShared();
            mPageStore.exclusiveCommitLock().unlock();
            return;
        }

        mPageStore.commit(new PageStore.CommitCallback() {
            @Override
            public byte[] prepare() throws IOException {
                return flush();
            }
        });
    }

    /**
     * Method is invoked with exclusive commit lock and shared root node latch held.
     */
    private byte[] flush() throws IOException {
        // Snapshot of all open trees.
        Tree[] trees;
        synchronized (mOpenTrees) {
            trees = mOpenTrees.values().toArray(new Tree[mOpenTrees.size()]);
        }

        final TreeNode root = mRegistry.mRoot;
        final long rootId = root.mId;
        final int stateToFlush = mCommitState;
        mCommitState = (byte) (CACHED_DIRTY_0 + ((stateToFlush - CACHED_DIRTY_0) ^ 1));
        mPageStore.exclusiveCommitLock().unlock();

        // Gather all nodes to flush.

        List<DirtyNode> dirtyList = new ArrayList<DirtyNode>(Math.min(1000, mMaxCachedNodeCount));
        mRegistry.gatherDirtyNodes(dirtyList, stateToFlush);

        for (Tree tree : trees) {
            tree.mRoot.acquireSharedUnfair();
            tree.gatherDirtyNodes(dirtyList, stateToFlush);
        }

        // Sort nodes by id, which helps make writes more sequentially ordered.
        Collections.sort(dirtyList);

        // Now write out all the dirty nodes. Some of them will have already
        // been concurrently written out, so check again.

        for (int mi=0; mi<dirtyList.size(); mi++) {
            TreeNode node = dirtyList.get(mi).mNode;
            dirtyList.set(mi, null);
            node.acquireExclusiveUnfair();
            if (node.mCachedState != stateToFlush) {
                // Was already evicted.
                node.releaseExclusive();
            } else {
                node.mCachedState = CACHED_CLEAN;
                node.downgrade();
                try {
                    node.write(this);
                } finally {
                    node.releaseShared();
                }
            }
        }

        byte[] header = new byte[12];
        DataIO.writeInt(header, 0, ENCODING_VERSION);
        DataIO.writeLong(header, 4, rootId);

        return header;
    }
}