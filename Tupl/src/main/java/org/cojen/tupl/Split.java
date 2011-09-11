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

import java.io.IOException;

/**
 * Short-lived object for capturing the state of a partially completed node split.
 *
 * @author Brian S O'Neill
 */
class Split {
    final boolean mSplitRight;
    private final long mSiblingId;
    // FIXME: Add state to sibling to prevent eviction and make this field final.
    private volatile TreeNode mSibling;

    // In many cases a copy of the key is not necessary; a simple reference to
    // the appropriate sub node works fine. This strategy assumes that the sub
    // node will not be compacted before the split is completed. For this
    // reason, Split is always constructed with a copied key.
    private final byte[] mSplitKey;

    /**
     * @param sibling must have exclusive lock when called; is released as a side-effect
     */
    Split(boolean splitRight, TreeNode sibling, byte[] splitKey) {
        mSplitRight = splitRight;
        mSiblingId = sibling.mId;
        mSibling = sibling;
        mSplitKey = splitKey;
        sibling.releaseExclusive();
    }

    /**
     * Compares to the split key, returning <0 if given key is lower, 0 if
     * equal, >0 if greater.
     */
    int compare(byte[] key) {
        return Utils.compareKeys(key, 0, key.length, mSplitKey, 0, mSplitKey.length);
    }

    /**
     * Allows a search to continue into a split node by selecting the original node or the
     * sibling. If the original node is returned, its shared lock is still held. If the
     * sibling is returned, it will have a shared latch held and the original node's latch
     * is released.
     *
     * @param node node which was split; shared latch must be held
     * @return original node or sibling
     */
    TreeNode selectNodeShared(TreeNodeStore store, TreeNode node, byte[] key) throws IOException {
        TreeNode sibling = mSibling;
        sibling.acquireSharedUnfair();

        if (mSiblingId != sibling.mId) {
            // Sibling was evicted, which is extremely rare.
            synchronized (this) {
                sibling = mSibling;
                if (mSiblingId != sibling.mId) {
                    sibling.releaseShared();
                    sibling = store.allocLatchedNode();
                    sibling.read(store, mSiblingId);
                    sibling.downgrade();
                    mSibling = sibling;
                }
            }
        }

        TreeNode left, right;
        if (mSplitRight) {
            left = node;
            right = sibling;
        } else {
            left = sibling;
            right = node;
        }

        if (compare(key) < 0) {
            right.releaseShared();
            return left;
        } else {
            left.releaseShared();
            return right;
        }
    }

    /**
     * Allows a search/insert/update to continue into a split node by selecting the
     * original node or the sibling. If the original node is returned, its exclusive lock
     * is still held. If the sibling is returned, it will have an exclusive latch held and
     * the original node's latch is released.
     *
     * @param node node which was split; exclusive latch must be held
     * @return original node or sibling
     */
    TreeNode selectNodeExclusive(TreeNodeStore store, TreeNode node, byte[] key)
        throws IOException
    {
        TreeNode sibling = latchSibling(store);

        TreeNode left, right;
        if (mSplitRight) {
            left = node;
            right = sibling;
        } else {
            left = sibling;
            right = node;
        }

        if (compare(key) < 0) {
            right.releaseExclusive();
            return left;
        } else {
            left.releaseExclusive();
            return right;
        }
    }

    /**
     * Performs a binary search against the split, returning the position
     * within the original node as if it had not split.
     */
    int binarySearchLeaf(TreeNodeStore store, TreeNode node, byte[] key) throws IOException {
        TreeNode sibling = latchSibling(store);

        TreeNode left, right;
        if (mSplitRight) {
            left = node;
            right = sibling;
        } else {
            left = sibling;
            right = node;
        }

        int searchPos;
        if (compare(key) < 0) {
            searchPos = left.binarySearchLeaf(key);
        } else {
            int highestPos = left.highestLeafPos();
            searchPos = right.binarySearchLeaf(key);
            if (searchPos < 0) {
                searchPos = searchPos - highestPos - 2;
            } else {
                searchPos = highestPos + 2 + searchPos;
            }
        }

        sibling.releaseExclusive();

        return searchPos;
    }

    /**
     * Returns the highest position within the original node as if it had not split.
     */
    int highestLeafPos(TreeNodeStore store, TreeNode node) throws IOException {
        TreeNode sibling = latchSibling(store);
        int pos = node.highestLeafPos() + sibling.highestLeafPos() + 2;
        sibling.releaseExclusive();
        return pos;
    }

    /**
     * Return the left split node, latched exclusively. Other node is unlatched.
     */
    TreeNode latchLeft(TreeNodeStore store, TreeNode node) throws IOException {
        if (mSplitRight) {
            return node;
        }
        TreeNode sibling = latchSibling(store);
        node.releaseExclusive();
        return sibling;
    }

    /**
     * @return sibling with exclusive latch held
     */
    TreeNode latchSibling(TreeNodeStore store) throws IOException {
        TreeNode sibling = mSibling;
        sibling.acquireExclusiveUnfair();
        if (mSiblingId != sibling.mId) {
            // Sibling was evicted, which is extremely rare.
            synchronized (this) {
                sibling = mSibling;
                if (mSiblingId != sibling.mId) {
                    sibling.releaseExclusive();
                    sibling = store.allocLatchedNode();
                    sibling.read(store, mSiblingId);
                    mSibling = sibling;
                }
            }
        }
        return sibling;
    }

    /**
     * @param frame frame affected by split; exclusive latch for sibling must also be held
     */
    void rebindFrame(TreeCursorFrame frame, TreeNode sibling) throws IOException {
        TreeNode node = frame.mNode;
        int pos = frame.mNodePos;

        if (mSplitRight) {
            int highestPos = node.highestPos();

            if (pos >= 0) {
                if (pos <= highestPos) {
                    // Nothing to do.
                } else {
                    frame.unbind();
                    frame.bind(sibling, pos - highestPos - 2);
                }
                return;
            }

            pos = ~pos;

            if (pos <= highestPos) {
                // Nothing to do.
                return;
            }

            if (pos == highestPos + 2) {
                byte[] key = frame.mNotFoundKey;
                if (compare(key) < 0) {
                    // Nothing to do.
                    return;
                }
            }

            frame.unbind();
            frame.bind(sibling, ~(pos - highestPos - 2));
        } else {
            int highestPos = sibling.highestPos();

            if (pos >= 0) {
                if (pos <= highestPos) {
                    frame.unbind();
                    frame.bind(sibling, pos);
                } else {
                    frame.mNodePos = pos - highestPos - 2;
                }
                return;
            }

            pos = ~pos;

            if (pos <= highestPos) {
                frame.unbind();
                frame.bind(sibling, ~pos);
                return;
            }

            if (pos == highestPos + 2) {
                byte[] key = frame.mNotFoundKey;
                if (compare(key) < 0) {
                    frame.unbind();
                    frame.bind(sibling, ~pos);
                    return;
                }
            }

            frame.mNodePos = ~(pos - highestPos - 2);
        }
    }

    /**
     * @return length of entry generated by copySplitKeyToParent
     */
    int splitKeyEncodedLength() {
        final int keyLen = mSplitKey.length;
        return ((keyLen <= 128 && keyLen > 0) ? 1 : 2) + keyLen;
    }

    /**
     * @param dest destination page of parent internal node
     * @param destLoc location in destination page
     * @return length of internal node encoded key entry
     */
    int copySplitKeyToParent(final byte[] dest, final int destLoc) {
        final byte[] key = mSplitKey;
        final int keyLen = key.length;

        int loc = destLoc;
        if (keyLen <= 128 && keyLen > 0) {
            dest[loc++] = (byte) (keyLen - 1);
        } else {
            dest[loc++] = (byte) (0x80 | (keyLen >> 8));
            dest[loc++] = (byte) keyLen;
        }
        System.arraycopy(key, 0, dest, loc, keyLen);

        return loc + keyLen - destLoc;
    }
}