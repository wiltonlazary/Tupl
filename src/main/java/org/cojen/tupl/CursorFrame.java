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

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 *
 *
 * @author Brian S O'Neill
 */
/*P*/
// Note: Atomic reference is to the next frame bound to a Node.
class CursorFrame extends AtomicReference<CursorFrame> {
    private static final int SPIN_LIMIT = Runtime.getRuntime().availableProcessors();

    private static final CursorFrame REBIND_FRAME = new CursorFrame();

    static final AtomicReferenceFieldUpdater<Node, CursorFrame>
        cLastUpdater = AtomicReferenceFieldUpdater.newUpdater
        (Node.class, CursorFrame.class, "mLastCursorFrame");

    // Linked list of CursorFrames bound to a Node. Atomic reference is the next frame.
    volatile CursorFrame mPrevCousin;

    // Node and position this CursorFrame is bound to.
    Node mNode;
    int mNodePos;

    // Parent stack frame. A CursorFrame which is bound to the root
    // Node has no parent frame.
    CursorFrame mParentFrame;

    // Reference to key which wasn't found. Only used by leaf frames.
    byte[] mNotFoundKey;

    CursorFrame() {
    }

    CursorFrame(CursorFrame parentFrame) {
        mParentFrame = parentFrame;
    }

    /**
     * Acquire a shared latch on this frame's bound node.
     *
     * @return frame node
     */
    final Node acquireShared() {
        Node node = mNode;
        while (true) {
            node.acquireShared();
            Node actualNode = mNode;
            if (actualNode == node) {
                return actualNode;
            }
            node.releaseShared();
            node = actualNode;
        }
    }

    /**
     * Acquire a shared latch on this frame's bound node.
     *
     * @return frame node, or null if not acquired
     */
    final Node tryAcquireShared() {
        Node node = mNode;
        while (node.tryAcquireShared()) {
            Node actualNode = mNode;
            if (actualNode == node) {
                return actualNode;
            }
            node.releaseShared();
            node = actualNode;
        }
        return null;
    }

    /**
     * Acquire an exclusive latch on this frame's bound node.
     *
     * @return frame node
     */
    final Node acquireExclusive() {
        Node node = mNode;
        while (true) {
            node.acquireExclusive();
            Node actualNode = mNode;
            if (actualNode == node) {
                return actualNode;
            }
            node.releaseExclusive();
            node = actualNode;
        }
    }

    /**
     * Acquire an exclusive latch on this frame's bound node.
     *
     * @return frame node, or null if not not bound
     */
    final Node acquireExclusiveIfBound() {
        Node node = mNode;
        while (node != null) {
            node.acquireExclusive();
            Node actualNode = mNode;
            if (actualNode == node) {
                return actualNode;
            }
            node.releaseExclusive();
            node = actualNode;
        }
        return null;
    }

    /**
     * Acquire an exclusive latch on this frame's bound node.
     *
     * @return frame node, or null if not acquired
     */
    final Node tryAcquireExclusive() {
        Node node = mNode;
        while (node.tryAcquireExclusive()) {
            Node actualNode = mNode;
            if (actualNode == node) {
                return actualNode;
            }
            node.releaseExclusive();
            node = actualNode;
        }
        return null;
    }

    /**
     * @param amount +/- 2
     */
    final void adjustParentPosition(int amount) {
        CursorFrame parent = mParentFrame;
        if (parent != null) {
            parent.mNodePos += amount;
        }
    }

    /**
     * Bind this unbound frame to a tree node. Node should be held with a shared or exclusive
     * latch.
     */
    final void bind(Node node, int nodePos) {
        mNode = node;
        mNodePos = nodePos;

        // Next is set to self to indicate that this frame is the last.
        this.set(this);

        int trials = 0;
        while (true) {
            CursorFrame last = node.mLastCursorFrame;
            mPrevCousin = last;
            if (last == null) {
                if (cLastUpdater.compareAndSet(node, null, this)) {
                    return;
                }
            } else if (last.get() == last && last.compareAndSet(last, this)) {
                // Note: The above check gets confused if the frame was recycled. Converting a
                // last frame to an interior frame doesn't imply that the frame is owned by the
                // same linked list.

                /*
                  Catch up before replacing the last frame reference. Here's why:

                  (T1 == thread 1 and T2 == thread 2)

                  1. T1 observes last frame A.
                  2.                                    T2 observes last frame A.
                  3.                                    T2 appends B to the end. (CAS)
                  4.                                    T2 sets the last frame to B.
                  5.                                    T2 unbinds B as the last frame.
                  6. T1 appends C to the end. (CAS)
                  7. T1 sets the last frame to C.
                  8.                                    T2 sets the last frame to A!

                  Step 8 should ideally occur before step 6, but it absolutely must be
                  performed before step 7. Step 8 could be performed with a CAS, simply giving
                  up if the CAS fails. CAS is expensive, but volatile reads are cheap. Note
                  that after the last frame reference has been confirmed to be correct, there's
                  no chance that it can change again. Another thread cannot sneak in because
                  step 6 atomically claims the new last frame within the list.

                */
                while (node.mLastCursorFrame != last);

                node.mLastCursorFrame = this;
                return;
            }

            trials++;

            if (trials >= SPIN_LIMIT) {
                // Spinning too much due to high contention. Back off a tad.
                Thread.yield();
                trials = 0;
            }
        }
    }

    /**
     * Bind this frame to a tree node, or moves the position if already bound. Node should be
     * held with a shared or exclusive latch.
     *
     * @throws IllegalStateException if bound to another node
     */
    final void bindOrReposition(Node node, int nodePos) {
        if (mNode == null) {
            bind(node, nodePos);
        } else if (mNode == node) {
            mNodePos = nodePos;
        } else {
            throw new IllegalStateException();
        }
    }

    /**
     * Rebind this already bound frame to another tree node, unless this frame is no longer
     * valid. Both Nodes should be held with an exclusive latch.
     */
    final void rebind(Node node, int nodePos) {
        // Unbind with a special marker, to prevent a concurrent full unbind operation from
        // thinking that the node is already unbound. The marker will force the other thread to
        // wait until the rebind is complete before unbinding.
        if (unbind(REBIND_FRAME)) {
            bind(node, nodePos);
        }
    }

    /**
     * Unbind this frame from a tree node. No latch is required. Unbound frames must not be
     * recycled, unless all nodes involved are latched exclusively. Concurrent recycling can
     * cause the bind method to believe that an observed last frame belongs to its linked
     * list. See comment in the bind method.
     *
     * @param to null to fully unbind and never use frame again, or REBIND_FRAME if rebinding
     */
    private boolean unbind(CursorFrame to) {
        int trials = 0;
        while (true) {
            CursorFrame n = this.get(); // get next frame

            if (n == null) {
                // Not in the list.
                return false;
            }

            if (n == this) {
                // Unbinding the last frame.
                Node node = mNode;
                if (node != null && node.mLastCursorFrame == this && this.compareAndSet(n, to)) {
                    if (node != mNode || node.mLastCursorFrame != this) {
                        // Frame is now locked, but node has changed due to a concurrent
                        // rebinding of this frame. Unlock and try again.
                        this.set(n);
                    } else {
                        // Update previous frame to be the new last frame.
                        CursorFrame p;
                        do {
                            p = this.mPrevCousin;
                        } while (p != null && (p.get() != this || !p.compareAndSet(this, p)));
                        // Update the last frame reference.
                        node.mLastCursorFrame = p;
                        return true;
                    }
                }
            } else {
                // Unbinding an interior or first frame.
                if (n.mPrevCousin == this && this.compareAndSet(n, to)) {
                    // Update next reference chain to skip over the unbound frame.
                    CursorFrame p;
                    do {
                        p = this.mPrevCousin;
                    } while (p != null && (p.get() != this || !p.compareAndSet(this, n)));
                    // Update previous reference chain to skip over the unbound frame.
                    n.mPrevCousin = p;
                    return true;
                }
            }

            trials++;

            if (trials >= SPIN_LIMIT) {
                // Spinning too much due to high contention. Back off a tad.
                Thread.yield();
                trials = 0;
            }
        }
    }

    /**
     * Lock this frame to prevent it from being concurrently unbound. No latch is required.
     *
     * @param lock non-null temporary frame to represent locked state
     * @return frame to pass to unlock method, or null if frame is not bound
     */
    final CursorFrame tryLock(CursorFrame lock) {
        int trials = 0;
        while (true) {
            CursorFrame n = this.get(); // get next frame

            if (n == null) {
                // Not in the list.
                return null;
            }

            if (n == this) {
                // Locking the last frame.
                Node node = mNode;
                if (node != null && node.mLastCursorFrame == this && this.compareAndSet(n, lock)) {
                    if (node != mNode || node.mLastCursorFrame != this) {
                        // Frame is now locked, but node has changed due to a concurrent
                        // rebinding of this frame. Unlock and try again.
                        this.set(n);
                    } else {
                        return n;
                    }
                }
            } else {
                // Locking an interior or first frame.
                if (n.mPrevCousin == this && this.compareAndSet(n, lock)) {
                    return n;
                }
            }

            trials++;

            if (trials >= SPIN_LIMIT) {
                // Spinning too much due to high contention. Back off a tad.
                Thread.yield();
                trials = 0;
            }
        }
    }

    /**
     * With this frame locked, lock the previous frame. Unlock it using this frame.
     *
     * @param lock non-null temporary frame to represent locked state
     * @return previous frame, or null if no previous frame exists
     */
    final CursorFrame tryLockPrevious(CursorFrame lock) {
        CursorFrame p;
        do {
            p = this.mPrevCousin;
        } while (p != null && (p.get() != this || !p.compareAndSet(this, lock)));
        return p;
    }

    /**
     * Unlock a locked frame.
     *
     * @param n non-null next frame, as provided by the tryLock method
     */
    final void unlock(CursorFrame n) {
        this.set(n);
    }

    /**
     * Returns the parent frame. No latch is required.
     */
    final CursorFrame peek() {
        return mParentFrame;
    }

    /**
     * Pop this, the leaf frame, returning the parent frame. No latch is required.
     */
    final CursorFrame pop() {
        unbind(null);
        CursorFrame parent = mParentFrame;
        mNode = null;
        mParentFrame = null;
        mNotFoundKey = null;
        return parent;
    }

    /**
     * Pop this, the leaf frame, returning void. No latch is required.
     */
    final void popv() {
        unbind(null);
        mNode = null;
        mParentFrame = null;
        mNotFoundKey = null;
    }

    /**
     * Pop the given non-null frame and all parent frames. No latch is required.
     */
    static void popAll(CursorFrame frame) {
        do {
            frame = frame.mNode == null ? frame.mParentFrame : frame.pop();
        } while (frame != null);
    }

    /**
     * Copy this frame and all parent frames.
     *
     * @param dest new frame instance to receive copy
     */
    final void copyInto(CursorFrame dest) {
        Node node = acquireShared();
        CursorFrame parent = mParentFrame;

        if (parent != null) {
            node.releaseShared();
            CursorFrame parentCopy = new CursorFrame();

            while (true) {
                // Need to check if parent is null, when looping back.
                if (parent != null) {
                    parent.copyInto(parentCopy);
                }

                // Parent can change when tree height is concurrently changing.
                node = acquireShared();
                final CursorFrame actualParent = mParentFrame;

                if (actualParent == parent) {
                    // Parent frame hasn't changed, so use the copy.
                    if (parent != null) {
                        dest.mParentFrame = parentCopy;
                    }
                    break;
                }

                // Get rid of the stale copy and do over, which should be rare.
                node.releaseShared();
                popAll(parentCopy);
                parent = actualParent;
            }
        }

        dest.mNotFoundKey = mNotFoundKey;
        dest.bind(node, mNodePos);
        node.releaseShared();
    }

    @Override
    public final String toString() {
        return Utils.toMiniString(this);
    }

    /**
     * Special frame type for tracking ghosted entries within leaf nodes. Unlike regular
     * frames, ghost frames don't prevent the bound node from being evicted.
     */
    final static class Ghost extends CursorFrame {
    }
}
