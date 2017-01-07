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

import java.lang.ref.WeakReference;
import java.lang.ref.ReferenceQueue;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class Checkpointer implements Runnable {
    private static final int STATE_INIT = 0, STATE_RUNNING = 1, STATE_CLOSED = 2;

    private final AtomicInteger mSuspendCount;
    private final ReferenceQueue<AbstractDatabase> mRefQueue;
    private final WeakReference<AbstractDatabase> mDatabaseRef;
    private final long mRateNanos;
    private final long mSizeThreshold;
    private final long mDelayThresholdNanos;
    private volatile Thread mThread;
    private volatile int mState;
    private Thread mShutdownHook;
    private List<ShutdownHook> mToShutdown;

    Checkpointer(AbstractDatabase db, DatabaseConfig config) {
        mSuspendCount = new AtomicInteger();

        mRateNanos = config.mCheckpointRateNanos;
        mSizeThreshold = config.mCheckpointSizeThreshold;
        mDelayThresholdNanos = config.mCheckpointDelayThresholdNanos;

        if (mRateNanos < 0) {
            mRefQueue = new ReferenceQueue<>();
            mDatabaseRef = new WeakReference<>(db, mRefQueue);
        } else {
            mRefQueue = null;
            mDatabaseRef = new WeakReference<>(db);
        }
    }

    /**
     * @param initialCheckpoint true to perform an initial checkpoint in the new thread
     */
    void start(boolean initialCheckpoint) {
        if (!initialCheckpoint) {
            mState = STATE_RUNNING;
        }

        Thread t = new Thread(this);
        t.setDaemon(true);
        t.setName("Checkpointer-" + Long.toUnsignedString(t.getId()));
        t.start();

        mThread = t;
    }

    @Override
    public void run() {
        try {
            if (mState == STATE_INIT) {
                // Start with an initial forced checkpoint.
                AbstractDatabase db = mDatabaseRef.get();
                if (db != null) {
                    db.checkpoint();
                }
                mState = STATE_RUNNING;
            }

            if (mRefQueue != null) {
                mRefQueue.remove();
                close(null);
                return;
            }

            long lastDurationNanos = 0;

            while (true) {
                long delayMillis = (mRateNanos - lastDurationNanos) / 1000000L;
                if (delayMillis > 0) {
                    Thread.sleep(delayMillis); 
                }

                AbstractDatabase db = mDatabaseRef.get();
                if (db == null) {
                    close(null);
                    return;
                }

                if (mSuspendCount.get() != 0) {
                    // Don't actually suspend the thread, allowing for weak reference checks.
                    lastDurationNanos = 0;
                } else try {
                    long startNanos = System.nanoTime();
                    db.checkpoint(false, mSizeThreshold, mDelayThresholdNanos);
                    long endNanos = System.nanoTime();

                    lastDurationNanos = endNanos - startNanos;
                } catch (DatabaseException e) {
                    EventListener listener = db.eventListener();
                    if (listener != null) {
                        listener.notify(EventType.CHECKPOINT_FAILED, "Checkpoint failed: %1$s", e);
                    }
                    if (!e.isRecoverable()) {
                        throw e;
                    }
                    lastDurationNanos = 0;
                }
            }
        } catch (Throwable e) {
            if (mState != STATE_CLOSED) {
                AbstractDatabase db = mDatabaseRef.get();
                if (db != null) {
                    Utils.closeQuietly(null, db, e);
                }
            }
            close(e);
        }
    }

    /**
     * Register to close the given object on shutdown or when the Database is
     * no longer referenced. The Shutdown object must not maintain a strong
     * reference to the Database.
     *
     * @param obj ignored if null
     * @return false if immediately shutdown
     */
    boolean register(ShutdownHook obj) {
        if (obj == null) {
            return false;
        }

        doRegister: if (mState != STATE_CLOSED) {
            synchronized (this) {
                if (mState == STATE_CLOSED) {
                    break doRegister;
                }

                if (mShutdownHook == null) {
                    Thread hook = new Thread(() -> Checkpointer.this.close(null));
                    try {
                        Runtime.getRuntime().addShutdownHook(hook);
                        mShutdownHook = hook;
                    } catch (IllegalStateException e) {
                        break doRegister;
                    }
                }

                if (mToShutdown == null) {
                    mToShutdown = new ArrayList<>(2);
                }

                mToShutdown.add(obj);
                return true;
            }
        }

        obj.shutdown();
        return false;
    }

    void suspend() {
        suspend(+1);
    }

    void resume() {
        suspend(-1);
    }

    private void suspend(int amt) {
        while (true) {
            int count = mSuspendCount.get() + amt;
            if (count < 0) {
                // Overflowed or too many resumes.
                throw new IllegalStateException();
            }
            if (mSuspendCount.compareAndSet(count - amt, count)) {
                break;
            }
        }
    }

    boolean isClosed() {
        return mState == STATE_CLOSED;
    }

    /**
     * @return thread to interrupt, when no checkpoint is in progress
     */
    Thread close(Throwable cause) {
        mState = STATE_CLOSED;
        mDatabaseRef.enqueue();
        mDatabaseRef.clear();

        List<ShutdownHook> toShutdown;
        synchronized (this) {
            if (mShutdownHook != null) {
                try {
                    Runtime.getRuntime().removeShutdownHook(mShutdownHook);
                } catch (Throwable e) {
                }
                mShutdownHook = null;
            }

            // Only run shutdown hooks if cleanly closing, to avoid deadlocks.
            if (mToShutdown == null || cause != null) {
                toShutdown = null;
            } else {
                toShutdown = new ArrayList<>(mToShutdown);
            }

            mToShutdown = null;
        }

        if (toShutdown != null) {
            for (ShutdownHook obj : toShutdown) {
                obj.shutdown();
            }
        }

        return mThread;
    }
}
