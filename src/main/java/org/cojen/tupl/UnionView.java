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

import java.io.IOException;

import java.util.Arrays;
import java.util.Comparator;

import java.util.function.Function;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class UnionView implements View {
    private final Ordering mOrdering;
    private final View[] mSources;

    /* 
       zero:       allow cursors for all sources
       bit 31 set: disallow cursors for first source
       bit 1 set:  disallow cursors for remaining sources

       Note: updates to this field don't need to be thread-safe
    */
    private int mAllowCursors;

    UnionView(View[] sources) {
        // Determining the ordering also validates that no inputs are null.
        Ordering ordering = sources[0].getOrdering();
        for (int i=1; i<sources.length; i++) {
            if (sources[i].getOrdering() != ordering) {
                ordering = Ordering.UNSPECIFIED;
            }
        }

        mOrdering = ordering;
        mSources = sources;
    }

    @Override
    public Ordering getOrdering() {
        return mOrdering;
    }

    @Override
    public int characteristics() {
        // FIXME
        throw null;
    }

    @Override
    public Comparator<byte[]> getComparator() {
        // FIXME
        throw null;
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Scanner newScanner(Transaction txn) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Scanner newScannerNoValues(Transaction txn) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Updater newUpdater(Transaction txn) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Updater newUpdaterNoValues(Transaction txn) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mSources[0].newTransaction(durabilityMode);
    }

    private Transaction enterTransaction(Transaction txn) throws IOException {
        if (txn == null) {
            txn = newTransaction(null);
        } else {
            txn.enter();
            txn.lockMode(LockMode.UPGRADABLE_READ);
        }
        return txn;
    }

    @Override
    public byte[] load(Transaction txn, byte[] key) throws IOException {
        txn = enterTransaction(txn);
        try {
            for (View source : mSources) {
                byte[] value = source.load(txn, key);
                if (value != null) {
                    return value;
                }
            }
            return null;
        } finally {
            txn.exit();
        }
    }

    @Override
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (value == null) {
            delete(txn, value);
            return;
        }

        txn = enterTransaction(txn);
        try {
            // Logically, must delete all existing entries, and then store an entry in the
            // first source. The lock acquisition order must always be from first to last
            // source, to avoid deadlocks. Using a cursor on the first source eliminates extra
            // work, but lock acquisition order must be the same. In addition, the sources
            // might map to the same underlying index, and so the first source must be stored
            // after all the others. Otherwise, the effective outcome might be a delete.

            if (mAllowCursors >= 0) withCursor: {
                Cursor first;
                try {
                    first = mSources[0].newCursor(txn);
                } catch (UnsupportedOperationException e) {
                    mAllowCursors |= 1 << 31;
                    break withCursor;
                }

                try {
                    // Find first now and lock.
                    first.autoload(false);
                    first.find(key);

                    for (int i=1; i<mSources.length; i++) {
                        mSources[i].delete(txn, key);
                    }

                    // Now store the final value.
                    first.commit(value);
                } finally {
                    first.reset();
                }

                return;
            }

            delete(txn, key);
            mSources[0].store(txn, key, value);
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        txn = enterTransaction(txn);
        try {
            // See notes in store method.

            if (mAllowCursors >= 0) withCursor: {
                Cursor first;
                try {
                    first = mSources[0].newCursor(txn);
                } catch (UnsupportedOperationException e) {
                    mAllowCursors |= 1 << 31;
                    break withCursor;
                }

                byte[] old;
                try {
                    // Find first now and lock.
                    first.find(key);
                    old = first.value();

                    for (int i=1; i<mSources.length; i++) {
                        mSources[i].delete(txn, key);
                    }

                    // Now store the final value.
                    first.commit(value);
                } finally {
                    first.reset();
                }

                return old;
            }

            byte[] old = mSources[0].exchange(txn, key, null);

            for (int i=1; i<mSources.length; i++) {
                mSources[i].delete(txn, key);
            }

            mSources[0].store(txn, key, value);

            txn.commit();
            return old;
        } finally {
            txn.exit();
        }
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (value == null) {
            return delete(txn, key);
        }

        txn = enterTransaction(txn);
        try {
            // See notes in store method.

            if (mAllowCursors >= 0) withCursor: {
                Cursor first;
                try {
                    first = mSources[0].newCursor(txn);
                } catch (UnsupportedOperationException e) {
                    mAllowCursors |= 1 << 31;
                    break withCursor;
                }

                try {
                    first.autoload(false);
                    first.find(key);

                    if (first.value() != null) {
                        // Found in the first source; delete from the remaining sources.
                        for (int i=1; i<mSources.length; i++) {
                            mSources[i].delete(txn, key);
                        }
                    } else checkRemaining: {
                        // Check for a match in the remaining sources.
                        if ((mAllowCursors & 1) == 0) remainingCursors: {
                            // Attempt to check the remaining sources with cursors.
                            Cursor[] cursors = new Cursor[mSources.length - 1];
                            try {
                                for (int i=1; i<mSources.length; i++) {
                                    cursors[i - 1] = mSources[i].newCursor(txn);
                                }
                            } catch (UnsupportedOperationException e) {
                                mAllowCursors |= 1;
                                for (Cursor c : cursors) {
                                    if (c != null) {
                                        c.reset();
                                    }
                                }
                                break remainingCursors;
                            }

                            try {
                                boolean found = false;
                                for (int i=0; i<cursors.length; i++) {
                                    Cursor c = cursors[i];
                                    c.autoload(false);
                                    c.find(key);
                                    if (c.value() == null) {
                                        c.reset();
                                        cursors[i] = null;
                                    } else {
                                        found = true;
                                    }
                                }
                                if (!found) {
                                    // All remaining sources checked; nothing found.
                                    txn.commit(); // keep the locks
                                    return false;
                                }
                                // Delete from the remaining sources.
                                for (Cursor c : cursors) {
                                    if (c != null) {
                                        c.store(null);
                                    }
                                }
                             } finally {
                                for (Cursor c : cursors) {
                                    if (c != null) {
                                        c.reset();
                                    }
                                }
                            }

                            // Commit the change in the first source.
                            break checkRemaining;
                        }

                        // Check the remaining sources without cursors.
                        find: {
                            for (int i=1; i<mSources.length; i++) {
                                if (mSources[i].load(txn, key) != null) {
                                    break find;
                                }
                            }
                            txn.commit(); // keep the locks
                            return false;
                        }

                        // Delete from the remaining sources.
                        for (int i=1; i<mSources.length; i++) {
                            mSources[i].delete(txn, key);
                        }

                        // Commit the change in the first source.
                    }

                    first.commit(value);
                    return true;
                } finally {
                    first.reset();
                }
            }

            boolean found = load(txn, key) != null;

            if (found) {
                delete(txn, key);
                mSources[0].store(txn, key, value);
            }

            txn.commit();
            return found;
        } finally {
            txn.exit();
        }
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        txn = enterTransaction(txn);
        try {
            // See notes in store method.

            if (mAllowCursors >= 0) withCursor: {
                Cursor first;
                try {
                    first = mSources[0].newCursor(txn);
                } catch (UnsupportedOperationException e) {
                    mAllowCursors |= 1 << 31;
                    break withCursor;
                }

                try {
                    first.autoload(oldValue != null);
                    first.find(key);

                    if (Arrays.equals(first.value(), oldValue)) {
                        // Found in the first source; delete from the remaining sources.
                        for (int i=1; i<mSources.length; i++) {
                            mSources[i].delete(txn, key);
                        }
                    } else checkRemaining: {
                        // Check for a match in the remaining sources.
                        if ((mAllowCursors & 1) == 0) remainingCursors: {
                            // Attempt to check the remaining sources with cursors.
                            Cursor[] cursors = new Cursor[mSources.length - 1];
                            try {
                                for (int i=1; i<mSources.length; i++) {
                                    cursors[i - 1] = mSources[i].newCursor(txn);
                                }
                            } catch (UnsupportedOperationException e) {
                                mAllowCursors |= 1;
                                for (Cursor c : cursors) {
                                    if (c != null) {
                                        c.reset();
                                    }
                                }
                                break remainingCursors;
                            }

                            try {
                                boolean found = false;
                                for (int i=0; i<cursors.length; i++) {
                                    Cursor c = cursors[i];
                                    c.autoload(oldValue != null);
                                    c.find(key);
                                    byte[] existing = c.value();
                                    if (existing == null) {
                                        c.reset();
                                        cursors[i] = null;
                                        found |= oldValue == null;
                                    } else {
                                        found |= Arrays.equals(existing, oldValue);
                                    }
                                }
                                if (!found) {
                                    // All remaining sources checked; nothing found.
                                    txn.commit(); // keep the locks
                                    return false;
                                }
                                // Delete from the remaining sources.
                                for (Cursor c : cursors) {
                                    if (c != null) {
                                        c.store(null);
                                    }
                                }
                             } finally {
                                for (Cursor c : cursors) {
                                    if (c != null) {
                                        c.reset();
                                    }
                                }
                            }

                            // Commit the change in the first source.
                            break checkRemaining;
                        }

                        // Check the remaining sources without cursors.
                        find: {
                            for (int i=1; i<mSources.length; i++) {
                                if (Arrays.equals(mSources[i].load(txn, key), oldValue)) {
                                    break find;
                                }
                            }
                            txn.commit(); // keep the locks
                            return false;
                        }

                        // Delete from the remaining sources.
                        for (int i=1; i<mSources.length; i++) {
                            mSources[i].delete(txn, key);
                        }

                        // Commit the change in the first source.
                    }

                    first.commit(newValue);
                    return true;
                } finally {
                    first.reset();
                }
            }

            boolean found = Arrays.equals(load(txn, key), oldValue);

            if (found) {
                delete(txn, key);
                if (newValue != null) {
                    mSources[0].store(txn, key, newValue);
                }
            }

            txn.commit();
            return found;
        } finally {
            txn.exit();
        }
    }

    @Override
    public boolean delete(Transaction txn, byte[] key) throws IOException {
        txn = enterTransaction(txn);
        try {
            boolean result = false;
            for (View source : mSources) {
                result |= source.delete(txn, key);
            }
            txn.commit();
            return result;
        } finally {
            txn.exit();
        }
    }

    @Override
    public LockResult lockShared(Transaction txn, byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResult lockUpgradable(Transaction txn, byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResult lockExclusive(Transaction txn, byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResult lockCheck(Transaction txn, byte[] key) {
        throw new UnsupportedOperationException();
    }

    private View newView(Function<View, View> op) {
        View[] sources = mSources.clone();
        for (int i=0; i<sources.length; i++) {
            sources[i] = op.apply(sources[i]);
        }
        return new UnionView(sources);
    }

    @Override
    public View viewGe(byte[] key) {
        return newView(v -> v.viewGe(key));
    }

    @Override
    public View viewGt(byte[] key) {
        return newView(v -> v.viewGt(key));
    }

    @Override
    public View viewLe(byte[] key) {
        return newView(v -> v.viewLe(key));
    }

    @Override
    public View viewLt(byte[] key) {
        return newView(v -> v.viewLt(key));
    }

    @Override
    public View viewPrefix(byte[] prefix, int trim) {
        return newView(v -> v.viewPrefix(prefix, trim));
    }

    @Override
    public View viewTransformed(Transformer transformer) {
        return newView(v -> v.viewTransformed(transformer));
    }

    @Override
    public View viewReverse() {
        return newView(View::viewReverse);
    }

    @Override
    public View viewUnmodifiable() {
        View[] sources = mSources;

        for (int i=0; i<sources.length; i++) {
            View source = sources[i];
            if (!source.isUnmodifiable()) {
                sources = sources.clone();
                while (true) {
                    sources[i] = source.viewUnmodifiable();
                    if (++i >= sources.length) {
                        return new UnionView(sources);
                    }
                    source = sources[i];
                }
            }
        }

        return this;
    }

    @Override
    public boolean isUnmodifiable() {
        for (View source : mSources) {
            if (!source.isUnmodifiable()) {
                return false;
            }
        }
        return true;
    }
}
