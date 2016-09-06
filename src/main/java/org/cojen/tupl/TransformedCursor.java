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

package org.cojen.tupl;

import java.io.IOException;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class TransformedCursor implements Cursor {
    private final Cursor mSource;
    private final Transformer mTransformer;

    private byte[] mKey;
    private byte[] mValue;

    TransformedCursor(Cursor source, Transformer transformer) {
        mSource = source;
        mTransformer = transformer;
    }

    @Override
    public Ordering getOrdering() {
        return mSource.getOrdering();
    }

    @Override
    public Transaction link(Transaction txn) {
        return mSource.link(txn);
    }

    @Override
    public Transaction link() {
        return mSource.link();
    }

    @Override
    public byte[] key() {
        return mKey;
    }

    @Override
    public byte[] value() {
        return mValue;
    }

    @Override
    public boolean autoload(boolean mode) {
        return mSource.autoload(mode);
    }

    @Override
    public boolean autoload() {
        return mSource.autoload();
    }

    @Override
    public final int compareKeyTo(byte[] rkey) {
        return mSource.compareKeyTo(mTransformer.inverseTransformKey(rkey));
    }

    @Override
    public final int compareKeyTo(byte[] rkey, int offset, int length) {
        if (offset != 0 || length != rkey.length) {
            byte[] newRkey = new byte[length];
            System.arraycopy(rkey, offset, newRkey, 0, length);
            rkey = newRkey;
        }
        return mSource.compareKeyTo(mTransformer.inverseTransformKey(rkey));
    }

    @Override
    public LockResult first() throws IOException {
        LockResult result;
        try {
            result = mSource.first();
        } catch (LockFailureException e) {
            throw transformCurrent(e);
        }
        result = transformCurrent(result);
        return result == null ? next() : result;
    }

    @Override
    public LockResult last() throws IOException {
        LockResult result;
        try {
            result = mSource.last();
        } catch (LockFailureException e) {
            throw transformCurrent(e);
        }
        result = transformCurrent(result);
        return result == null ? previous() : result;
    }

    @Override
    public LockResult skip(long amount) throws IOException {
        return ViewUtils.skipWithLocks(this, amount);
    }

    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        return ViewUtils.skipWithLocks(this, amount, limitKey, inclusive);
    }

    @Override
    public LockResult next() throws IOException {
        final Cursor c = mSource;
        while (true) {
            LockResult result;
            try {
                result = c.next();
            } catch (LockFailureException e) {
                throw transformCurrent(e);
            }
            result = transformCurrent(result);
            if (result != null) {
                return result;
            }
        }
    }

    @Override
    public LockResult nextLe(final byte[] limitTKey) throws IOException {
        byte[] limitKey = inverseTransformKey(limitTKey);
        if (limitKey == null) {
            limitKey = mTransformer.inverseTransformKeyLt(limitTKey);
            if (limitKey == null) {
                reset();
                return LockResult.UNOWNED;
            }
        }
        final Cursor c = mSource;
        while (true) {
            LockResult result;
            try {
                result = c.nextLe(limitKey);
            } catch (LockFailureException e) {
                throw transformCurrent(e);
            }
            result = transformCurrent(result);
            if (result != null) {
                return result;
            }
        }
    }

    @Override
    public LockResult nextLt(final byte[] limitTKey) throws IOException {
        final Cursor c = mSource;
        LockResult result;
        byte[] limitKey = inverseTransformKey(limitTKey);
        if (limitKey == null) {
            limitKey = mTransformer.inverseTransformKeyLt(limitTKey);
            if (limitKey == null) {
                reset();
                return LockResult.UNOWNED;
            }
            while (true) {
                try {
                    result = c.nextLe(limitKey);
                } catch (LockFailureException e) {
                    throw transformCurrent(e);
                }
                result = transformCurrent(result);
                if (result != null) {
                    return result;
                }
            }
        } else {
            while (true) {
                try {
                    result = c.nextLt(limitKey);
                } catch (LockFailureException e) {
                    throw transformCurrent(e);
                }
                result = transformCurrent(result);
                if (result != null) {
                    return result;
                }
            }
        }
    }

    @Override
    public LockResult previous() throws IOException {
        final Cursor c = mSource;
        while (true) {
            LockResult result;
            try {
                result = c.previous();
            } catch (LockFailureException e) {
                throw transformCurrent(e);
            }
            result = transformCurrent(result);
            if (result != null) {
                return result;
            }
        }
    }

    @Override
    public LockResult previousGe(final byte[] limitTKey) throws IOException {
        byte[] limitKey = inverseTransformKey(limitTKey);
        if (limitKey == null) {
            limitKey = mTransformer.inverseTransformKeyGt(limitTKey);
            if (limitKey == null) {
                reset();
                return LockResult.UNOWNED;
            }
        }
        final Cursor c = mSource;
        while (true) {
            LockResult result;
            try {
                result = c.previousGe(limitKey);
            } catch (LockFailureException e) {
                throw transformCurrent(e);
            }
            result = transformCurrent(result);
            if (result != null) {
                return result;
            }
        }
    }

    @Override
    public LockResult previousGt(final byte[] limitTKey) throws IOException {
        final Cursor c = mSource;
        LockResult result;
        byte[] limitKey = inverseTransformKey(limitTKey);
        if (limitKey == null) {
            limitKey = mTransformer.inverseTransformKeyGt(limitTKey);
            if (limitKey == null) {
                reset();
                return LockResult.UNOWNED;
            }
            while (true) {
                try {
                    result = c.previousGe(limitKey);
                } catch (LockFailureException e) {
                    throw transformCurrent(e);
                }
                result = transformCurrent(result);
                if (result != null) {
                    return result;
                }
            }
        } else {
            while (true) {
                try {
                    result = c.previousGt(limitKey);
                } catch (LockFailureException e) {
                    throw transformCurrent(e);
                }
                result = transformCurrent(result);
                if (result != null) {
                    return result;
                }
            }
        }
    }

    @Override
    public LockResult find(final byte[] tkey) throws IOException {
        final byte[] key = inverseTransformKey(tkey);
        if (key == null) {
            reset();
            return LockResult.UNOWNED;
        }
        mKey = tkey;
        mValue = NOT_LOADED;
        return transformCurrent(mSource.find(key), key, tkey);
    }

    @Override
    public LockResult findGe(final byte[] tkey) throws IOException {
        byte[] key = inverseTransformKey(tkey);
        if (key == null) {
            key = mTransformer.inverseTransformKeyGt(tkey);
            if (key == null) {
                reset();
                return LockResult.UNOWNED;
            }
        }

        LockResult result;
        try {
            result = mSource.findGe(key);
        } catch (LockFailureException e) {
            throw transformCurrent(e);
        }

        result = transformCurrent(result);
        return result == null ? next() : result;
    }

    @Override
    public LockResult findGt(final byte[] tkey) throws IOException {
        final Cursor c = mSource;
        LockResult result;
        try {
            byte[] key = inverseTransformKey(tkey);
            if (key == null) {
                key = mTransformer.inverseTransformKeyGt(tkey);
                if (key == null) {
                    reset();
                    return LockResult.UNOWNED;
                }
                result = c.findGe(key);
            } else {
                result = c.findGt(key);
            }
        } catch (LockFailureException e) {
            throw transformCurrent(e);
        }

        result = transformCurrent(result);
        return result == null ? next() : result;
    }

    @Override
    public LockResult findLe(final byte[] tkey) throws IOException {
        byte[] key = inverseTransformKey(tkey);
        if (key == null) {
            key = mTransformer.inverseTransformKeyLt(tkey);
            if (key == null) {
                reset();
                return LockResult.UNOWNED;
            }
        }

        LockResult result;
        try {
            result = mSource.findLe(key);
        } catch (LockFailureException e) {
            throw transformCurrent(e);
        }

        result = transformCurrent(result);
        return result == null ? previous() : result;
    }

    @Override
    public LockResult findLt(final byte[] tkey) throws IOException {
        final Cursor c = mSource;
        LockResult result;
        try {
            byte[] key = inverseTransformKey(tkey);
            if (key == null) {
                key = mTransformer.inverseTransformKeyLt(tkey);
                if (key == null) {
                    reset();
                    return LockResult.UNOWNED;
                }
                result = c.findLe(key);
            } else {
                result = c.findLt(key);
            }
        } catch (LockFailureException e) {
            throw transformCurrent(e);
        }

        result = transformCurrent(result);
        return result == null ? previous() : result;
    }

    @Override
    public LockResult findNearby(final byte[] tkey) throws IOException {
        final byte[] key = inverseTransformKey(tkey);
        if (key == null) {
            reset();
            return LockResult.UNOWNED;
        }
        mKey = tkey;
        mValue = NOT_LOADED;
        return transformCurrent(mSource.findNearby(key), key, tkey);
    }

    @Override
    public LockResult random(final byte[] lowTKey, final byte[] highTKey) throws IOException {
        byte[] lowKey = null;
        if (lowTKey != null) {
            lowKey = mTransformer.inverseTransformKey(lowTKey);
            if (lowKey == null) {
                lowKey = mTransformer.inverseTransformKeyGt(lowTKey);
                if (lowKey == null) {
                    reset();
                    return LockResult.UNOWNED;
                }
            }
        }

        byte[] highKey = null;
        if (highTKey != null) {
            highKey = mTransformer.inverseTransformKey(highTKey);
            if (highKey == null) {
                highKey = mTransformer.inverseTransformKeyLt(highTKey);
                if (highKey == null) {
                    reset();
                    return LockResult.UNOWNED;
                }
            }
        }

        LockResult result;
        try {
            result = mSource.random(lowKey, highKey);
        } catch (LockFailureException e) {
            throw transformCurrent(e);
        }

        result = transformCurrent(result);

        if (result == null) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                result = next();
                if (mKey == null) {
                    // Reached the end, so wrap around.
                    result = first();
                }
            } else {
                result = previous();
                if (mKey == null) {
                    // Reached the end, so wrap around.
                    result = last();
                }
            }
        }

        return result;
    }

    @Override
    public LockResult lock() throws IOException {
        return mSource.lock();
    }

    @Override
    public LockResult load() throws IOException {
        final byte[] tkey = mKey;
        ViewUtils.positionCheck(tkey);
        mKey = tkey;
        mValue = NOT_LOADED;
        final Cursor c = mSource;
        return transformCurrent(c.load(), c.key(), tkey);
    }

    @Override
    public void store(final byte[] tvalue) throws IOException {
        final byte[] tkey = mKey;
        ViewUtils.positionCheck(tkey);
        final Cursor c = mSource;
        final byte[] key = c.key();
        ViewUtils.positionCheck(key);
        c.store(mTransformer.inverseTransformValue(tvalue, key, tkey));
        mValue = tvalue;
    }

    @Override
    public void commit(final byte[] tvalue) throws IOException {
        final byte[] tkey = mKey;
        ViewUtils.positionCheck(tkey);
        final Cursor c = mSource;
        final byte[] key = c.key();
        ViewUtils.positionCheck(key);
        c.commit(mTransformer.inverseTransformValue(tvalue, key, tkey));
        mValue = tvalue;
    }

    /*
    @Override
    public Stream newStream() {
        Cursor c = mSource;
        if (mKey == null && c.key() != null) {
            c = c.copy();
            c.reset();
        }
        return new TransformedStream(c.newStream(), mTransformer);
    }
    */

    @Override
    public Cursor copy() {
        TransformedCursor copy = new TransformedCursor(mSource.copy(), mTransformer);
        copy.mKey = Utils.cloneArray(mKey);
        copy.mValue = Utils.cloneArray(mValue);
        return copy;
    }

    @Override
    public void reset() {
        mKey = null;
        mValue = null;
        mSource.reset();
    }

    private byte[] inverseTransformKey(final byte[] tkey) {
        Utils.keyCheck(tkey);
        return mTransformer.inverseTransformKey(tkey);
    }

    private LockFailureException transformCurrent(LockFailureException e) throws IOException {
        mValue = NOT_LOADED;
        try {
            mKey = mTransformer.transformKey(mSource.key(), NOT_LOADED);
        } catch (Throwable e2) {
            reset();
            throw e2;
        }
        return e;
    }

    /**
     * Method returns null if entry was filtered out and cursor must be moved. As a
     * side-effect, the mKey and mValue fields are set to null when filtered out.
     *
     * @param result must not be null
     * @return null if cursor must be moved
     */
    private LockResult transformCurrent(LockResult result) throws IOException {
        final Cursor c = mSource;

        final byte[] key = c.key();
        if (key == null) {
            mKey = null;
            mValue = null;
            return LockResult.UNOWNED;
        }

        byte[] value = c.value();

        if (value == null) {
            byte[] tkey = mTransformer.transformKey(key, null);
            mKey = tkey;
            mValue = null;
            if (tkey != null) {
                // Retain the position and lock when value doesn't exist.
                return result;
            }
        } else {
            if (value == NOT_LOADED && mTransformer.requireValue()) {
                // Disabling autoload mode makes little sense when using a value
                // transformer, because the value must be loaded anyhow.
                c.load();
                value = c.value();
            }
            byte[] tkey = mTransformer.transformKey(key, value);
            mKey = tkey;
            if (tkey != null) {
                byte[] tvalue = mTransformer.transformValue(value, key, tkey);
                if (tvalue != null) {
                    mValue = tvalue;
                    return result;
                }
            }
            mValue = null;
        }

        // This point is reached when the entry was filtered out and the cursor must move.

        if (result == LockResult.ACQUIRED) {
            // Release the lock when filtered out, but maintain the cursor position.
            c.link().unlock();
        }

        return null;
    }

    /**
     * @param tkey mKey must have been set to this non-null key already
     */
    private LockResult transformCurrent(LockResult result, final byte[] key, final byte[] tkey)
        throws IOException
    {
        final Cursor c = mSource;
        final byte[] value = c.value();

        if (value == null) {
            // Retain the position and lock when value doesn't exist.
            mValue = null;
            return result;
        }

        byte[] tvalue;

        if (value != NOT_LOADED || !mTransformer.requireValue()) {
            tvalue = mTransformer.transformValue(value, key, tkey);
        } else {
            // Disabling autoload mode makes little sense when using a value transformer,
            // because the value must be loaded anyhow.
            c.load();
            tvalue = mTransformer.transformValue(c.value(), key, tkey);
        }

        mValue = tvalue;

        if (tvalue == null && result == LockResult.ACQUIRED) {
            // Release the lock when filtered out, but maintain the cursor position.
            c.link().unlock();
            result = LockResult.UNOWNED;
        }

        return result;
    }

    // Used by tests.
    Cursor source() {
        return mSource;
    }
}
