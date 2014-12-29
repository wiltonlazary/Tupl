/*
 *  Copyright 2014 Brian S O'Neill
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

package org.cojen.tupl.map;

import java.io.IOException;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public abstract class Type {
    /** Set to indicate that natural ordering is descending. */
    public static final short FLAG_DESCENDING = 1;

    /** Set to indicate that values can be null. */
    public static final short FLAG_NULLABLE = 2;

    /** Set to indicate that nulls are ordered lower than non-null values. */
    public static final short FLAG_NULL_LOW = 4;

    /** Set to indicate that value should be encoded in little-endian format. */
    public static final short FLAG_LITTLE_ENDIAN = 8;

    /**
     * For array and assembled types, set to indicate that elements smaller than one byte are
     * packed together.
     */
    public static final short FLAG_PACK_ELEMENTS = 16;

    /**
     * For array and assembled types, set to indicate that clusters of nullable elements can
     * share a common header byte.
     */
    public static final short FLAG_PACK_NULLS = 32;

    final Schemata mSchemata;
    final long mTypeId;
    final short mFlags;

    Type(Schemata schemata, long typeId, short flags) {
        mSchemata = schemata;
        mTypeId = typeId;
        mFlags = flags;
    }

    public long getTypeId() {
        return mTypeId;
    }

    public short getFlags() {
        return mFlags;
    }

    public Codec createCodec(Transaction txn) throws IOException {
        // FIXME
        throw null;
    }

    static long mixHash(long hash, Type type) {
        if (type == null) {
            hash *= 7;
        } else {
            hash = hash * 31 + type.getTypeId();
        }
        return hash;
    }

    static long mixHash(long hash, Type[] types) {
        if (types == null) {
            hash *= 7;
        } else {
            for (Type t : types) {
                hash = mixHash(hash, t);
            }
        }
        return hash;
    }

    static long mixHash(long hash, byte[] bytes) {
        if (bytes == null) {
            hash *= 7;
        } else {
            for (byte b : bytes) {
                hash = hash * 31 + b;
            }
        }
        return hash;
    }

    static void encodeType(byte[] value, int off, Type type) {
        Utils.encodeLongBE(value, off, type == null ? 0 : type.getTypeId());
    }
}
