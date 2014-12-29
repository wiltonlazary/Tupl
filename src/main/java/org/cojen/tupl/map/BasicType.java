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

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class BasicType extends Type {
    /** Denotes any type. Bit length and range must be zero. */
    public static final short FORMAT_ANY = 0;

    /** Fixed size signed integer, specified with a bit length. Range must be zero. */
    public static final short FORMAT_FIXED_INTEGER = 1;

    /** Fixed size unsigned integer, specified with a bit length. Range must be zero. */
    public static final short FORMAT_UNSIGNED_FIXED_INTEGER = 2;

    /** Variable sized signed integer, specified with a bit length and range. */
    public static final short FORMAT_VARIABLE_INTEGER = 3;

    /** Variable sized unsigned integer, specified with a bit length and range. */
    public static final short FORMAT_UNSIGNED_VARIABLE_INTEGER = 4;

    /** Big variable sized signed integer. Bit length and range must be zero. */
    public static final short FORMAT_BIG_INTEGER = 5;

    /** Big variable sized floating point decimal value. Bit length and range must be zero. */
    public static final short FORMAT_BIG_DECIMAL = 6;

    private static final long HASH_BASE = 5353985800091834713L;

    private final short mFormat;
    private final int mMinBitLength;
    private final int mMaxBitRange;

    BasicType(Schemata schemata, long typeId, short flags,
              short format, int minBitLength, int maxBitRange)
    {
        super(schemata, typeId, flags);
        mFormat = format;
        mMinBitLength = minBitLength;
        mMaxBitRange = maxBitRange;
    }

    public short getFormat() {
        return mFormat;
    }

    public int getMinBitLength() {
        return mMinBitLength;
    }

    public int getMaxBitRange() {
        return mMaxBitRange;
    }

    static long computeHash(short flags, short format, int minBitLength, int maxBitRange) {
        long hash = HASH_BASE + flags;
        hash = hash * 31 + format;
        hash = hash * 31 + minBitLength;
        hash = hash * 31 + maxBitRange;
        return hash;
    }

    static byte[] encodeValue(short flags, short format, int minBitLength, int maxBitRange) {
        byte[] value = new byte[1 + 2 + 2 + 2 + 2];
        value[0] = 1; // polymorph prefix
        Utils.encodeShortBE(value, 1, flags);
        Utils.encodeShortBE(value, 3, format);
        Utils.encodeShortBE(value, 5, minBitLength);
        Utils.encodeShortBE(value, 7, maxBitRange);
        return value;
    }
}
