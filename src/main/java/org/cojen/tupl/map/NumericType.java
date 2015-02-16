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

import java.math.BigInteger;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class NumericType extends Type {
    /** Denotes any type. Bit length and range must be zero. */
    // FIXME: remove; define new top-level type instead
    //public static final short FORMAT_ANY = 0;

    /** Fixed size signed integer, specified with a bit length. Range must be zero. */
    public static final short FORMAT_FIXED_INTEGER = 1;

    /** Fixed size unsigned integer, specified with a bit length. Range must be zero. */
    public static final short FORMAT_FIXED_INTEGER_UNSIGNED = 2;

    /** IEEE binary floating point value. Bit length is 16, 32, 64, or 128. Range must be zero. */
    public static final short FORMAT_FIXED_FLOAT = 3;

    /** IEEE decimal floating point value. Bit length is 32, 64, or 128. Range must be zero. */
    public static final short FORMAT_FIXED_FLOAT_DECIMAL = 4;

    /** Variable sized signed integer, specified with a bit length and range. */
    public static final short FORMAT_VARIABLE_INTEGER = 5;

    /** Variable sized unsigned integer, specified with a bit length and range. */
    public static final short FORMAT_VARIABLE_INTEGER_UNSIGNED = 6;

    /** Big variable sized signed integer. Bit length and range must be zero. */
    public static final short FORMAT_BIG_INTEGER = 7;

    // Reserved.
    //public static final short FORMAT_BIG_INTEGER_UNSIGNED = 8;

    // Reserved.
    //public static final short FORMAT_BIG_FLOAT = 9;

    /** Big variable sized floating point decimal value. Bit length and range must be zero. */
    public static final short FORMAT_BIG_FLOAT_DECIMAL = 10;

    // FIXME: Code generation converts one format to another. Don't expose the encoder stuff.

    private static final long HASH_BASE = 5353985800091834713L;

    private final short mFormat;
    private final int mMinBitLength;
    private final int mMaxBitRange;

    NumericType(Schemata schemata, long typeId, short flags,
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

    @Override
    public boolean isFixedLength() {
        return mFormat >= FORMAT_FIXED_INTEGER & mFormat <= FORMAT_FIXED_FLOAT_DECIMAL;
    }

    @Override
    public String printData(byte[] data) {
        if (data == null) {
            return "null";
        }

        data = toBigEndian(data);

        switch (mFormat) {
            // FIXME: honor bit length
            // FIXME: non-divisible-by-8 length
        case FORMAT_FIXED_INTEGER:
            return new BigInteger(data).toString();

            // FIXME: honor bit length
            // FIXME: non-divisible-by-8 length
        case FORMAT_FIXED_INTEGER_UNSIGNED:
            return new BigInteger(1, data).toString();

        case FORMAT_FIXED_FLOAT:
        case FORMAT_FIXED_FLOAT_DECIMAL:
        case FORMAT_VARIABLE_INTEGER:
        case FORMAT_VARIABLE_INTEGER_UNSIGNED:
        case FORMAT_BIG_INTEGER:
        case FORMAT_BIG_FLOAT_DECIMAL:
        }

        // FIXME
        throw null;
    }

    @Override
    public String printKey(byte[] data) {
        if (data == null) {
            return "null";
        }

        data = toBigEndian(data);

        // FIXME
        throw null;
    }

    private byte[] toBigEndian(byte[] data) {
        if ((mFlags & FLAG_LITTLE_ENDIAN) != 0) {
            byte[] newData = new byte[data.length];
            for (int i=0; i<data.length; i++) {
                newData[newData.length - i - 1] = data[i];
            }
            data = newData;
        }
        return data;
    }

    @Override
    public byte[] parseData(String str) {
        // FIXME
        throw null;
    }

    @Override
    public byte[] parseKey(String str) {
        // FIXME
        throw null;
    }

    @Override
    void appendTo(StringBuilder b) {
        b.append("NumericType");
        b.append(" {");
        appendCommon(b);
        b.append(", ");
        b.append("format=");

        switch (mFormat) {
            /*
        case FORMAT_ANY:
            b.append("Any");
            break;
            */

        case FORMAT_FIXED_INTEGER:
            appendFixedInfo("FixedInteger", b);
            break;

        case FORMAT_FIXED_INTEGER_UNSIGNED:
            appendFixedInfo("FixedIntegerUnsigned", b);
            break;

        case FORMAT_FIXED_FLOAT:
            appendFixedInfo("FixedFloat", b);
            break;

        case FORMAT_FIXED_FLOAT_DECIMAL:
            appendFixedInfo("FixedFloatDecimal", b);
            break;

        case FORMAT_VARIABLE_INTEGER:
            appendVariableInfo("VariableInteger", b);
            break;

        case FORMAT_VARIABLE_INTEGER_UNSIGNED:
            appendVariableInfo("VariableIntegerUnsigned", b);
            break;

        case FORMAT_BIG_INTEGER:
            b.append("BigInteger");
            break;

        case FORMAT_BIG_FLOAT_DECIMAL:
            b.append("BigFloatDecimal");
            break;

        default:
            b.append(mFormat & 0xffffffff);
            break;
        }

        b.append('}');
    }

    private void appendFixedInfo(String desc, StringBuilder b) {
        b.append(desc);
        b.append(", ");
        b.append("bitLength=");
        b.append(mMinBitLength);
    }

    private void appendVariableInfo(String desc, StringBuilder b) {
        b.append(desc);
        b.append(", ");
        b.append("minBitLength=");
        b.append(mMinBitLength);
        b.append(", ");
        b.append("maxBitRange=");
        b.append(mMaxBitRange);
    }

    static NumericType decode(Schemata schemata, long typeId, byte[] value) {
        if (value[0] != TYPE_PREFIX_NUMERIC) {
            throw new IllegalArgumentException();
        }
        return new NumericType(schemata, typeId,
                               (short) Utils.decodeUnsignedShortBE(value, 1), // flags
                               (short) Utils.decodeUnsignedShortBE(value, 3), // format
                               Utils.decodeUnsignedShortBE(value, 5),  // minBitLength
                               Utils.decodeUnsignedShortBE(value, 7)); // maxBitRange
    }

    @Override
    long computeHash() {
        long hash = HASH_BASE + mFlags;
        hash = hash * 31 + mFormat;
        hash = hash * 31 + mMinBitLength;
        hash = hash * 31 + mMaxBitRange;
        return hash;
    }

    @Override
    byte[] encodeValue() {
        byte[] value = new byte[1 + 2 + 2 + 2 + 2];
        value[0] = TYPE_PREFIX_NUMERIC;
        Utils.encodeShortBE(value, 1, mFlags);
        Utils.encodeShortBE(value, 3, mFormat);
        Utils.encodeShortBE(value, 5, mMinBitLength);
        Utils.encodeShortBE(value, 7, mMaxBitRange);
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    <T extends Type> T equivalent(T type) {
        if (type instanceof NumericType) {
            NumericType other = (NumericType) type;
            if (mFlags == other.mFlags &&
                mFormat == other.mFormat &&
                mMinBitLength == other.mMinBitLength &&
                mMaxBitRange == other.mMaxBitRange)
            {
                return (T) this;
            }
        }
        return null;
    }
}
