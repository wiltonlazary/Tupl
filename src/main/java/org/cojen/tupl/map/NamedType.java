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

import java.util.Arrays;

import java.io.IOException;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class NamedType extends Type {
    private static final long HASH_BASE = 4009787156610659885L;

    private final Type mBaseType;
    private final Type mNameType;
    private final byte[] mName;

    NamedType(Schemata schemata, long typeId, short flags,
              Type baseType, Type nameType, byte[] name)
    {
        super(schemata, typeId, flags);
        mBaseType = baseType;
        mNameType = nameType;
        mName = name;
    }

    public Type getBaseType() {
        return mBaseType;
    }

    public Type getNameType() {
        return mNameType;
    }

    public byte[] getName() {
        byte[] name = mName;
        if (name != null && name.length != 0) {
            name = name.clone();
        }
        return name;
    }

    @Override
    public boolean isFixedLength() {
        return mBaseType != null && mBaseType.isFixedLength();
    }

    @Override
    void appendTo(StringBuilder b) {
        b.append("NamedType");
        b.append(" {");
        appendCommon(b);
        b.append(", ");
        b.append("baseType=");
        appendType(b, mBaseType);
        b.append(", ");
        b.append("nameType=");
        appendType(b, mNameType);
        b.append(", ");
        b.append("name=");
        appendName(b);
        b.append('}');
    }

    private void appendName(StringBuilder b) {
        if (mName == null) {
            b.append("null");
            return;
        }

        /* FIXME
        if (mNameType instanceof ArrayType) {
            Type elementType = ((ArrayType) mNameType).getElementType();
            if (elementType instanceof BasicType) {
                short format = ((BasicType) elementType).getFormat();
                if (format
            }
        }
        */
    }

    static NamedType decode(Transaction txn, Schemata schemata, long typeId, byte[] value)
        throws IOException
    {
        if (value[0] != TYPE_PREFIX_NAMED) {
            throw new IllegalArgumentException();
        }
        return new NamedType(schemata, typeId,
                             (short) Utils.decodeUnsignedShortBE(value, 1), // flags
                             schemata.decodeType(txn, value, 3),  // baseType
                             schemata.decodeType(txn, value, 11), // nameType
                             decodeName(value, 19));
    }

    private static byte[] decodeName(byte[] value, int off) {
        byte[] name = new byte[value.length - off];
        System.arraycopy(value, off, name, 0, name.length);
        return name;
    }

    @Override
    long computeHash() {
        long hash = mixHash(HASH_BASE + mFlags, mBaseType);
        hash = mixHash(hash, mNameType);
        hash = mixHash(hash, mName);
        if (mName == null) {
            hash *= 7;
        } else {
            for (byte b : mName) {
                hash = hash * 31 + b;
            }
        }
        return hash;
    }

    @Override
    byte[] encodeValue() {
        byte[] value = new byte[1 + 2 + 8 + 8 + mName.length];
        value[0] = TYPE_PREFIX_NAMED;
        Utils.encodeShortBE(value, 1, mFlags);
        encodeType(value, 3, mBaseType);
        encodeType(value, 11, mNameType);
        System.arraycopy(mName, 0, value, 19, mName.length);
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    <T extends Type> T equivalent(T type) {
        if (type instanceof NamedType) {
            NamedType other = (NamedType) type;
            if (mFlags == other.mFlags &&
                equalTypeIds(mBaseType, other.mBaseType) &&
                equalTypeIds(mNameType, other.mNameType) &&
                Arrays.equals(mName, other.mName))
            {
                return (T) this;
            }
        }
        return null;
    }
}
