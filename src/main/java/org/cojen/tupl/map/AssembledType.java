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
public class AssembledType extends Type {
    private static final long HASH_BASE = 3315411704127731845L;

    private final Type[] mElementTypes;

    AssembledType(Schemata schemata, long typeId, short flags, Type[] elementTypes) {
        super(schemata, typeId, flags);
        mElementTypes = elementTypes;
    }

    public Type[] getElementTypes() {
        Type[] types = mElementTypes;
        if (types != null && types.length != 0) {
            types = types.clone();
        }

        return types;
    }

    @Override
    public boolean isFixedLength() {
        Type[] types = mElementTypes;
        if (types != null) {
            for (Type t : types) {
                if (!t.isFixedLength()) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    void appendTo(StringBuilder b) {
        b.append("AssembledType");
        b.append(" {");
        appendCommon(b);
        b.append(", ");
        b.append("elementTypes=");
        appendTypes(b, mElementTypes);
        b.append('}');
    }

    static AssembledType decode(Transaction txn, Schemata schemata, long typeId, byte[] value)
        throws IOException
    {
        if (value[0] != TYPE_PREFIX_ASSEMBLED) {
            throw new IllegalArgumentException();
        }
        return new AssembledType(schemata, typeId,
                                 (short) Utils.decodeUnsignedShortBE(value, 1), // flags
                                 schemata.decodeTypes(txn, value, 3)); // elementTypes
    }

    @Override
    long computeHash() {
        return mixHash(HASH_BASE + mFlags, mElementTypes);
    }

    @Override
    byte[] encodeValue() {
        byte[] value = new byte[1 + 2 + mElementTypes.length * 8];
        value[0] = TYPE_PREFIX_ASSEMBLED;
        Utils.encodeShortBE(value, 1, mFlags);
        int off = 3;
        for (Type t : mElementTypes) {
            encodeType(value, off, t);
            off += 8;
        }
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    <T extends Type> T equivalent(T type) {
        if (type instanceof AssembledType) {
            AssembledType other = (AssembledType) type;
            if (mFlags == other.mFlags &&
                equalTypeIds(mElementTypes, other.mElementTypes))
            {
                return (T) this;
            }
        }
        return null;
    }
}
