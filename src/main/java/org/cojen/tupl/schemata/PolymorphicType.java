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

package org.cojen.tupl.schemata;

import java.io.IOException;

import org.cojen.tupl.Transaction;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class PolymorphicType extends Type {
    private static final long HASH_BASE = 1902668584782181472L;

    /* FIXME: Should be: Type nameType, byte[] name, Type[] allowedTypes
    private final long mIdentifier;
    private final Type mTargetType;
    */

    // FIXME: User defines the tag to type mappings. Tag is a simple byte[] constant which must
    // not match a prefix of any other tag. This eliminates decoding ambiguity.

    PolymorphicType(Schemata schemata, long typeId, short flags) {
        super(schemata, typeId, flags);
    }

    /* FIXME: remove
    public long getIdentifier() {
        return mIdentifier;
    }

    public Type getTargetType() {
        return mTargetType;
    }
    */

    @Override
    public boolean isFixedLength() {
        // FIXME
        throw null;
    }

    @Override
    public String printData(byte[] data) {
        // FIXME
        throw null;
    }

    @Override
    public String printKey(byte[] data) {
        // FIXME
        throw null;
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
        b.append("PolymorphicType");
        b.append(" {");
        appendCommon(b);
        b.append(", ");
        // FIXME
        b.append('}');
    }

    static PolymorphicType decode(Transaction txn, Schemata schemata, long typeId, byte[] value)
        throws IOException
    {
        if (value[0] != TYPE_PREFIX_POLYMORPH) {
            throw new IllegalArgumentException();
        }
        // FIXME
        throw null;
    }

    @Override
    long computeHash() {
        // FIXME
        throw null;
    }

    @Override
    byte[] encodeValue() {
        // FIXME: polymorph prefix, uint_16 flags, MapType<uvarint_64, uvarint_64>
        throw null;
    }

    @Override
    @SuppressWarnings("unchecked")
    <T extends Type> T equivalent(T type) {
        // FIXME
        throw null;
    }
}
