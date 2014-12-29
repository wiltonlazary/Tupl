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

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.io.Utils;

import static org.cojen.tupl.map.BasicType.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class Schemata {
    private static final byte PRIMARY_IX_PREFIX = 1, HASH_IX_PREFIX = 2;

    private final Database mDb;
    private final View mPrimaryIx;
    private final View mHashIx;

    /**
     * @param ix storage for schemata
     */
    public Schemata(Database db, View ix) {
        mDb = db;
        mPrimaryIx = ix.viewPrefix(new byte[] {PRIMARY_IX_PREFIX}, 1);
        mHashIx = ix.viewPrefix(new byte[] {HASH_IX_PREFIX}, 1);
    }

    // Note: Built-in type zero is reserved and means "void".

    public BasicType defineBasicType(Transaction txn,
                                     short flags, short format, int minBitLength, int maxBitRange)
        throws IOException
    {
        checkCommonFlags(flags);

        if (format < FORMAT_ANY | format > FORMAT_BIG_DECIMAL
            | (minBitLength & ~0xffff) != 0 | (maxBitRange & ~0xffff) != 0)
        {
            throw new IllegalArgumentException();
        }

        if (minBitLength != 0
            && (format < FORMAT_FIXED_INTEGER | format > FORMAT_UNSIGNED_VARIABLE_INTEGER))
        {
            throw new IllegalArgumentException();
        }

        if (maxBitRange != 0
            && (format < FORMAT_VARIABLE_INTEGER | format > FORMAT_UNSIGNED_VARIABLE_INTEGER))
        {
            throw new IllegalArgumentException();
        }

        // FIXME: Better design is to construct BasicType object and use common code for the
        // rest. The hash and value creation methods become virtual.

        long hash = BasicType.computeHash(flags, format, minBitLength, maxBitRange);
        byte[] value = BasicType.encodeValue(flags, format, minBitLength, maxBitRange);

        byte[] key = new byte[16];
        Utils.encodeLongBE(key, 0, hash);

        if (txn == null) {
            txn = mDb.newTransaction();
        } else {
            txn.enter();
        }

        try {
            Cursor hashCursor = mHashIx.newCursor(txn);
            try {
                hashCursor.findGe(key);
                byte[] existing = hashCursor.value();

                /* FIXME: if null, at end, else check if hash is same
                if (existing == null) {
                    // No collision.
                    // FIXME: key is wrong! need type id!
                    hashCursor.store(value);
                }
                */

            } finally {
                hashCursor.reset();
            }
        } finally {
            txn.exit();
        }

        // FIXME
        throw null;
    }

    public ArrayType defineArrayType(Transaction txn,
                                     short flags, Type elementType, long minLength, long maxRange)
        throws IOException
    {
        checkAssembledFlags(flags);

        if (elementType == null) {
            throw new IllegalArgumentException();
        }

        checkSchemata(elementType);

        if (minLength < 0 || maxRange < 0) {
            throw new IllegalArgumentException();
        }

        long hash = ArrayType.computeHash(flags, elementType, minLength, maxRange);

        // FIXME
        throw null;
    }

    /**
     * Note: Elements are always encoded in the order defined here. Optimal encoding can be
     * achieved by ordering fixed sized elements first and by enabling packing.
     */
    public AssembledType defineAssembledType(Transaction txn,
                                             short flags, Type[] elementTypes)
        throws IOException
    {
        checkAssembledFlags(flags);

        if (elementTypes == null) {
            throw new IllegalArgumentException();
        }

        for (Type t : elementTypes) {
            checkSchemata(t);
        }

        long hash = AssembledType.computeHash(flags, elementTypes);

        // FIXME
        throw null;
    }

    public NamedType defineNamedType(Transaction txn,
                                     short flags, Type baseType, Type nameType, byte[] name)
        throws IOException
    {
        checkCommonFlags(flags);

        // Note: Base type of null is void (type zero).

        checkSchemata(baseType);

        checkName(nameType, name);

        long hash = NamedType.computeHash(flags, baseType, nameType, name);

        // FIXME
        throw null;
    }

    public MapType defineMapType(Transaction txn,
                                 short flags, Type keyType, Type valueType)
        throws IOException
    {
        checkCommonFlags(flags);

        if (keyType == null) {
            throw new IllegalArgumentException();
        }

        checkSchemata(keyType);

        // Note: Value type of null is void, and so this defines a set instead of a map.

        checkSchemata(valueType);

        long hash = MapType.computeHash(flags, keyType, valueType);

        // FIXME
        throw null;
    }

    public PolymorphicType definePolymorphicType(Transaction txn,
                                                 short flags, Type nameType, byte[] name)
        throws IOException
    {
        checkCommonFlags(flags);

        checkName(nameType, name);

        /*
        for (Type t : allowedTypes) {
            checkSchemata(t);
        }
        */

        // Note: Identifiers are encoded as unsigned variable integers.

        long hash = PolymorphicType.computeHash(flags, nameType, name);

        // FIXME
        throw null;
    }

    private static void checkCommonFlags(short flags) {
        if ((flags & ~0b00000000_00001111) != 0) {
            throw new IllegalArgumentException();
        }
    }

    private static void checkAssembledFlags(short flags) {
        if ((flags & ~0b00000000_00111111) != 0) {
            throw new IllegalArgumentException();
        }
    }

    private void checkSchemata(Type type) {
        if (type != null && type.mSchemata != this) {
            throw new IllegalArgumentException();
        }
    }

    private void checkName(Type nameType, byte[] name) {
        if (nameType == null) {
            throw new IllegalArgumentException();
        }

        checkSchemata(nameType);

        if (name == null && (nameType.getFlags() & FLAG_NULLABLE) == 0) {
            throw new IllegalArgumentException();
        }
    }
}
