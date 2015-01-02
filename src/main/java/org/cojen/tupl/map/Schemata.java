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

import java.util.NoSuchElementException;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.LockMode;
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
    // FIXME: testing
    public static void main(String[] args) throws Exception {
        Database db = Database.open(new org.cojen.tupl.DatabaseConfig());
        View ix = db.openIndex("org.cojen.tupl.map.Schemata");
        Schemata s = new Schemata(db, ix);

        BasicType t1 = s.defineBasicType(0, BasicType.FORMAT_FIXED_INTEGER, 32, 0);
        System.out.println(t1);

        ArrayType t2 = s.defineArrayType(Type.FLAG_NULLABLE, t1, 0, 1000);
        System.out.println(t2);

        AssembledType t3 = s.defineAssembledType
            (Type.FLAG_NULLABLE | Type.FLAG_DESCENDING, t1, t2);
        System.out.println(t3);

        ArrayType t4 = s.defineArrayType(0, t3, 10, 10);
        System.out.println(t4);

        NamedType t5 = s.defineNamedType(0, null, t1, new byte[4]);
        System.out.println(t5);

        MapType t6 = s.defineMapType(0, t1, t2);
        System.out.println(t6);
    }

    private static final byte PRIMARY_IX_PREFIX = 1, HASH_IX_PREFIX = 2;

    private final Database mDb;

    // Maps type identifier to encoded type value.
    private final View mPrimaryIx;

    // Maps type hashcode plus type identifier to empty values.
    private final View mHashIx;

    private long mNextTypeId;

    /**
     * @param ix storage for schemata
     */
    public Schemata(Database db, View ix) throws IOException {
        mDb = db;
        mPrimaryIx = ix.viewPrefix(new byte[] {PRIMARY_IX_PREFIX}, 1);
        mHashIx = ix.viewPrefix(new byte[] {HASH_IX_PREFIX}, 1);

        synchronized (this) {
            Cursor c = mPrimaryIx.newCursor(null);
            try {
                c.autoload(false);
                c.last();
                byte[] key = c.key();
                if (key == null) {
                    mNextTypeId = 1;
                } else {
                    mNextTypeId = Utils.decodeLongBE(key, 0) + 1;
                }
            } finally {
                c.reset();
            }
        }
    }

    // Note: Built-in type zero is reserved and means "void".

    public BasicType defineBasicType(int flags, int format, int minBitLength, int maxBitRange)
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

        synchronized (this) {
            return defineType
                (new BasicType
                 (this, mNextTypeId, (short) flags, (short) format, minBitLength, maxBitRange));
        }
    }

    public ArrayType defineArrayType(int flags, Type elementType, long minLength, long maxRange)
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

        synchronized (this) {
            return defineType
                (new ArrayType
                 (this, mNextTypeId, (short) flags, elementType, minLength, maxRange));
        }
    }

    /**
     * Note: Elements are always encoded in the order defined here. Optimal encoding can be
     * achieved by ordering fixed sized elements first and by enabling packing.
     */
    public AssembledType defineAssembledType(int flags, Type... elementTypes)
        throws IOException
    {
        checkAssembledFlags(flags);

        if (elementTypes == null) {
            throw new IllegalArgumentException();
        }

        for (Type t : elementTypes) {
            checkSchemata(t);
        }

        synchronized (this) {
            return defineType
                (new AssembledType
                 (this, mNextTypeId, (short) flags, elementTypes));
        }
    }

    public NamedType defineNamedType(int flags, Type baseType, Type nameType, byte[] name)
        throws IOException
    {
        checkCommonFlags(flags);

        // Note: Base type of null is void (type zero).

        checkSchemata(baseType);

        checkNameAndSchemata(nameType, name);

        // FIXME: make sure name is correct for its type

        synchronized (this) {
            return defineType
                (new NamedType
                 (this, mNextTypeId, (short) flags, baseType, nameType, name));
        }
    }

    public MapType defineMapType(int flags, Type keyType, Type valueType)
        throws IOException
    {
        checkCommonFlags(flags);

        if (keyType == null) {
            throw new IllegalArgumentException();
        }

        checkSchemata(keyType);

        // Note: Value type of null is void, and so this defines a set instead of a map.

        checkSchemata(valueType);

        synchronized (this) {
            return defineType
                (new MapType
                 (this, mNextTypeId, (short) flags, keyType, valueType));
        }
    }

    public PolymorphicType definePolymorphicType(int flags, Type nameType, byte[] name)
        throws IOException
    {
        checkCommonFlags(flags);

        checkNameAndSchemata(nameType, name);

        /*
        for (Type t : allowedTypes) {
            checkSchemata(t);
        }
        */

        // Note: Identifiers are encoded as unsigned variable integers.

        // FIXME
        throw null;
    }

    /**
     * @throws NoSuchElementException if not found
     */
    public Type loadType(long typeId) throws IOException {
        return loadType(null, typeId);
    }

    private Type loadType(Transaction txn, long typeId) throws IOException {
        byte[] key = new byte[8];
        Utils.encodeLongBE(key, 0, typeId);

        byte[] value = mPrimaryIx.load(txn, key);

        if (value == null) {
            throw new NoSuchElementException();
        }

        switch (value[0]) {
        case TYPE_PREFIX_BASIC:
            return BasicType.decode(this, typeId, value);
        case TYPE_PREFIX_ARRAY:
            return ArrayType.decode(txn, this, typeId, value);
        case TYPE_PREFIX_ASSEMBLED:
            return AssembledType.decode(txn, this, typeId, value);
        case TYPE_PREFIX_NAMED:
            return NamedType.decode(txn, this, typeId, value);
        case TYPE_PREFIX_MAP:
            return MapType.decode(txn, this, typeId, value);
        case TYPE_PREFIX_POLYMORPH:
            return PolymorphicType.decode(txn, this, typeId, value);
        default:
            throw new IllegalArgumentException("Unknown type encoding: " + (value[0] & 0xff));
        }
    }

    Type decodeType(Transaction txn, byte[] value, int off) throws IOException {
        long typeId = Utils.decodeLongBE(value, off);
        return typeId == 0 ? null : loadType(txn, typeId);
    }

    Type[] decodeTypes(Transaction txn, byte[] value, int off) throws IOException {
        Type[] types = new Type[(value.length - off) / 8];
        for (int i=0; i<types.length; i++) {
            types[i] = decodeType(txn, value, off);
            off +=8;
        }
        return types;
    }

    // Caller must be synchronized.
    private <T extends Type> T defineType(T type) throws IOException {
        if (type.mTypeId == 0) {
            throw new IllegalArgumentException();
        }

        Transaction txn = mDb.newTransaction();
        try {
            txn.lockMode(LockMode.READ_COMMITTED);

            long hash = type.computeHash();

            byte[] hashPrefix = new byte[8];
            Utils.encodeLongBE(hashPrefix, 0, hash);

            Cursor hashCursor = mHashIx.viewPrefix(hashPrefix, 8).newCursor(txn);
            try {
                for (hashCursor.first(); hashCursor.key() != null; hashCursor.next()) {
                    Type existing = loadType(txn, Utils.decodeLongBE(hashCursor.key(), 0));
                    T matched = existing.equivalent(type);
                    if (matched != null) {
                        // Type definition already exists.
                        return matched;
                    }
                }
            } finally {
                hashCursor.reset();
            }

            byte[] hashKey = new byte[16];
            Utils.encodeLongBE(hashKey, 0, hash);
            Utils.encodeLongBE(hashKey, 8, type.mTypeId);

            if (!mHashIx.insert(txn, hashKey, Cursor.NOT_LOADED)) { // store empty value
                throw new IllegalStateException();
            }

            byte[] primaryKey = new byte[8];
            Utils.encodeLongBE(primaryKey, 0, type.mTypeId);

            if (!mPrimaryIx.insert(txn, primaryKey, type.encodeValue())) {
                throw new IllegalStateException();
            }

            txn.commit();

            // This is why caller must be synchronized.
            mNextTypeId = type.mTypeId + 1;
        } finally {
            txn.exit();
        }

        return type;
    }

    private static void checkCommonFlags(int flags) {
        if ((flags & ~0b00000000_00001111) != 0) {
            throw new IllegalArgumentException();
        }
    }

    private static void checkAssembledFlags(int flags) {
        if ((flags & ~0b00000000_00111111) != 0) {
            throw new IllegalArgumentException();
        }
    }

    private void checkSchemata(Type type) {
        if (type != null && type.mSchemata != this) {
            throw new IllegalArgumentException();
        }
    }

    private void checkNameAndSchemata(Type nameType, byte[] name) {
        if (nameType == null) {
            throw new IllegalArgumentException();
        }

        checkSchemata(nameType);

        if (name == null && (nameType.getFlags() & FLAG_NULLABLE) == 0) {
            throw new IllegalArgumentException();
        }
    }
}
