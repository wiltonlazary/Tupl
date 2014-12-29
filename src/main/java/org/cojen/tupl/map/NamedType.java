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
public class NamedType extends Type {
    private static final long HASH_BASE = 4009787156610659885L;

    NamedType(Schemata schemata, long typeId, short flags) {
        super(schemata, typeId, flags);
    }

    public Type getBaseType() {
        // FIXME
        throw null;
    }

    public Type getNameType() {
        // FIXME
        throw null;
    }

    public byte[] getName() {
        // FIXME
        throw null;
    }

    static long computeHash(short flags, Type baseType, Type nameType, byte[] name) {
        long hash = mixHash(HASH_BASE + flags, baseType);
        hash = mixHash(hash, nameType);
        hash = mixHash(hash, name);
        if (name == null) {
            hash *= 7;
        } else {
            for (byte b : name) {
                hash = hash * 31 + b;
            }
        }
        return hash;
    }

    static byte[] encodeValue(short flags, Type baseType, Type nameType, byte[] name) {
        byte[] value = new byte[1 + 2 + 8 + 8 + 4 + name.length]; // FIXME: uvarint_32 length
        value[0] = 4; // polymorph prefix
        Utils.encodeShortBE(value, 1, flags);
        encodeType(value, 3, baseType);
        encodeType(value, 11, nameType);
        // FIXME: uvarint_32 length
        Utils.encodeIntBE(value, 19, name.length);
        System.arraycopy(name, 0, value, 23, name.length);
        return value;
    }
}
