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
public class ArrayType extends Type {
    private static final long HASH_BASE = 1858514625934181638L;

    ArrayType(Schemata schemata, long typeId, short flags) {
        super(schemata, typeId, flags);
    }

    public Type getElementType() {
        // FIXME
        throw null;
    }

    public long getMinLength() {
        // FIXME
        throw null;
    }

    public long getMaxRange() {
        // FIXME
        throw null;
    }

    static long computeHash(short flags, Type elementType, long minLength, long maxRange) {
        long hash = mixHash(HASH_BASE + flags, elementType);
        hash = hash * 31 + minLength;
        hash = hash * 31 + maxRange;
        return hash;
    }

    static byte[] encodeValue(short flags, Type elementType, long minLength, long maxRange) {
        byte[] value = new byte[1 + 2 + 8 + 8 + 8];
        value[0] = 2; // polymorph prefix
        Utils.encodeShortBE(value, 1, flags);
        encodeType(value, 3, elementType);
        Utils.encodeLongBE(value, 11, minLength);
        Utils.encodeLongBE(value, 19, maxRange);
        return value;
    }
}
