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
public class AssembledType extends Type {
    private static final long HASH_BASE = 3315411704127731845L;

    AssembledType(Schemata schemata, long typeId, short flags) {
        super(schemata, typeId, flags);
    }

    public Type[] getElementTypes() {
        // FIXME
        throw null;
    }

    static long computeHash(short flags, Type[] elementTypes) {
        return mixHash(HASH_BASE + flags, elementTypes);
    }

    static byte[] encodeValue(short flags, Type[] elementTypes) {
        byte[] value = new byte[1 + 2 + elementTypes.length * 8];
        value[0] = 3; // polymorph prefix
        Utils.encodeShortBE(value, 1, flags);
        int off = 3;
        for (Type t : elementTypes) {
            encodeType(value, off, t);
            off += 8;
        }
        return value;
    }
}
