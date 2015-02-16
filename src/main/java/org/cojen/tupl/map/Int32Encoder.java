/*
 *  Copyright 2015 Brian S O'Neill
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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class Int32Encoder extends Encoder {
    public int getMinLength() {
        return 4;
    }

    public int getMaxRange() {
        return 0;
    }

    public void encode(Record rec, boolean v) {
        encode(rec, v ? 1 : 0);
    }

    public void encode(Record rec, byte v) {
        encode(rec, (int) v);
    }

    public void encode(Record rec, char v) {
        encode(rec, (int) v);
    }

    public void encode(Record rec, short v) {
        encode(rec, (int) v);
    }

    public void encode(Record rec, int v) {
        Utils.encodeIntBE(rec.data, rec.start, v);
    }

    // FIXME: testing
    public void encodeKey(Record rec, int v) {
        encode(rec, v ^ 0x8000_0000);
    }

    // FIXME: testing
    public int intDecode(Record rec) {
        int start = rec.start;
        int result = Utils.decodeIntBE(rec.data, start);
        rec.start += 4;
        return result;
    }

    // FIXME: testing
    public int intDecodeKey(Record rec) {
        return intDecode(rec) ^ 0x8000_0000;
    }

    public void encode(Record rec, float v) {
        encode(rec, (int) v);
    }

    public void encode(Record rec, long v) {
        encode(rec, (int) v);
    }

    public void encode(Record rec, double v) {
        encode(rec, (int) v);
    }

    public void encode(Record rec, BigInteger v) {
        encode(rec, v.intValue());
    }

    public void encode(Record rec, BigDecimal v) {
        encode(rec, v.intValue());
    }

    public void encode(Record rec, String v) {
        // FIXME:
        throw null;
    }

    public void encode(Record rec, Boolean v) {
        // FIXME:
        throw null;
    }

    public void encode(Record rec, Byte v) {
        encode(rec, (int) v);
    }

    public void encode(Record rec, Character v) {
        encode(rec, (int) v);
    }

    public void encode(Record rec, Short v) {
        encode(rec, (int) v);
    }

    public void encode(Record rec, Integer v) {
        encode(rec, (int) v);
    }

    public void encode(Record rec, Float v) {
        encode(rec, (int) (float) v);
    }

    public void encode(Record rec, Long v) {
        encode(rec, (int) (long) v);
    }

    public void encode(Record rec, Double v) {
        encode(rec, (int) (double) v);
    }

    public void encode(Record rec, boolean[] v) {
        // FIXME:
        throw null;
    }

    public void encode(Record rec, byte[] v) {
        // FIXME:
        throw null;
    }

    public void encode(Record rec, char[] v) {
        // FIXME:
        throw null;
    }

    public void encode(Record rec, short[] v) {
        // FIXME:
        throw null;
    }

    public void encode(Record rec, int[] v) {
        // FIXME:
        throw null;
    }

    public void encode(Record rec, float[] v) {
        // FIXME:
        throw null;
    }

    public void encode(Record rec, long[] v) {
        // FIXME:
        throw null;
    }

    public void encode(Record rec, double[] v) {
        // FIXME:
        throw null;
    }

    public <T> void encode(Record rec, T[] v, Class<T> t) {
        // FIXME:
        throw null;
    }

    public <T> void encode(Record rec, List<T> v, Class<T> t) {
        // FIXME:
        throw null;
    }

    public <T> void encode(Record rec, Set<T> v, Class<T> t) {
        // FIXME:
        throw null;
    }

    public <K, V> void encode(Record rec, Map<K, V> v, Class<K> kt, Class<V> vt) {
        // FIXME:
        throw null;
    }

    public void encode(Record rec, Object v) {
        // FIXME:
        throw null;
    }
}
