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

/**
 * 
 *
 * @author Brian S O'Neill
 */
public abstract class Encoder {
    public abstract int getMinLength();

    public abstract int getMaxRange();

    public abstract void encode(Record rec, boolean v);

    public abstract void encode(Record rec, byte v);

    public abstract void encode(Record rec, char v);

    public abstract void encode(Record rec, short v);

    public abstract void encode(Record rec, int v);

    public abstract void encode(Record rec, float v);

    public abstract void encode(Record rec, long v);

    public abstract void encode(Record rec, double v);

    public abstract void encode(Record rec, BigInteger v);

    public abstract void encode(Record rec, BigDecimal v);

    public abstract void encode(Record rec, String v);

    public abstract void encode(Record rec, Boolean v);

    public abstract void encode(Record rec, Byte v);

    public abstract void encode(Record rec, Character v);

    public abstract void encode(Record rec, Short v);

    public abstract void encode(Record rec, Integer v);

    public abstract void encode(Record rec, Float v);

    public abstract void encode(Record rec, Long v);

    public abstract void encode(Record rec, Double v);

    public abstract void encode(Record rec, boolean[] v);

    public abstract void encode(Record rec, byte[] v);

    public abstract void encode(Record rec, char[] v);

    public abstract void encode(Record rec, short[] v);

    public abstract void encode(Record rec, int[] v);

    public abstract void encode(Record rec, float[] v);

    public abstract void encode(Record rec, long[] v);

    public abstract void encode(Record rec, double[] v);

    public abstract <T> void encode(Record rec, T[] v, Class<T> t);

    public abstract <T> void encode(Record rec, List<T> v, Class<T> t);

    public abstract <T> void encode(Record rec, Set<T> v, Class<T> t);

    public abstract <K, V> void encode(Record rec, Map<K, V> v, Class<K> kt, Class<V> vt);

    public abstract void encode(Record rec, Object v);
}
