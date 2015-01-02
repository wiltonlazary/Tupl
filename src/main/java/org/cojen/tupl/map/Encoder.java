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

    public abstract void init();

    // Note: Array might be replaced if not large enough.
    public abstract void init(byte[] dst, int off);

    public abstract int getLength();

    public abstract byte[] finish();

    public abstract void encode(boolean v);

    public abstract void encode(byte v);

    public abstract void encode(char v);

    public abstract void encode(short v);

    public abstract void encode(int v);

    public abstract void encode(float v);

    public abstract void encode(long v);

    public abstract void encode(double v);

    public abstract void encode(BigInteger v);

    public abstract void encode(BigDecimal v);

    public abstract void encode(String v);

    public abstract void encode(Boolean v);

    public abstract void encode(Byte v);

    public abstract void encode(Character v);

    public abstract void encode(Short v);

    public abstract void encode(Integer v);

    public abstract void encode(Float v);

    public abstract void encode(Long v);

    public abstract void encode(Double v);

    public abstract void encode(boolean[] v);

    public abstract void encode(byte[] v);

    public abstract void encode(char[] v);

    public abstract void encode(short[] v);

    public abstract void encode(int[] v);

    public abstract void encode(float[] v);

    public abstract void encode(long[] v);

    public abstract void encode(double[] v);

    public abstract <T> void encode(T[] v, Class<T> t);

    public abstract <T> void encode(List<T> v, Class<T> t);

    public abstract <T> void encode(Set<T> v, Class<T> t);

    public abstract <K, V> void encode(Map<K, V> v, Class<K> kt, Class<V> vt);

    public abstract void encode(Object v);
}
