/*
 *  Copyright 2016 Cojen.org
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

package org.cojen.tupl;

import java.io.IOException;

import java.util.Comparator;
import java.util.Spliterator;

/**
 * Base interface for Scanners and Updaters.
 *
 * @author Brian S O'Neill
 */
interface Splittable<S> extends AutoCloseable {
    /**
     * Attempt to split the remaining set of entries between this scanner and a new one.
     */
    public default S trySplit() throws IOException {
        return null;
    }

    public default long estimateSize() throws IOException {
        return Long.MAX_VALUE;
    }

    public default long getExactSizeIfKnown() throws IOException {
        return (characteristics() & Spliterator.SIZED) == 0 ? -1L : estimateSize();
    }

    public default int characteristics() {
        return 0;
    }

    public default boolean hasCharacteristics(int characteristics) {
        return (characteristics() & characteristics) == characteristics;
    }

    public default Comparator<byte[]> getComparator() {
        throw new IllegalStateException();
    }

    @Override
    public void close();
}
