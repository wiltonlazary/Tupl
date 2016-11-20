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

import java.util.function.BiFunction;
import java.util.function.Consumer;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Scans through all entries in a view, similar to a {@link Spliterator Spliterator}. A
 * Scanner implementation which performs pre-fetching can be more efficient than a {@link
 * Cursor cursor}. Any exception thrown by a scan action automatically closes the Scanner.
 *
 * <p>Scanner instances can only be safely used by one thread at a time, and they must be
 * closed when no longer needed. Instances can be exchanged by threads, as long as a
 * happens-before relationship is established. Without proper exclusion, multiple threads
 * interacting with a Scanner instance may cause database corruption.
 *
 * @author Brian S O'Neill
 * @see View#newScanner View.newScanner
 * @see Updater
 */
public interface Scanner extends Splittable<Scanner>, AutoCloseable {
    /**
     * Step forward and call the given action for just one entry, unless none remain.
     */
    boolean step(EntryConsumer action) throws IOException;

    /**
     * Calls the given action for each entry, and then closes the scanner.
     */
    default void forEach(EntryConsumer action) throws IOException {
        while (step(action));
    }

    /**
     * Attempt to split the remaining set of entries between this scanner and a new one.
     */
    @Override
    default Scanner trySplit() throws IOException {
        return null;
    }

    /**
     * Returns a new spliterator backed by this scanner. Any checked exceptions are rethrown as
     * unchecked.
     *
     * @param adapter converts view entries to objects
     */
    default <T> Spliterator<T> spliterator(BiFunction<byte[], byte[], T> adapter) {
        return new Spliterator<T>() {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                try {
                    return Scanner.this.step((k, v) -> action.accept(adapter.apply(k, v)));
                } catch (IOException e) {
                    throw Utils.rethrow(e);
                }
            }

            @Override
            public Spliterator<T> trySplit() {
                try {
                    Scanner split = Scanner.this.trySplit();
                    return split == null ? null : split.spliterator(adapter);
                } catch (IOException e) {
                    throw Utils.rethrow(e);
                }
            }

            @Override
            public long estimateSize() {
                try {
                    return Scanner.this.estimateSize();
                } catch (IOException e) {
                    throw Utils.rethrow(e);
                }
            }

            @Override
            public long getExactSizeIfKnown() {
                try {
                    return Scanner.this.getExactSizeIfKnown();
                } catch (IOException e) {
                    throw Utils.rethrow(e);
                }
            }

            @Override
            public int characteristics() {
                // Cannot be considered sorted because the adapted object doesn't define the
                // sort order of the underlying view.
                return Scanner.this.characteristics() & ~Spliterator.SORTED;
            }

            @Override
            public boolean hasCharacteristics(int characteristics) {
                return Scanner.this.hasCharacteristics(characteristics);
            }
        };
    }

    /**
     * Returns a new stream backed by this scanner. Any checked exceptions are rethrown as
     * unchecked.
     *
     * @param adapter converts view entries to objects
     */
    default <T> Stream<T> stream(BiFunction<byte[], byte[], T> adapter) {
        return StreamSupport.stream(spliterator(adapter), false);
    }

    /**
     * Returns a new parallel stream backed by this scanner. Any checked exceptions are
     * rethrown as unchecked.
     *
     * @param adapter converts view entries to objects
     */
    default <T> Stream<T> parallelStream(BiFunction<byte[], byte[], T> adapter) {
        return StreamSupport.stream(spliterator(adapter), true);
    }
}
