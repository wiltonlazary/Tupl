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

/**
 * Scans through all entries in a view, similar to a {@link Spliterator Spliterator}, except
 * scanned entries can also be updated. An Updater implementation which performs pre-fetching
 * can be more efficient than a {@link Cursor cursor}. Any exception thrown by an updating
 * action automatically closes the Updater.
 *
 * <p>Updater instances can only be safely used by one thread at a time, and they must be
 * closed when no longer needed. Instances can be exchanged by threads, as long as a
 * happens-before relationship is established. Without proper exclusion, multiple threads
 * interacting with a Updater instance may cause database corruption.
 *
 * @author Brian S O'Neill
 * @see View#newUpdater View.newUpdater
 * @see Scanner
 */
public interface Updater extends Splittable<Updater>, AutoCloseable {
    /**
     * Special value returned by an updating action to signal that no change should be made.
     */
    public static final byte[] NO_CHANGE = new byte[0];

    /**
     * Step forward and apply the given updating action for just one entry, unless none remain.
     */
    boolean step(EntryFunction updateAction) throws IOException;

    /**
     * Applies the given updating action for each remaining entry, and then closes the updater.
     */
    default void forEach(EntryFunction action) throws IOException {
        while (step(action));
    }

    /**
     * Attempt to split the remaining set of entries between this updater and a new one.
     */
    @Override
    default Updater trySplit() throws IOException {
        return null;
    }

    /**
     * Returns a new scanner which applies the given update action for each visited entry.
     */
    default Scanner scanner(EntryFunction updateAction) {
        return new Scanner() {
            @Override
            public boolean step(EntryConsumer action) throws IOException {
                return Updater.this.step((key, value) -> {
                    byte[] newValue = updateAction.apply(key, value);
                    action.accept(key, newValue == NO_CHANGE ? value : newValue);
                    return newValue;
                });
            }

            @Override
            public Scanner trySplit() throws IOException {
                Updater split = Updater.this.trySplit();
                return split == null ? null : split.scanner(updateAction);
            }

            @Override
            public long estimateSize() throws IOException {
                return Updater.this.estimateSize();
            }

            @Override
            public long getExactSizeIfKnown() throws IOException {
                return Updater.this.getExactSizeIfKnown();
            }

            @Override
            public int characteristics() {
                return Updater.this.characteristics();
            }

            @Override
            public boolean hasCharacteristics(int characteristics) {
                return Updater.this.hasCharacteristics(characteristics);
            }

            @Override
            public Comparator<byte[]> getComparator() {
                return Updater.this.getComparator();
            }

            @Override
            public void close() {
                Updater.this.close();
            }
        };
    }
}
