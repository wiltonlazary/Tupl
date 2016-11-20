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
import java.util.Objects;

/**
 * Represents an operation that applies a key and value, just like a {@link
 * java.util.function.BiFunction BiFunction}.
 *
 * @author Brian S O'Neill
 * @see Updater
 */
@FunctionalInterface
public interface EntryFunction {
    byte[] apply(byte[] key, byte[] value) throws IOException;

    default EntryFunction andThen(EntryFunction after) {
        Objects.requireNonNull(after);

        return (key, value) -> {
            return after.apply(key, apply(key, value));
        };
    }
}
