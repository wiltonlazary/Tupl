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

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ViewScanner extends CursorSplittable<Scanner> implements Scanner {
    /**
     * @param cursor unpositioned cursor
     */
    ViewScanner(View view, Cursor cursor) throws IOException {
        super(view, cursor);
    }

    private ViewScanner(Cursor cursor, View view) {
        super(cursor, view);
    }

    @Override
    public boolean step(EntryConsumer action) throws IOException {
        try {
            Cursor c = mCursor;
            byte[] key = c.key();
            if (key == null) {
                return false;
            }
            action.accept(key, c.value());
            c.next();
            return true;
        } catch (Throwable e) {
            throw fail(e);
        }
    }

    @Override
    protected ViewScanner newSplittable(Cursor cursor, View view) {
        return new ViewScanner(cursor, view);
    }
}
