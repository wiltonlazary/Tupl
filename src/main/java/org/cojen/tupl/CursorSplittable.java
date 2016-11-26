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

import java.util.Arrays;
import java.util.Comparator;

/**
 * Base class for Scanners and Updaters.
 *
 * @author Brian S O'Neill
 */
abstract class CursorSplittable<S> implements Splittable<S> { 
    protected View mView;
    protected Cursor mCursor;

    /**
     * @param cursor unpositioned cursor
     */
    CursorSplittable(View view, Cursor cursor) throws IOException {
        mView = view;
        mCursor = cursor;
        cursor.first();
    }

    protected CursorSplittable(Cursor cursor, View view) {
        mView = view;
        mCursor = cursor;
    }

    @Override
    public S trySplit() throws IOException {
        try {
            View view = mView;

            if (view.getOrdering() == Ordering.UNSPECIFIED) {
                return null;
            }

            Cursor cursor = mCursor;
            Cursor highCursor = view.newCursor(cursor.link());
            highCursor.autoload(false);
            highCursor.random(cursor.key(), null);

            byte[] highKey = highCursor.key();

            if (highKey == null || Arrays.equals(highKey, cursor.key())) {
                highCursor.reset();
                return null;
            }

            S highSplittable = newSplittable(highCursor, new BoundedView(view, highKey, null, 0));

            if (cursor.autoload()) {
                highCursor.autoload(true);
                highCursor.load();
            }

            if (cursor instanceof BoundedCursor) {
                BoundedCursor boundedCursor = ((BoundedCursor) cursor);
                view = boundedCursor.mView;
                cursor = boundedCursor.mSource;
            } 

            BoundedView lowView = new BoundedView(view, null, highKey, BoundedView.END_EXCLUSIVE);
            BoundedCursor lowCursor = new BoundedCursor(lowView, cursor);

            mView = lowView;
            mCursor = lowCursor;

            return highSplittable;
        } catch (Throwable e) {
            throw fail(e);
        }
    }

    @Override
    public long estimateSize() throws IOException {
        return mView.estimateSize(null, null, 1);
    }

    @Override
    public long getExactSizeIfKnown() {
        return -1;
    }

    @Override
    public int characteristics() {
        return mView.characteristics();
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return mView.getComparator();
    }

    @Override
    public void close() {
        mCursor.reset();
    }

    protected RuntimeException fail(Throwable e) {
        try {
            close();
        } catch (Throwable e2) {
            e.addSuppressed(e2);
        }
        throw Utils.rethrow(e);
    }

    protected abstract S newSplittable(Cursor cursor, View view);
}
