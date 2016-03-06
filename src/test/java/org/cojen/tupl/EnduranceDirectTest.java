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

import org.junit.Before;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class EnduranceDirectTest extends EnduranceTest {
    public static void main(String[] args) {
        org.junit.runner.JUnitCore.main(EnduranceDirectTest.class.getName());
    }

    @Before
    @Override
    public void createTempDb() throws Exception {
        mDb = TestUtils.newTempDatabase(new DatabaseConfig().pageSize(2048)
                                        .minCacheSize(1_000_000)
                                        .maxCacheSize(1_000_000)    // cacheSize ~ 500 nodes
                                        .durabilityMode(DurabilityMode.NO_FLUSH)
                                        .directPageAccess(true));
        mIx = mDb.openIndex("test");
    }
}