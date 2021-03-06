/*
 *  Copyright 2015 Cojen.org
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

import java.io.*;
import java.util.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class PageAccessTransformer {
    public static void main(String[] args) throws Exception {
        File src = new File(args[0]);
        File dst = new File(args[1]);
        PageAccessTransformer pa = new PageAccessTransformer(src, dst);
        pa.findFiles();
        pa.transform();
    }

    private static final int STATE_NORMAL = 0, STATE_DISABLE = 1, STATE_ENABLE = 2;

    private int mState;

    private final Pattern mDisablePattern = Pattern.compile("\\s+");

    private final File mSrc;
    private final File mDst;

    private Map<String, Pattern> mNames;

    PageAccessTransformer(File src, File dst) {
        dirCheck(src);
        dirCheck(dst);
        mSrc = src;
        mDst = dst;
    }

    private static void dirCheck(File dir) {
        if (dir.exists() && !dir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + dir);
        }
    }

    void findFiles() throws IOException {
        Map<String, Pattern> names = new HashMap<>();

        File[] files = mSrc.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isFile() && requiresTransform(f)) {
                    String name = f.getName();
                    name = name.substring(0, name.length() - 5);
                    names.put(name, Pattern.compile("\\b" + name + "\\b"));
                }
            }
        }

        mNames = names;
    }

    /**
     * @return set of generated file names
     */
    Collection<String> transform() throws IOException {
        List<String> all = new ArrayList<>(mNames.size());

        for (String name : mNames.keySet()) {
            String newName = "_" + name + ".java";
            transform(new File(mSrc, name + ".java"), new File(mDst, newName));
            all.add(newName);
        }

        return all;
    }

    private void transform(File src, File dst) throws IOException {
        try (BufferedReader in = new BufferedReader(new FileReader(src))) {
            dst.getParentFile().mkdirs();
            try (BufferedWriter out = new BufferedWriter(new FileWriter(dst))) {
                mState = STATE_NORMAL;
                String line;
                while ((line = in.readLine()) != null) {
                    int index = line.indexOf("@author ");
                    if (index > 0) {
                        line = line.substring(0, index + 8) +
                            "Generated by PageAccessTransformer from " + src.getName();
                    } else {
                        line = transform(line);
                    }

                    if (line != null) {
                        out.write(line);
                        out.write('\n');
                    }
                }
            }
        }
    }

    private String transform(String line) {
        line = replaceNames(line);

        while (true) {
            int index = line.indexOf("/*P*/ ");

            if (index < 0) {
                if (mState == STATE_DISABLE) {
                    Matcher m = mDisablePattern.matcher(line);
                    if (m.find()) {
                        line = m.group() + m.replaceFirst("// ");
                    }
                } else {
                    index = line.indexOf("PageOps");
                    if (index >= 0
                        && (index < 6 || !line.regionMatches(index - 6, "Direct", 0, 6)))
                    {
                        line = line.substring(0, index) + "Direct" + line.substring(index);
                    }
                }
                return line;
            }

            int typeIndex = index + 6;

            if (line.indexOf("byte[]", typeIndex) == typeIndex) {
                line = line.substring(0, index) + "long" + line.substring(typeIndex + 6);
                continue;
            }

            if (line.indexOf("// ", typeIndex) == typeIndex) {
                int tagIndex = typeIndex + 3;
                if (tagIndex < line.length()) {
                    switch (line.charAt(tagIndex)) {
                    case '[':
                        if (mState != STATE_NORMAL) {
                            throw new IllegalStateException();
                        }
                        if (++tagIndex < line.length() && line.charAt(tagIndex) == '|') {
                            mState = STATE_ENABLE;
                        } else {
                            mState = STATE_DISABLE;
                        }
                        return line;
                    case '|':
                        if (mState != STATE_DISABLE) {
                            throw new IllegalStateException();
                        }
                        mState = STATE_ENABLE;
                        return line;
                    case ']':
                        if (mState == STATE_NORMAL) {
                            throw new IllegalStateException();
                        }
                        mState = STATE_NORMAL;
                        return line;
                    }
                }

                if (mState == STATE_ENABLE) {
                    line = line.substring(0, index) + line.substring(tagIndex);
                    continue;
                }
            }

            return line;
        }
    }

    private String replaceNames(String line) {
        for (Map.Entry<String, Pattern> e : mNames.entrySet()) {
            String line2 = e.getValue().matcher(line).replaceAll("_" + e.getKey());
            if (!line2.equals(line) && line2.indexOf("\"_") <= 0) {
                line = line2;
            }
        }
        return line;
    }

    private static boolean requiresTransform(File file) throws IOException {
        String name = file.getName();

        if (!name.endsWith(".java") ||
            name.equals("PageAccessTransformer.java") ||
            name.startsWith("_") ||
            name.endsWith("PageOps.java"))
        {
            return false;
        }

        try (BufferedReader in = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.indexOf("/*P*/") >= 0) {
                    return true;
                }
            }
        }

        return false;
    }
}
