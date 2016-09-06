/*
 *  Copyright 2012-2015 Cojen.org
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

package org.cojen.tupl.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.EnumSet;

import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import com.sun.jna.Native;
import com.sun.jna.Platform;

/**
 * Lowest I/O interface to a file or device.
 *
 * @author Brian S O'Neill
 */
public abstract class FileIO implements CauseCloseable {
    private static final String USE_JNA = FileIO.class.getName() + ".useJNA";
    private static final int IO_TYPE; // 0: platform independent, 1: POSIX
    private static final boolean NEEDS_DIR_SYNC;

    static {
        int type = 0;

        String jnaProp = System.getProperty(USE_JNA, null);
        if (jnaProp == null || Boolean.parseBoolean(jnaProp)) {
            try {
                if (Native.SIZE_T_SIZE >= 8 && !Platform.isWindows()) {
                    type = 1;
                }
            } catch (Throwable e) {
                if (jnaProp != null) {
                    throw e;
                }
            }
        }

        IO_TYPE = type;

        NEEDS_DIR_SYNC = !System.getProperty("os.name").startsWith("Windows");
    }

    public static FileIO open(File file, EnumSet<OpenOption> options)
        throws IOException
    {
        return open(file, options, 32);
    }

    public static FileIO open(File file, EnumSet<OpenOption> options, int openFileCount)
        throws IOException
    {
        if (options == null) {
            options = EnumSet.noneOf(OpenOption.class);
        }
        if (IO_TYPE == 1 && (!options.contains(OpenOption.MAPPED) || DirectAccess.isSupported())) {
            return new PosixFileIO(file, options);
        }
        return new JavaFileIO(file, options, openFileCount);
    }

    public abstract boolean isReadOnly();

    /**
     * Preallocates blocks to the file. This call ensures that disk space is allocated 
     * for this file for the bytes in the range starting at offset and continuing for 
     * length bytes.  Subsequent writes to the specified range are guaranteed not to 
     * fail because of lack of disk space.
     *
     * @param pos zero-based position in file.
     * @param length amount of bytes to preallocate starting at pos.
     * @throws IllegalArgumentException
     */
    abstract void preallocate(long pos, long length) throws IOException;

    public abstract long length() throws IOException;

    /**
     * Attempt to set the length of the file. It isn't critical that the
     * operation succeed, and so any exceptions can be suppressed.
     */
    public abstract void setLength(long length) throws IOException;

    /**
     * @param pos zero-based position in file
     * @param buf receives read data
     * @param offset offset into data buffer
     * @param length amount of data to read
     * @throws IllegalArgumentException
     */
    public abstract void read(long pos, byte[] buf, int offset, int length) throws IOException;

    /**
     * @param pos zero-based position in file
     * @param bb receives read data
     * @throws IllegalArgumentException
     */
    public abstract void read(long pos, ByteBuffer bb) throws IOException;

    public void read(long pos, long ptr, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * @param pos zero-based position in file
     * @param buf data to write
     * @param offset offset into data buffer
     * @param length amount of data
     * @throws IllegalArgumentException
     */
    public abstract void write(long pos, byte[] buf, int offset, int length) throws IOException;

    /**
     * @param pos zero-based position in file
     * @param bb data to write
     * @throws IllegalArgumentException
     */
    public abstract void write(long pos, ByteBuffer bb) throws IOException;

    public void write(long pos, long ptr, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Maps or remaps the file into main memory, up to the current file length.
     */
    public abstract void map() throws IOException;

    /**
     * If file is mapped, remaps it if the file length has changed. Method does nothing if not
     * already mapped.
     */
    public abstract void remap() throws IOException;

    /**
     * Unmaps the file from main memory, leaving the file open. Method does nothing if not
     * already mapped.
     */
    public abstract void unmap() throws IOException;

    /**
     * Durably flushes all writes to the underlying device.
     *
     * @param metadata pass true to flush all file metadata
     */
    public abstract void sync(boolean metadata) throws IOException;

    /**
     * Durably flushes the given directory, if required. If the given file is not a directory,
     * the parent directory is flushed.
     */
    public static void dirSync(File file) throws IOException {
        if (!NEEDS_DIR_SYNC) {
            return;
        }

        while (!file.isDirectory()) {
            file = file.getParentFile();
            if (file == null) {
                return;
            }
        }

        if (IO_TYPE == 1) {
            int fd = PosixFileIO.openFd(file, EnumSet.of(OpenOption.READ_ONLY));
            try {
                PosixFileIO.fsyncFd(fd);
            } finally {
                PosixFileIO.closeFd(fd);
            }
        } else {
            FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ);
            try {
                fc.force(true);
            } finally {
                fc.close();
            }
        }
    }
}
