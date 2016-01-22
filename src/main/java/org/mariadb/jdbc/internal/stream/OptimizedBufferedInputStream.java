/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson, Trond Norbye

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.stream;

import java.io.IOException;
import java.io.InputStream;

public class OptimizedBufferedInputStream extends InputStream {

    private static final int BUFFER_SIZE = 32768;
    private final byte[] buffer = new byte[BUFFER_SIZE];
    private final InputStream stream;
    protected int count;
    protected int position;

    /**
     * Optimized bufferedInputStream for inputStream.
     * This inputStream is NOT synchronized since locking is done by reentrantLock before that.
     *
     * @param stream underlyingStream
     */
    public OptimizedBufferedInputStream(InputStream stream) {
        this.stream = stream;
    }

    @Override
    public int read() throws IOException {
        throw new IOException("Not implemented");
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if (count <= 0) {
            if (len >= BUFFER_SIZE) {
                return stream.read(bytes, off, len);
            }
            readFromUnderLyingStream(len);
            if (count <= 0) {
                return -1;
            }
        }

        int readSize = (count < len) ? count : len;
        System.arraycopy(buffer, position, bytes, off, readSize);
        position += readSize;
        count -= readSize;
        return readSize;
    }

    private void readFromUnderLyingStream(int readMinLength) throws IOException {
        position = 0;
        count = 0;

        int availableLength = stream.available();
        int maxReadLength = (availableLength > BUFFER_SIZE) ? BUFFER_SIZE : ((availableLength > readMinLength) ? availableLength : readMinLength);

        int read = stream.read(buffer, position, maxReadLength);
        if (read > 0) {
            count = read;
        }
    }

    @Override
    public long skip(long length) throws IOException {
        if (count <= 0) {
            readFromUnderLyingStream((int) length);
            if (count <= 0) {
                return 0;
            }
        }

        long skipLength = (count < length) ? count : length;
        position += skipLength;
        count -= skipLength;
        return skipLength;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    //Not needed inputStream capabilities
    @Override
    public boolean markSupported() {
        return false;
    }


    @Override
    public int available() throws IOException {
        throw new IOException("Not implemented");
    }
}