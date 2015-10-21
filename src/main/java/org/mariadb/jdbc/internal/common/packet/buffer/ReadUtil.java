package org.mariadb.jdbc.internal.common.packet.buffer;
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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


public final class ReadUtil {
    private ReadUtil() {

    }

    /**
     * Read a number of bytes from the stream and store it in the buffer, and fix the problem with "incomplete" reads by
     * doing another read if we don't have all of the data yet.
     *
     * @param stream the input stream to read from
     * @param bytes      buffer where to store the data
     * @param off    offset in the buffer
     * @param len    bytes to read
     * @throws java.io.IOException if an error occurs while reading the stream.
     *                             java.io.EOFException of end of stream is hit.
     */
    public static void readFully(InputStream stream, byte[] bytes, int off, int len) throws IOException {
        long start = System.nanoTime();
        int remaining = len;
        do {
            int count = stream.read(bytes, off, remaining);
            if (count <= 0) {
                throw new EOFException("unexpected end of stream, read " + (len - remaining) + " bytes from " + len);
            }
            remaining -= count;
            off += count;
        } while (remaining > 0);

    }

    public static void readFully(InputStream stream, byte[] bytes) throws IOException {
        readFully(stream, bytes, 0, bytes.length);
    }


    /**
     * Checks whether the next packet is EOF.
     *
     * @param byteBuffer the raw packet
     * @return true if the packet is an EOF packet
     */
    public static boolean eofIsNext(ByteBuffer byteBuffer) {
        return (byteBuffer.get(0) == (byte) 0xfe && byteBuffer.limit() < 9);

    }

    public static boolean isErrorPacket(ByteBuffer byteBuffer) {
        return byteBuffer.get(0) == (byte) 0xff;
    }
}