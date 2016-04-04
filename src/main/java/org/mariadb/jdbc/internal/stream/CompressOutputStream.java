/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2015-2016 MariaDB Ab.

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

Copyright (c) 2009-2011, Marcus Eriksson

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;

public class CompressOutputStream extends OutputStream {

    private static final int MIN_COMPRESSION_SIZE = 16 * 1024;
    private static final int MAX_PACKET_LENGTH = 16 * 1024 * 1024 - 1;
    private static final float MIN_COMPRESSION_RATIO = 0.9f;

    OutputStream baseStream;
    byte[] header = new byte[7];
    int seqNo = 0;

    public CompressOutputStream(OutputStream baseStream) {
        this.baseStream = baseStream;
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        if (len > MAX_PACKET_LENGTH) {
            for (; ; ) {
                int bytesToWrite = Math.min(len, MAX_PACKET_LENGTH);
                write(bytes, off, bytesToWrite);
                off += bytesToWrite;
                len -= bytesToWrite;
                if (len == 0) {
                    return;
                }
            }
        }

        int compressedLength = len;
        int uncompressedLength = 0;

        if (bytes.length > MIN_COMPRESSION_SIZE) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DeflaterOutputStream deflater = new DeflaterOutputStream(baos);
            deflater.write(bytes, off, len);
            deflater.finish();
            deflater.close();
            byte[] compressedBytes = baos.toByteArray();
            baos.close();
            if (compressedBytes.length < (int) (MIN_COMPRESSION_RATIO * len)) {
                compressedLength = compressedBytes.length;
                uncompressedLength = len;
                bytes = compressedBytes;
                off = 0;
            }
        }

        header[0] = (byte) (compressedLength & 0xff);
        header[1] = (byte) ((compressedLength >> 8) & 0xff);
        header[2] = (byte) ((compressedLength >> 16) & 0xff);
        header[3] = (byte) seqNo++;
        header[4] = (byte) (uncompressedLength & 0xff);
        header[5] = (byte) ((uncompressedLength >> 8) & 0xff);
        header[6] = (byte) ((uncompressedLength >> 16) & 0xff);

        baseStream.write(header);
        baseStream.write(bytes, off, compressedLength);

    }

    @Override
    public void write(byte[] bytes) throws IOException {
        if (bytes.length < 3) {
            throw new AssertionError("Invalid call, at least 3 byte writes are required");
        }
        write(bytes, 0, bytes.length);

    }

    @Override
    public void write(int bytes) throws IOException {
        throw new AssertionError("Invalid call, at least 3 byte writes are required");
    }

    @Override
    public void flush() throws IOException {
        baseStream.flush();
        seqNo = 0;
    }
}
