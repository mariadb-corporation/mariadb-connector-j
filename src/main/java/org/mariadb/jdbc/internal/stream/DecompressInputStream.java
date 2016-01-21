/*
MariaDB Client for Java

Copyright (c) 2016 MariaDB.

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

import org.mariadb.jdbc.internal.util.buffer.ReadUtil;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidParameterException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class DecompressInputStream extends InputStream {
    InputStream baseStream;
    int remainingBytes;
    byte[] header;
    boolean doDecompress;
    ByteArrayInputStream decompressedByteStream;

    public DecompressInputStream(InputStream baseStream) {
        this.baseStream = baseStream;
        header = new byte[7];
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if (len == 0 || off < 0 || bytes == null) {
            throw new InvalidParameterException();
        }

        if (remainingBytes == 0) {
            nextPacket();
        }

        int ret;
        int bytesToRead = Math.min(remainingBytes, len);
        if (doDecompress) {
            ret = decompressedByteStream.read(bytes, off, bytesToRead);
        } else {
            ret = baseStream.read(bytes, off, bytesToRead);
        }
        if (ret <= 0) {
            throw new EOFException("got " + ret + " bytes, bytesToRead = " + bytesToRead);
        }

        remainingBytes -= ret;
        return ret;
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        return read(bytes, 0, bytes.length);
    }

    @Override
    public int read() throws IOException {
        byte[] buffer = new byte[1];
        if (read(buffer) == 0) {
            return -1;
        }
        return (buffer[0] & 0xff);
    }


    /**
     * Read stream header. If required, decompress compressed stream.
     *
     * @throws IOException exception
     */
    private void nextPacket() throws IOException {
        ReadUtil.readFully(baseStream, header);
        int compressedLength = (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);
        int decompressedLength = (header[4] & 0xff) + ((header[5] & 0xff) << 8) + ((header[6] & 0xff) << 16);
        if (decompressedLength != 0) {
            doDecompress = true;
            remainingBytes += decompressedLength;
            byte[] compressedBuffer = new byte[compressedLength];
            byte[] decompressedBuffer = new byte[decompressedLength];
            ReadUtil.readFully(baseStream, compressedBuffer);
            Inflater inflater = new Inflater();
            inflater.setInput(compressedBuffer);
            try {
                int actualUncompressBytes = inflater.inflate(decompressedBuffer);
                if (actualUncompressBytes != decompressedLength) {
                    throw new IOException("Invalid stream length after decompression " + actualUncompressBytes + ",expected "
                            + decompressedLength);
                }
            } catch (DataFormatException dfe) {
                throw new IOException(dfe);
            }
            inflater.end();
            decompressedByteStream = new ByteArrayInputStream(decompressedBuffer);

        } else {
            doDecompress = false;
            remainingBytes += compressedLength;
            decompressedByteStream = null;
        }
    }

}
