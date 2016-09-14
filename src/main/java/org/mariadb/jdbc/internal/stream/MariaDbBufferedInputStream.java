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

import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.util.Utils;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class MariaDbBufferedInputStream extends BufferedInputStream implements MariaDbInputStream {
    private static Logger logger = LoggerFactory.getLogger(MariaDbBufferedInputStream.class);
    private int lastPacketSeq;
    private byte[] headerBuffer = new byte[4];

    public MariaDbBufferedInputStream(InputStream in, int size) {
        super(in, size);
    }

    /**
     * Permit to return mysql packet header length super fast if already in cache.
     * (no System.arraycopy)
     * @return headerLength.
     * @throws IOException id stream throw error
     */
    public int readHeader() throws IOException {
        int avail = count - pos;
        if (avail >= 4) {
            int returnValue = (buf[pos] & 0xff) + ((buf[pos + 1] & 0xff) << 8) + ((buf[pos + 2] & 0xff) << 16);
            lastPacketSeq = buf[pos + 3] & 0xff;
            pos += 4;
            logger.trace("read packet seq:" + lastPacketSeq + " length:" + returnValue);
            return returnValue;
        }

        int read = 0;
        do {
            int count = read(headerBuffer, read, 4 - read);
            if (count <= 0) {
                throw new EOFException("unexpected end of stream, read " + read + " bytes from " + 4);
            }
            read += count;
        } while (read < 4);
        lastPacketSeq = headerBuffer[3] & 0xff;
        int length = (headerBuffer[0] & 0xff) + ((headerBuffer[1] & 0xff) << 8) + ((headerBuffer[2] & 0xff) << 16);
        logger.trace("read packet seq:" + lastPacketSeq + " length:" + length);
        return length;
    }

    @Override
    public int getLastPacketSeq() {
        return lastPacketSeq;
    }

    @Override
    public void setLastPacketSeq(int lastPacketSeq) {
        this.lastPacketSeq = lastPacketSeq;
    }
}
