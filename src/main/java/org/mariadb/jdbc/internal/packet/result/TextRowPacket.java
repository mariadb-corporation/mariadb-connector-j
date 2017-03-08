/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

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

package org.mariadb.jdbc.internal.packet.result;

import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.stream.MariaDbInputStream;
import org.mariadb.jdbc.internal.util.buffer.Buffer;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;


public class TextRowPacket implements RowPacket {
    private final int columnInformationLength;

    /**
     * Constructor.
     *
     * @param columnInformationLength number of column
     */
    public TextRowPacket(int columnInformationLength) {
        this.columnInformationLength = columnInformationLength;
    }

    /**
     * Read text row stream. (to fetch Resulset.next() datas)
     *
     * @param packetFetcher packetFetcher
     * @param buffer        current buffer
     * @return datas object
     * @throws IOException if any connection error occur
     */
    public byte[][] getRow(ReadPacketFetcher packetFetcher, Buffer buffer) throws IOException {

        byte[][] valueObjects = new byte[columnInformationLength][];
        for (int i = 0; i < columnInformationLength; i++) {
            while (buffer.remaining() == 0) {
                buffer.appendPacket(packetFetcher.getPacket());
            }
            long valueLen = buffer.getLengthEncodedBinary();
            if (valueLen == -1) {
                valueObjects[i] = null;
            } else {
                if (buffer.remaining() < valueLen) {
                    buffer.grow((int) valueLen);
                    while (buffer.remaining() < valueLen) {
                        buffer.appendPacket(packetFetcher.getPacket());
                    }
                }
                valueObjects[i] = buffer.readRawBytes((int) valueLen);
            }
        }
        return valueObjects;
    }

    /**
     * Read text row stream. (to fetch Resulset.next() datas)
     *
     * @param packetFetcher packetFetcher
     * @param inputStream inputStream
     * @return datas object
     * @throws IOException if any connection error occur
     */
    public byte[][] getRow(ReadPacketFetcher packetFetcher, MariaDbInputStream inputStream, int remaining, int read) throws IOException {

        byte[][] valueObjects = new byte[columnInformationLength][];
        int position = 0;
        int toReadLen;

        while (true) {
            switch (read) {
                case 251:
                    toReadLen = -1;
                    break;
                case 252:
                    toReadLen = ((inputStream.read() & 0xff) + ((inputStream.read() & 0xff) << 8));
                    remaining -= 2;
                    break;
                case 253:
                    toReadLen = (inputStream.read() & 0xff)
                            + ((inputStream.read() & 0xff) << 8)
                            + ((inputStream.read() & 0xff) << 16);
                    remaining -= 3;
                    break;
                case 254:
                    toReadLen = (int) (((inputStream.read() & 0xff)
                            + ((long) (inputStream.read() & 0xff) << 8)
                            + ((long) (inputStream.read() & 0xff) << 16)
                            + ((long) (inputStream.read() & 0xff) << 24)
                            + ((long) (inputStream.read() & 0xff) << 32)
                            + ((long) (inputStream.read() & 0xff) << 40)
                            + ((long) (inputStream.read() & 0xff) << 48)
                            + ((long) (inputStream.read() & 0xff) << 56)));
                    remaining -= 8;
                    break;
                default:
                    toReadLen = read;
            }
            if (toReadLen == -1) {
                valueObjects[position++] = null;
            } else if (toReadLen == 0) {
                valueObjects[position++] = new byte[0];
            } else {
                valueObjects[position++] = packetFetcher.readLength(toReadLen);
                remaining -= toReadLen;
            }
            if (remaining <= 0) {
                break;
            }
            read = inputStream.read() & 0xff;
            remaining -= 1;
        }
        return valueObjects;
    }


}