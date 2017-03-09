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

import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.stream.MariaDbInputStream;
import org.mariadb.jdbc.internal.util.buffer.Buffer;

import java.io.IOException;


public class BinaryRowPacket extends RowPacket {

    /**
     * Constructor.
     *
     * @param columnInformations      column information.
     * @param columnInformationLength number of columns
     * @param maxFieldSize            max field size
     */
    public BinaryRowPacket(ColumnInformation[] columnInformations, int columnInformationLength, int maxFieldSize) {
        super(columnInformations, columnInformationLength, maxFieldSize);
    }

    /**
     * Fetch stream to retrieve data. Data length is unknown.
     *
     * @param buffer        buffer
     * @param packetFetcher packetFetcher
     * @return next field length
     * @throws IOException if a connection error occur
     */
    public long appendPacketIfNeeded(Buffer buffer, ReadPacketFetcher packetFetcher) throws IOException {
        long encLength = buffer.getLengthEncodedBinary();
        if (encLength > buffer.remaining()) {
            buffer.grow((int) encLength);
            while (encLength > buffer.remaining()) {
                buffer.appendPacket(packetFetcher.getPacket());
            }
        }
        return encLength;
    }

    /**
     * Fetch stream to retrieve data. Data length is known.
     *
     * @param buffer        reader
     * @param packetFetcher packetFetcher
     * @param encLength     data binary length
     * @throws IOException if a connection error occur
     */
    public void appendPacketIfNeeded(Buffer buffer, ReadPacketFetcher packetFetcher, long encLength) throws IOException {
        if (encLength > buffer.remaining()) {
            buffer.grow((int) encLength);
            while (encLength > buffer.remaining()) {
                buffer.appendPacket(packetFetcher.getPacket());
            }
        }
    }

    /**
     * Get next row data.
     *
     * @param packetFetcher packetFetcher
     * @param buffer        current buffer
     * @return read data
     * @throws IOException if any connection error occur
     */
    public byte[][] getRow(ReadPacketFetcher packetFetcher, Buffer buffer) throws IOException {
        byte[][] valueObjects = new byte[getColumnInformationLength()][];
        buffer.skipByte(); //stream header
        int nullCount = (getColumnInformationLength() + 9) / 8;
        byte[] nullBitsBuffer = buffer.readRawBytes(nullCount);

        for (int i = 0; i < getColumnInformationLength(); i++) {
            if ((nullBitsBuffer[(i + 2) / 8] & (1 << ((i + 2) % 8))) > 0) {
                //field is null
                valueObjects[i] = null;
            } else {
                switch (getColumnInformations()[i].getColumnType()) {
                    case VARCHAR:
                    case BIT:
                    case ENUM:
                    case SET:
                    case TINYBLOB:
                    case MEDIUMBLOB:
                    case LONGBLOB:
                    case BLOB:
                    case VARSTRING:
                    case STRING:
                    case GEOMETRY:
                    case OLDDECIMAL:
                    case DECIMAL:
                    case TIME:
                    case DATE:
                    case DATETIME:
                    case TIMESTAMP:
                        long length = appendPacketIfNeeded(buffer, packetFetcher);
                        if (isColumnAffectedByMaxFieldSize(getColumnInformations()[i]) && length > getMaxFieldSize()) {
                            //only part of data will be fetched, according to Statement.maxFieldSize
                            valueObjects[i] = buffer.getLengthEncodedBytesWithLength(getMaxFieldSize());
                            buffer.skipBytes((int) length - getMaxFieldSize());
                        } else {
                            valueObjects[i] = buffer.getLengthEncodedBytesWithLength(length);
                        }
                        break;

                    case BIGINT:
                        appendPacketIfNeeded(buffer, packetFetcher, 8);
                        valueObjects[i] = buffer.getLengthEncodedBytesWithLength(8);
                        break;

                    case INTEGER:
                    case MEDIUMINT:
                        appendPacketIfNeeded(buffer, packetFetcher, 4);
                        valueObjects[i] = buffer.getLengthEncodedBytesWithLength(4);
                        break;

                    case SMALLINT:
                    case YEAR:
                        appendPacketIfNeeded(buffer, packetFetcher, 2);
                        valueObjects[i] = buffer.getLengthEncodedBytesWithLength(2);
                        break;

                    case TINYINT:
                        appendPacketIfNeeded(buffer, packetFetcher, 1);
                        valueObjects[i] = buffer.getLengthEncodedBytesWithLength(1);
                        break;

                    case DOUBLE:
                        appendPacketIfNeeded(buffer, packetFetcher, 8);
                        valueObjects[i] = buffer.getLengthEncodedBytesWithLength(8);
                        break;

                    case FLOAT:
                        appendPacketIfNeeded(buffer, packetFetcher, 4);
                        valueObjects[i] = buffer.getLengthEncodedBytesWithLength(4);
                        break;
                    default:
                        appendPacketIfNeeded(buffer, packetFetcher);
                        valueObjects[i] = null;
                        break;
                }
            }
        }
        return valueObjects;
    }

    /**
     * Read text row stream. (to fetch Resulset.next() datas)
     *
     * @param packetFetcher packetFetcher
     * @param inputStream   inputStream
     * @return datas object
     * @throws IOException if any connection error occur
     */
    public byte[][] getRow(ReadPacketFetcher packetFetcher, MariaDbInputStream inputStream, int remaining, int read) throws IOException {
        byte[][] valueObjects = new byte[getColumnInformationLength()][];
        int toReadLen;
        int nullCount = (getColumnInformationLength() + 9) / 8;
        byte[] nullBitsBuffer = packetFetcher.readLength(nullCount);
        remaining -= nullCount;
        for (int i = 0; i < getColumnInformationLength(); i++) {
            if ((nullBitsBuffer[(i + 2) / 8] & (1 << ((i + 2) % 8))) > 0) {
                //field is null
                valueObjects[i] = null;
            } else {
                switch (getColumnInformations()[i].getColumnType()) {
                    case VARCHAR:
                    case BIT:
                    case ENUM:
                    case SET:
                    case TINYBLOB:
                    case MEDIUMBLOB:
                    case LONGBLOB:
                    case BLOB:
                    case VARSTRING:
                    case STRING:
                    case GEOMETRY:
                    case OLDDECIMAL:
                    case DECIMAL:
                    case TIME:
                    case DATE:
                    case DATETIME:
                    case TIMESTAMP:
                        read = inputStream.read() & 0xff;
                        remaining -= 1;
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
                            valueObjects[i] = null;
                        } else if (toReadLen == 0) {
                            valueObjects[i] = new byte[0];
                        } else {
                            if (isColumnAffectedByMaxFieldSize(getColumnInformations()[i]) && toReadLen > getMaxFieldSize()) {
                                //only part of data will be fetched, according to Statement.maxFieldSize
                                valueObjects[i] = packetFetcher.readLength(getMaxFieldSize());
                                packetFetcher.skipLength(toReadLen - getMaxFieldSize());
                            } else {
                                valueObjects[i] = packetFetcher.readLength(toReadLen);
                            }
                            remaining -= toReadLen;
                        }
                        break;

                    case BIGINT:
                    case DOUBLE:
                        valueObjects[i] = packetFetcher.readLength(8);
                        remaining -= 8;
                        break;

                    case INTEGER:
                    case MEDIUMINT:
                    case FLOAT:
                        valueObjects[i] = packetFetcher.readLength(4);
                        remaining -= 4;
                        break;

                    case SMALLINT:
                    case YEAR:
                        valueObjects[i] = packetFetcher.readLength(2);
                        remaining -= 2;
                        break;

                    case TINYINT:
                        valueObjects[i] = new byte[]{(byte) inputStream.read()};
                        remaining -= 1;
                        break;
                    default:
                        valueObjects[i] = null;
                        break;
                }
            }
        }
        return valueObjects;
    }
}