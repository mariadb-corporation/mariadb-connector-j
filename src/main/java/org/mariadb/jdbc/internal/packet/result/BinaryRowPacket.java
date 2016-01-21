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

import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.queryresults.resultset.value.ValueObject;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.util.buffer.Reader;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.queryresults.resultset.value.MariaDbValueObject;

import java.io.IOException;
import java.nio.ByteBuffer;


public class BinaryRowPacket implements RowPacket {
    private final ColumnInformation[] columnInformation;
    private final Options options;
    private final int columnInformationLength;

    /**
     * Constructor.
     * @param columnInformation column information.
     * @param options options
     * @param columnInformationLength number of columns
     */
    public BinaryRowPacket(ColumnInformation[] columnInformation, Options options, int columnInformationLength) {
        this.columnInformation = columnInformation;
        this.options = options;
        this.columnInformationLength = columnInformationLength;
    }

    /**
     * Fetch stream to retrieve data. Data length is unknown.
     * @param reader reader
     * @param packetFetcher packetFetcher
     * @throws IOException if a connection error occur
     */
    public void appendPacketIfNeeded(Reader reader, ReadPacketFetcher packetFetcher) throws IOException {
        long encLength = reader.getSilentLengthEncodedBinary();
        long remaining = reader.getRemainingSize();
        while (encLength > remaining) {
            reader.appendPacket(packetFetcher.getRawPacket());
            encLength = reader.getSilentLengthEncodedBinary();
            remaining = reader.getRemainingSize();
        }
    }

    /**
     * Fetch stream to retrieve data. Data length is known.
     * @param reader reader
     * @param packetFetcher packetFetcher
     * @param encLength data binary length
     * @throws IOException if a connection error occur
     */
    public void appendPacketIfNeeded(Reader reader, ReadPacketFetcher packetFetcher, long encLength) throws IOException {
        long remaining = reader.getRemainingSize();
        while (encLength > remaining) {
            reader.appendPacket(packetFetcher.getRawPacket(), encLength);
            encLength = reader.getSilentLengthEncodedBinary();
        }
    }

    /**
     * Get next row data.
     * @param packetFetcher packetFetcher
     * @param buffer current buffer
     * @return read data
     * @throws IOException if any connection error occur
     */
    public ValueObject[] getRow(ReadPacketFetcher packetFetcher, ByteBuffer buffer) throws IOException {
        ValueObject[] valueObjects = new ValueObject[columnInformationLength];
        Reader reader = new Reader(buffer);
        reader.skipByte(); //stream header
        int nullCount = (columnInformationLength + 9) / 8;
        byte[] nullBitsBuffer = reader.readRawBytes(nullCount);

        for (int i = 0; i < columnInformationLength; i++) {
            if ((nullBitsBuffer[(i + 2) / 8] & (1 << ((i + 2) % 8))) > 0) {
                //field is null
                valueObjects[i] = new MariaDbValueObject(null, columnInformation[i], true, options);
            } else {
                switch (columnInformation[i].getType()) {
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
                        appendPacketIfNeeded(reader, packetFetcher);
                        valueObjects[i] = new MariaDbValueObject(reader.getLengthEncodedBytes(), columnInformation[i], true, options);
                        break;

                    case BIGINT:
                        appendPacketIfNeeded(reader, packetFetcher, 8);
                        valueObjects[i] = new MariaDbValueObject(reader.getLengthEncodedBytesWithLength(8), columnInformation[i], true, options);
                        break;

                    case INTEGER:
                    case MEDIUMINT:
                        appendPacketIfNeeded(reader, packetFetcher, 4);
                        valueObjects[i] = new MariaDbValueObject(reader.getLengthEncodedBytesWithLength(4), columnInformation[i], true, options);
                        break;

                    case SMALLINT:
                    case YEAR:
                        appendPacketIfNeeded(reader, packetFetcher, 2);
                        valueObjects[i] = new MariaDbValueObject(reader.getLengthEncodedBytesWithLength(2), columnInformation[i], true, options);
                        break;

                    case TINYINT:
                        appendPacketIfNeeded(reader, packetFetcher, 1);
                        valueObjects[i] = new MariaDbValueObject(reader.getLengthEncodedBytesWithLength(1), columnInformation[i], true, options);
                        break;

                    case DOUBLE:
                        appendPacketIfNeeded(reader, packetFetcher, 8);
                        valueObjects[i] = new MariaDbValueObject(reader.getLengthEncodedBytesWithLength(8), columnInformation[i], true, options);
                        break;

                    case FLOAT:
                        appendPacketIfNeeded(reader, packetFetcher, 4);
                        valueObjects[i] = new MariaDbValueObject(reader.getLengthEncodedBytesWithLength(4), columnInformation[i], true, options);
                        break;

                    case TIME:
                    case DATE:
                    case DATETIME:
                    case TIMESTAMP:
                        appendPacketIfNeeded(reader, packetFetcher);
                        valueObjects[i] = new MariaDbValueObject(reader.getLengthEncodedBytes(), columnInformation[i], true, options);
                        break;
                    default:
                        appendPacketIfNeeded(reader, packetFetcher);
                        valueObjects[i] = new MariaDbValueObject(null, columnInformation[i], true, options);
                        break;
                }
            }
        }
        return valueObjects;
    }

}