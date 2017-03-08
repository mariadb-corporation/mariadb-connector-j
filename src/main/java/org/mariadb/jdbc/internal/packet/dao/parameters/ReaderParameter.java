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

package org.mariadb.jdbc.internal.packet.dao.parameters;

import org.mariadb.jdbc.internal.packet.Packet;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ReaderParameter extends LongDataParameter {
    private static final int BUF_SIZE = 1024 * 1024 + 4; //big buffer (server reallocate array each send)
    private static final int CHAR_BUF_SIZE = 4096;
    private Reader reader;
    private long length;
    private boolean noBackslashEscapes;

    /**
     * Constructor.
     * @param reader reader to write
     * @param length max length to write (can be null)
     * @param noBackslashEscapes must backslash be escape
     */
    public ReaderParameter(Reader reader, long length, boolean noBackslashEscapes) {
        this.reader = reader;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
        if (reader.markSupported()) {
            try {
                reader.mark(1024);
            } catch (IOException e) { }
        }
    }

    public ReaderParameter(Reader reader, boolean noBackslashEscapes) {
        this(reader, Long.MAX_VALUE, noBackslashEscapes);
    }

    /**
     * Write reader to database in text format.
     *
     * @param os database outputStream
     * @throws IOException if any error occur when reading reader
     */
    public void writeTo(final PacketOutputStream os) throws IOException {
        if (length == Long.MAX_VALUE) {
            ParameterWriter.write(os, reader, noBackslashEscapes);
        } else {
            ParameterWriter.write(os, reader, length, noBackslashEscapes);
        }
    }

    /**
     * Write reader to database in text format without checking buffer size.
     *
     * @param os database outputStream
     * @throws IOException if any error occur when reading reader
     */
    public void writeUnsafeTo(final PacketOutputStream os) throws IOException {
        throw new IOException("Cannot use unsafe with Reader");
    }

    /**
     * Return approximated data calculated length for rewriting queries
     *
     * @return approximated data length.
     * @throws IOException if error reading stream
     */
    public long getApproximateTextProtocolLength() throws IOException {
        return -1;
    }

    /**
     * Send reader in one or many COM_STMT_LONG_DATA.
     * (reading is using a big buffer to avoid having a lot of packet, because server will allocate/deallocate array each send)
     *
     * @param statementId statement id
     * @param parameterId parameter number
     * @param writer      writer
     * @throws IOException if any connection exception occur
     * @throws QueryException if query size is to big according to server max_allowed_size
     */
    public void sendComLongData(int statementId, short parameterId, PacketOutputStream writer) throws IOException, QueryException {

        byte[] arr = new byte[BUF_SIZE];
        char[] charBuffer = new char[CHAR_BUF_SIZE];
        int len;
        int position;
        long remainingReadLength = length;

        while (remainingReadLength > 0) {
            writer.startPacket(0);
            //write statement id
            arr[0] = (byte) (statementId & 0xff);
            arr[1] = (byte) (statementId >>> 8);
            arr[2] = (byte) (statementId >>> 16);
            arr[3] = (byte) (statementId >>> 24);

            //write parameter number
            arr[4] = (byte) (parameterId & 0xff);
            arr[5] = (byte) (parameterId >>> 8);

            position = 6;

            len = 0;
            //write part of reader
            //will read until stream is finished, or nearly complete the buffer
            //(we cannot guess the exact size of characters in byte, but max size is * 3)
            if (length == Long.MAX_VALUE) {
                while (position + 3 * CHAR_BUF_SIZE < BUF_SIZE && (len = reader.read(charBuffer)) > 0) {
                    byte[] bytes = new String(charBuffer, 0, len).getBytes("UTF-8");
                    System.arraycopy(bytes, 0, arr, position, bytes.length);
                    position += bytes.length;
                }
            } else {
                while (position + 3 * CHAR_BUF_SIZE < BUF_SIZE
                        && (len = reader.read(charBuffer, 0, Math.min((int) remainingReadLength, CHAR_BUF_SIZE))) > 0) {
                    byte[] bytes = new String(charBuffer, 0, len).getBytes("UTF-8");
                    System.arraycopy(bytes, 0, arr, position, bytes.length);
                    position += bytes.length;
                    remainingReadLength -= len;
                }
            }

            if (position > 6) {
                writer.sendDirect(arr, 0, position, Packet.COM_STMT_SEND_LONG_DATA);
            } else {
                if (len == -1) break;
            }
        }
    }

    public MariaDbType getMariaDbType() {
        return MariaDbType.STRING;
    }


    @Override
    public String toString() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            if (reader.markSupported()) reader.reset();
            if (length == Long.MAX_VALUE) {
                ParameterWriter.write(baos, reader, noBackslashEscapes);
            } else {
                ParameterWriter.write(baos, reader, length, noBackslashEscapes);
            }
            byte[] bytes = baos.toByteArray();
            if (bytes.length < 1024) {
                return "<Buffer:" + new String(bytes, StandardCharsets.UTF_8) + ">";
            } else {
                // cut overlong strings.
                return "<Buffer:" + new String(bytes, 0, 1024, StandardCharsets.UTF_8) + "...>";
            }
        } catch (Exception e) {
            return "";
        }
    }

}
