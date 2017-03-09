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

import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.packet.Packet;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class StreamParameter extends LongDataParameter {
    private static final int BUF_SIZE = 1024 * 1024 + 4; //big buffer (server reallocate array each send)

    private InputStream is;
    private long length;
    private boolean noBackslashEscapes;

    /**
     * Constructor.
     *
     * @param is                 stream to write
     * @param length             max length to write (if null the whole stream will be send)
     * @param noBackslashEscapes must backslash be escape
     */
    public StreamParameter(InputStream is, long length, boolean noBackslashEscapes) {
        this.is = is;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
        if (is.markSupported()) is.mark(1024);
    }

    public StreamParameter(InputStream is, boolean noBackSlashEscapes) {
        this(is, Long.MAX_VALUE, noBackSlashEscapes);
    }

    /**
     * Write stream in text format.
     *
     * @param os database outputStream
     * @throws IOException if any error occur when reader stream
     */
    public void writeTo(final PacketOutputStream os) throws IOException {
        if (length == Long.MAX_VALUE) {
            ParameterWriter.write(os, is, noBackslashEscapes);
        } else {
            ParameterWriter.write(os, is, length, noBackslashEscapes);
        }
    }

    /**
     * Write stream in text format without checking buffer size.
     *
     * @param os database outputStream
     * @throws IOException if any error occur when reader stream
     */
    public void writeUnsafeTo(final PacketOutputStream os) throws IOException {
        throw new IOException("Cannot use unsafe with Stream");
    }

    /**
     * Return approximated data calculated length.
     *
     * @return approximated data length.
     * @throws IOException if error reading stream
     */
    public long getApproximateTextProtocolLength() throws IOException {
        return -1;
    }

    /**
     * Send stream in one or many COM_STMT_LONG_DATA.
     * (stream read is using a big buffer to avoid having a lot of packet, because server will allocate/deallocate array each send)
     *
     * @param statementId statement id
     * @param parameterId parameter number
     * @param writer      writer
     * @throws IOException if any connection exception occur
     * @throws SQLException if query size is to big according to server max_allowed_size
     */
    public void sendComLongData(int statementId, short parameterId, PacketOutputStream writer) throws IOException, SQLException {
        byte[] array = new byte[BUF_SIZE];
        int len;
        if (length == Long.MAX_VALUE) {
            while ((len = is.read(array, 6, BUF_SIZE - 6)) > 0) {
                sendComPacket(statementId, parameterId, writer, array, len);
            }
        } else {
            long remainingReadLength = length;
            while (remainingReadLength > 0) {
                len = is.read(array, 6, Math.min((int) remainingReadLength, BUF_SIZE - 6));
                if (len == -1) return;
                sendComPacket(statementId, parameterId, writer, array, len);
                remainingReadLength -= len;
            }
        }
    }

    private void sendComPacket(int statementId, short parameterId, PacketOutputStream writer, byte[] array, int len)
            throws IOException, SQLException {
        writer.startPacket(0);
        array[0] = (byte) (statementId & 0xff);
        array[1] = (byte) (statementId >>> 8);
        array[2] = (byte) (statementId >>> 16);
        array[3] = (byte) (statementId >>> 24);
        array[4] = (byte) (parameterId & 0xff);
        array[5] = (byte) (parameterId >>> 8);
        writer.sendDirect(array, 0, len + 6, Packet.COM_STMT_SEND_LONG_DATA);
    }


    @Override
    public String toString() {
        try {
            if (is.markSupported()) {
                is.reset();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                if (length == Long.MAX_VALUE) {
                    ParameterWriter.write(baos, is, noBackslashEscapes);
                } else {
                    ParameterWriter.write(baos, is, length, noBackslashEscapes);
                }
                byte[] bytes = baos.toByteArray();
                if (bytes.length < 1024) {
                    return "<Stream:" + new String(bytes, StandardCharsets.UTF_8) + ">";
                } else {
                    // cut overlong strings.
                    return "<Stream:" + new String(bytes, 0, 1024, StandardCharsets.UTF_8) + "...>";
                }
            } else {
                return "<Stream>";
            }
        } catch (Exception e) {
            return "";
        }
    }

    public ColumnType getMariaDbType() {
        return ColumnType.BLOB;
    }


    public boolean isNullData() {
        return false;
    }

}