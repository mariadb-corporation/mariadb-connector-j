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
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.util.Options;

import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;


public class OffsetTimeParameter implements ParameterHolder, Cloneable {
    private OffsetTime time;
    private boolean fractionalSeconds;

    /**
     * Constructor.
     *
     * @param offsetTime        time with offset
     * @param serverZoneId      server session zoneId
     * @param fractionalSeconds must fractional Seconds be send to database.
     * @param options           session options
     */
    public OffsetTimeParameter(OffsetTime offsetTime, ZoneId serverZoneId, boolean fractionalSeconds, Options options) throws SQLException {
        ZoneId zoneId = options.useLegacyDatetimeCode ? ZoneOffset.systemDefault() : serverZoneId;
        if (ZoneOffset.class.isInstance(zoneId)) {
            throw new SQLException("cannot set OffsetTime, since server time zone is set to '"
                    + serverZoneId.toString() + "' (check server variables time_zone and system_time_zone)");
        }
        this.time = offsetTime.withOffsetSameInstant(ZoneOffset.class.cast(zoneId));
        this.fractionalSeconds = fractionalSeconds;
    }

    /**
     * Write timestamps to outputStream.
     *
     * @param os the stream to write to
     */
    public void writeTo(final PacketOutputStream os) {
        os.write(ParameterWriter.QUOTE);
        os.write(dateToByte());
        os.write(ParameterWriter.QUOTE);
    }

    /**
     * Write timestamps to outputStream without checking buffer size.
     *
     * @param os the stream to write to
     */
    public void writeUnsafeTo(final PacketOutputStream os) {
        os.buffer.put(ParameterWriter.QUOTE);
        os.writeUnsafe(dateToByte());
        os.buffer.put(ParameterWriter.QUOTE);
    }

    private byte[] dateToByte() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                fractionalSeconds ? "HH:mm:ss.SSSSSS" : "HH:mm:ss", Locale.ENGLISH );
        return formatter.format(time).getBytes();
    }

    public long getApproximateTextProtocolLength() throws IOException {
        return 15;
    }

    /**
     * Write timeStamp in binary format.
     *
     * @param pos buffer to write
     */
    public void writeBinary(final PacketOutputStream pos) {
        if (fractionalSeconds) {
            pos.assureBufferCapacity(13);
            pos.buffer.put((byte) 12);//length
            pos.buffer.put((byte) 0);
            pos.buffer.putInt(0);
            pos.buffer.put((byte) time.getHour());
            pos.buffer.put((byte) time.getMinute());
            pos.buffer.put((byte) time.getSecond());
            pos.buffer.putInt(time.getNano() / 1000);
        } else {
            pos.assureBufferCapacity(9);
            pos.buffer.put((byte) 8);//length
            pos.buffer.put((byte) 0);
            pos.buffer.putInt(0);
            pos.buffer.put((byte) time.getHour());
            pos.buffer.put((byte) time.getMinute());
            pos.buffer.put((byte) time.getSecond());
        }
    }

    public ColumnType getMariaDbType() {
        return ColumnType.TIME;
    }

    @Override
    public String toString() {
        return "'" + time.toString() + "'";
    }

    public boolean isLongData() {
        return false;
    }

    public boolean isNullData() {
        return false;
    }

}