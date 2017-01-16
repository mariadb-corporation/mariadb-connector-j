package org.mariadb.jdbc.internal.packet.dao.parameters;

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


import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.util.Options;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateParameter implements ParameterHolder, Cloneable {
    private Date date;
    private Calendar calendar;
    private Options options;

    /**
     * Represents a date, constructed with time in millis since epoch.
     *
     * @param date    the date
     * @param cal     the calendar to use for timezone
     * @param options jdbc options
     */
    public DateParameter(Date date, Calendar cal, Options options) {
        this.date = date;
        this.calendar = cal;
        this.options = options;
    }


    /**
     * Write to server OutputStream in text protocol.
     *
     * @param os output buffer
     */
    public void writeTo(final PacketOutputStream os) {
        os.write(ParameterWriter.QUOTE);
        os.write(dateByteFormat());
        os.write(ParameterWriter.QUOTE);
    }

    /**
     * Write to server OutputStream in text protocol without checking buffer size.
     *
     * @param os output buffer
     */
    public void writeUnsafeTo(final PacketOutputStream os) {
        os.writeUnsafe(ParameterWriter.QUOTE);
        os.writeUnsafe(dateByteFormat());
        os.writeUnsafe(ParameterWriter.QUOTE);
    }

    private byte[] dateByteFormat() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(calendar().getTime()).getBytes();
    }

    private Calendar calendar() {
        if (options.useLegacyDatetimeCode || options.maximizeMysqlCompatibility) {
            calendar = Calendar.getInstance();
        }
        calendar.setTimeInMillis(date.getTime());
        return calendar;
    }

    public long getApproximateTextProtocolLength() {
        return 16;
    }


    /**
     * Write to server OutputStream in binary protocol.
     *
     * @param writeBuffer output buffer
     */
    public void writeBinary(final PacketOutputStream writeBuffer) {
        calendar = Calendar.getInstance();
        calendar.setTimeInMillis(date.getTime());
        writeBuffer.writeDateLength(calendar);
    }

    public ColumnType getMariaDbType() {
        return ColumnType.DATE;
    }

    @Override
    public String toString() {
        return "'" + date.toString() + "'";
    }

    public boolean isLongData() {
        return false;
    }

    public boolean isNullData() {
        return false;
    }

}