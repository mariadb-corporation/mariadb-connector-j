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


import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;

import java.io.IOException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class TimeParameter extends NotLongDataParameter implements Cloneable {
    private Time time;
    private Calendar calendar;
    private boolean fractionalSeconds;

    /**
     * Constructor.
     * @param time time to write
     * @param cal session calendar
     * @param fractionalSeconds must fractional seconds be send.
     */
    public TimeParameter(Time time, Calendar cal, boolean fractionalSeconds) {
        this.time = time;
        this.calendar = cal;
        this.fractionalSeconds = fractionalSeconds;
    }

    /**
     * Write Time parameter to outputStream.
     *
     * @param os the stream to write to
     */
    public void writeTo(final PacketOutputStream os) {
        os.write(ParameterWriter.QUOTE);
        os.write(dateToBytes());
        ParameterWriter.formatMicroseconds(os, (int) (time.getTime() % 1000) * 1000, fractionalSeconds);
        os.write(ParameterWriter.QUOTE);
    }

    /**
     * Write time parameter to outputStream without checking buffer size.
     * @param os the stream to write to
     */
    public void writeUnsafeTo(final PacketOutputStream os) {
        os.writeUnsafe(ParameterWriter.QUOTE);
        os.writeUnsafe(dateToBytes());
        ParameterWriter.formatMicrosecondsUnsafe(os, (int) (time.getTime() % 1000) * 1000, fractionalSeconds);
        os.writeUnsafe(ParameterWriter.QUOTE);
    }

    private byte[] dateToBytes() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        sdf.setCalendar(Calendar.getInstance());
        String dateString = sdf.format(time);
        if (time.getTime() < 0) {
            dateString = "-" + dateString;
        }
        return dateString.getBytes();
    }

    public long getApproximateTextProtocolLength() throws IOException {
        return 15;
    }

    /**
     * Write time in binary format.
     * @param writeBuffer write buffer
     */
    public void writeBinary(final PacketOutputStream writeBuffer) {
        calendar = Calendar.getInstance();
        calendar.setTime(time);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        writeBuffer.writeTimeLength(calendar, fractionalSeconds);
    }

    public MariaDbType getMariaDbType() {
        return MariaDbType.TIME;
    }

    @Override
    public String toString() {
        return time.toString();
    }

}