/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */


package org.mariadb.jdbc.internal.com.send.parameters;


import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;


public class TimeParameter implements Cloneable, ParameterHolder {

    private Time time;
    private TimeZone timeZone;
    private boolean fractionalSeconds;

    /**
     * Constructor.
     *
     * @param time              time to write
     * @param timeZone          timezone
     * @param fractionalSeconds must fractional seconds be send.
     */
    public TimeParameter(Time time, TimeZone timeZone, boolean fractionalSeconds) {
        this.time = time;
        this.timeZone = timeZone;
        this.fractionalSeconds = fractionalSeconds;
    }

    /**
     * Write Time parameter to outputStream.
     *
     * @param pos the stream to write to
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        sdf.setTimeZone(timeZone);
        String dateString = sdf.format(time);
        if (time.getTime() < 0) {
            dateString = "-" + dateString;
        }

        pos.write(QUOTE);
        pos.write(dateString.getBytes());
        int microseconds = (int) (time.getTime() % 1000) * 1000;
        if (microseconds > 0 && fractionalSeconds) {
            pos.write('.');
            int factor = 100000;
            while (microseconds > 0) {
                int dig = microseconds / factor;
                pos.write('0' + dig);
                microseconds -= dig * factor;
                factor /= 10;
            }
        }
        pos.write(QUOTE);
    }

    public long getApproximateTextProtocolLength() throws IOException {
        return 15;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */

    public void writeBinary(final PacketOutputStream pos) throws IOException {
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.setTime(time);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        if (fractionalSeconds) {
            pos.write((byte) 12);
            pos.write((byte) 0);
            pos.writeInt(0);
            pos.write((byte) calendar.get(Calendar.HOUR_OF_DAY));
            pos.write((byte) calendar.get(Calendar.MINUTE));
            pos.write((byte) calendar.get(Calendar.SECOND));
            pos.writeInt(calendar.get(Calendar.MILLISECOND) * 1000);
        } else {
            pos.write((byte) 8);//length
            pos.write((byte) 0);
            pos.writeInt(0);
            pos.write((byte) calendar.get(Calendar.HOUR_OF_DAY));
            pos.write((byte) calendar.get(Calendar.MINUTE));
            pos.write((byte) calendar.get(Calendar.SECOND));
        }
    }

    public ColumnType getColumnType() {
        return ColumnType.TIME;
    }

    @Override
    public String toString() {
        return time.toString();
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}