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

package org.mariadb.jdbc.internal.packet.dao.parameters;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

/**
 * Helper class for serializing query parameters.
 */
//TODO integrate that text helper class to packetOutput that already has binary helper
public class ParameterWriter {
    private static final byte[] BINARY_INTRODUCER = {'_', 'b', 'i', 'n', 'a', 'r', 'y', ' ', '\''};
    private static final byte[] QUOTE = {'\''};

    private static void writeBytesEscaped(OutputStream out, byte[] bytes, int count, boolean noBackslashEscapes)
            throws IOException {
        if (noBackslashEscapes) {
            for (int i = 0; i < count; i++) {
                byte bit = bytes[i];
                switch (bit) {
                    case '\'':
                        out.write('\'');
                        out.write(bit);
                        break;
                    default:
                        out.write(bit);
                }
            }
        } else {
            for (int i = 0; i < count; i++) {
                byte bit = bytes[i];
                switch (bit) {
                    case '\\':
                    case '\'':
                    case '"':
                    case 0:
                        out.write('\\');
                        out.write(bit);
                        break;
                    default:
                        out.write(bit);
                }
            }
        }
    }

    public static void writeBytesEscaped(OutputStream out, byte[] bytes, boolean noBackslashEscapes) throws IOException {
        writeBytesEscaped(out, bytes, bytes.length, noBackslashEscapes);
    }

    /**
     * Escape string value.
     * @param bytes string in utf-8 bytes
     * @param noBackslashEscapes flag
     * @return escaped string
     */
    public static String getBytesEscaped(char[] bytes, boolean noBackslashEscapes) {
        StringBuilder returnValue = new StringBuilder();
        int count = bytes.length;
        if (noBackslashEscapes) {
            for (int i = 0; i < count; i++) {
                char bit = bytes[i];
                switch (bit) {
                    case '\'':
                        returnValue.append('\'');
                        returnValue.append(bit);
                        break;
                    default:
                        returnValue.append(bit);
                }
            }
        } else {
            for (int i = 0; i < count; i++) {
                char bit = bytes[i];
                switch (bit) {
                    case '\\':
                    case '\'':
                    case '"':
                    case 0:
                        returnValue.append('\\');
                        returnValue.append(bit);
                        break;
                    default:
                        returnValue.append(bit);
                }
            }
        }
        return returnValue.toString();
    }

    public static String getWriteValue(String value, boolean noBackslashEscapes) {
        return "'" + getBytesEscaped(value.toCharArray(), noBackslashEscapes) + "'";
    }

    /**
     * Write byte array in text format.
     * @param out database stream
     * @param bytes byte arrayto send
     * @param noBackslashEscapes must backslash be escape
     * @throws IOException if any error occur when writing to database
     */
    public static void write(OutputStream out, byte[] bytes, boolean noBackslashEscapes) throws IOException {
        out.write(BINARY_INTRODUCER);
        writeBytesEscaped(out, bytes, noBackslashEscapes);
        out.write(QUOTE);
    }

    /**
     * Write string in text format.
     * @param out database stream
     * @param value String value to send
     * @param noBackslashEscapes must backslash be escape
     * @throws IOException if any error occur when writing to database
     */
    public static void write(OutputStream out, String value, boolean noBackslashEscapes) throws IOException {
        byte[] bytes = value.getBytes("UTF-8");
        out.write(QUOTE);
        writeBytesEscaped(out, bytes, noBackslashEscapes);
        out.write(QUOTE);
    }

    /**
     * Write stream in text format.
     * @param out database stream
     * @param is input stream to write
     * @param noBackslashEscapes must backslash be escape
     * @throws IOException if any error occur when writing to database
     */
    public static void write(OutputStream out, InputStream is, boolean noBackslashEscapes) throws IOException {
        out.write(QUOTE);
        byte[] buffer = new byte[1024];
        int len;
        while ((len = is.read(buffer)) >= 0) {
            writeBytesEscaped(out, buffer, len, noBackslashEscapes);
        }
        out.write(QUOTE);
    }

    /**
     * Write stream in text format.
     * @param out database stream
     * @param is input stream to write
     * @param length max inputstream length to write
     * @param noBackslashEscapes must backslash be escape
     * @throws IOException if any error occur when writing to database
     */
    public static void write(OutputStream out, InputStream is, long length, boolean noBackslashEscapes) throws IOException {
        out.write(QUOTE);
        byte[] buffer = new byte[1024];
        long bytesLeft = length;
        int len;

        for (; ; ) {
            int bytesToRead = (int) Math.min(bytesLeft, buffer.length);
            if (bytesToRead == 0) {
                break;
            }
            len = is.read(buffer, 0, bytesToRead);
            if (len <= 0) {
                break;
            }
            writeBytesEscaped(out, buffer, len, noBackslashEscapes);
            bytesLeft -= len;
        }
        out.write(QUOTE);
    }

    /**
     * Write whole reader in text format.
     * @param out database stream
     * @param reader reader to write
     * @param noBackslashEscapes must backslash be escape
     * @throws IOException if any error occur when writing to database
     */
    public static void write(OutputStream out, java.io.Reader reader, boolean noBackslashEscapes) throws IOException {
        out.write(QUOTE);
        char[] buffer = new char[1024];
        int len;
        while ((len = reader.read(buffer)) >= 0) {
            writeBytesEscaped(out, new String(buffer, 0, len).getBytes("UTF-8"), noBackslashEscapes);
        }
        out.write(QUOTE);
    }

    /**
     * Write cached reader char array to buffer.
     * @param out output buffer
     * @param readArrays cache char array
     * @param noBackslashEscapes backslash must be escape flag
     * @throws IOException if error occur when writing to buffer
     */
    public static void write(OutputStream out, ArrayList<char[]> readArrays, boolean noBackslashEscapes) throws IOException {
        out.write(QUOTE);
        for (char[] charArray : readArrays) {
            writeBytesEscaped(out, new String(charArray, 0, charArray.length).getBytes("UTF-8"), noBackslashEscapes);
        }
        out.write(QUOTE);
    }

    /**
     * Write reader in text format.
     * @param out database stream
     * @param reader reader to write
     * @param length reader max length to write
     * @param noBackslashEscapes must backslash be escape
     * @throws IOException if any error occur when writing to database
     */
    public static void write(OutputStream out, java.io.Reader reader, long length, boolean noBackslashEscapes)
            throws IOException {
        out.write(QUOTE);
        char[] buffer = new char[1024];
        long charsLeft = length;
        int len;

        for (; ; ) {
            int charsToRead = (int) Math.min(charsLeft, buffer.length);
            if (charsToRead == 0) {
                break;
            }
            len = reader.read(buffer, 0, charsToRead);
            if (len <= 0) {
                break;
            }
            byte[] bytes = new String(buffer, 0, len).getBytes("UTF-8");
            writeBytesEscaped(out, bytes, bytes.length, noBackslashEscapes);
            charsLeft -= len;
        }
        out.write(QUOTE);
    }

    public static void write(OutputStream out, int value) throws IOException {
        out.write(String.valueOf(value).getBytes());
    }

    public static void write(OutputStream out, long value) throws IOException {
        out.write(String.valueOf(value).getBytes());
    }

    public static void write(OutputStream out, double value) throws IOException {
        out.write(String.valueOf(value).getBytes());
    }

    public static void write(OutputStream out, BigDecimal bd) throws IOException {
        out.write(bd.toPlainString().getBytes());
    }

    /**
     * Write cache byte array to buffer.
     * @param out buffer
     * @param readArrays cache byte array
     * @param noBackslashEscapes must escape backslash flag
     * @throws IOException if error occur writing buffer
     */
    public static void writeBytesArray(OutputStream out, ArrayList<byte[]> readArrays, boolean noBackslashEscapes) throws IOException {
        out.write(QUOTE);
        for (byte[] buffer : readArrays) {
            writeBytesEscaped(out, buffer, buffer.length, noBackslashEscapes);
        }
        out.write(QUOTE);
    }


    /**
     * Write date in text format.
     * @param out database stream
     * @param calendar session calendar
     * @throws IOException if any error occur when writing to database
     */
    public static void writeDate(OutputStream out, Calendar calendar) throws IOException {
        out.write(QUOTE);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = sdf.format(calendar.getTime());
        out.write(dateString.getBytes());
        out.write(QUOTE);
    }

    static void formatMicroseconds(OutputStream out, int microseconds, boolean writeFractionalSeconds) throws IOException {
        if (microseconds == 0 || !writeFractionalSeconds) {
            return;
        }
        out.write('.');
        int factor = 100000;
        while (microseconds > 0) {
            int dig = microseconds / factor;
            out.write('0' + dig);
            microseconds -= dig * factor;
            factor /= 10;
        }
    }

    /**
     * Write timestamps in text format
     * @param out database outputstream
     * @param ts timestamp to write
     * @param calendar session calendar
     * @param writeFractionalSeconds must fractional seconds be send to database
     * @throws IOException if any error occur when writing to database.
     */
    public static void writeTimestamp(OutputStream out, Timestamp ts, Calendar calendar, boolean writeFractionalSeconds)
            throws IOException {
        out.write(QUOTE);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (calendar != null) {
            sdf.setCalendar(calendar);
        }
        String dateString = sdf.format(ts);
        out.write(dateString.getBytes());
        formatMicroseconds(out, ts.getNanos() / 1000, writeFractionalSeconds);
        out.write(QUOTE);
    }

    /**
     * Write time in text format.
     * @param out database outputStream
     * @param time time to write
     * @param calendar session calendar
     * @param writeFractionalSeconds must fractional seconds be send to database
     * @throws IOException if any error occur when writing to database.
     */
    public static void writeTime(OutputStream out, Time time, Calendar calendar, boolean writeFractionalSeconds)
            throws IOException {
        out.write(QUOTE);
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        if (calendar != null) {
            sdf.setCalendar(calendar);
        }
        String dateString = sdf.format(time);
        if (time.getTime() < 0) {
            dateString = "-" + dateString;
        }
        out.write(dateString.getBytes());
        int microseconds = (int) (time.getTime() % 1000) * 1000;
        formatMicroseconds(out, microseconds, writeFractionalSeconds);
        out.write(QUOTE);
    }

    /**
     * Write object to buffer.
     * @param out outputStream
     * @param obj Object to send
     * @param noBackslashEscapes must back slash be escaped
     * @throws IOException if any connection error occued
     */
    public static void writeObject(OutputStream out, Object obj, boolean noBackslashEscapes) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        write(out, baos.toByteArray(), noBackslashEscapes);
    }

    /**
     * Write integer to byte LittleEndian order.
     * @param value int value to convert
     * @return byte array
     */
    public static byte[] writeLittleEndian(int value) {
        return new byte[]{
                (byte) ((value >> 0) & 0xff),
                (byte) ((value >> 8) & 0xff),
                (byte) ((value >> 16) & 0xff),
                (byte) ((value >> 24) & 0xff)};
    }


}
