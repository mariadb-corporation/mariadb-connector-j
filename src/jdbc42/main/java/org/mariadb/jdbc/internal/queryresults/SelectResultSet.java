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

package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.queryresults.resultset.SelectResultSetCommon;
import org.mariadb.jdbc.internal.util.ExceptionMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.time.*;
import java.time.format.*;
import java.util.List;


public class SelectResultSet extends SelectResultSetCommon {

    public static final DateTimeFormatter TEXT_LOCAL_DATE_TIME;
    public static final DateTimeFormatter TEXT_OFFSET_DATE_TIME;
    public static final DateTimeFormatter TEXT_ZONED_DATE_TIME;

    static {

        TEXT_LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .toFormatter();

        TEXT_OFFSET_DATE_TIME = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(TEXT_LOCAL_DATE_TIME)
                .appendOffsetId()
                .toFormatter();

        TEXT_ZONED_DATE_TIME = new DateTimeFormatterBuilder()
                .append(TEXT_OFFSET_DATE_TIME)
                .optionalStart()
                .appendLiteral('[')
                .parseCaseSensitive()
                .appendZoneRegionId()
                .appendLiteral(']')
                .toFormatter();
    }

    /**
     * Create Streaming resultSet.
     *
     * @param columnInformation   column information
     * @param results             results
     * @param protocol            current protocol
     * @param fetcher             stream fetcher
     * @param callableResult      is it from a callableStatement ?
     * @throws IOException if any connection error occur
     * @throws SQLException if any connection error occur
     */
    public SelectResultSet(ColumnInformation[] columnInformation, Results results, Protocol protocol,
                           ReadPacketFetcher fetcher, boolean callableResult)
            throws IOException, SQLException {
        super(columnInformation, results, protocol, fetcher, callableResult);
    }

    /**
     * Create filled resultset.
     *
     * @param columnInformation   column information
     * @param resultSet           resultset
     * @param protocol            current protocol
     * @param resultSetScrollType one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     */
    public SelectResultSet(ColumnInformation[] columnInformation, List<byte[][]> resultSet, Protocol protocol,
                                 int resultSetScrollType) {
        super(columnInformation, resultSet, protocol, resultSetScrollType);
    }

    protected <T> T getAdditionalObject(byte[] rawBytes, ColumnInformation columnInfo, Class<T> type) throws SQLException {

        if (rawBytes == null || rawBytes.length == 0) return null;

        switch (type.getName()) {
            case "java.time.LocalDateTime":
                ZonedDateTime zonedDateTime = getZonedDateTime(rawBytes, columnInfo, LocalDateTime.class);
                return zonedDateTime == null ? null : type.cast(zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime());

            case "java.time.ZonedDateTime":
                return type.cast(getZonedDateTime(rawBytes, columnInfo, ZonedDateTime.class));

            case "java.time.OffsetDateTime":
                ZonedDateTime tmpZonedDateTime = getZonedDateTime(rawBytes, columnInfo, OffsetDateTime.class);
                return tmpZonedDateTime == null ? null : type.cast(tmpZonedDateTime.toOffsetDateTime());

            case "java.time.LocalDate":
                return type.cast(getLocalDate(rawBytes, columnInfo));

            case "java.time.LocalTime":
                return type.cast(getLocalTime(rawBytes, columnInfo));

            case "java.time.OffsetTime":
                return type.cast(getOffsetTime(rawBytes, columnInfo));

            default:
                throw ExceptionMapper.getFeatureNotSupportedException("Type class '" + type.getName() + "' is not supported");

        }

    }


    /**
     * Get LocalDateTime from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @param clazz      ending class
     * @return timestamp.
     * @throws ParseException if text value cannot be parse
     */
    private ZonedDateTime getZonedDateTime(byte[] rawBytes, ColumnInformation columnInfo, Class clazz) throws SQLException {

        if (!this.isBinaryEncoded) {

            String raw = new String(rawBytes, StandardCharsets.UTF_8);

            switch (columnInfo.getColumnType().getSqlType()) {
                case Types.TIMESTAMP:

                    if (raw.startsWith("0000-00-00 00:00:00")) return null;
                    try {
                        LocalDateTime localDateTime = LocalDateTime.parse(raw, TEXT_LOCAL_DATE_TIME.withZone(timeZone.toZoneId()));
                        return ZonedDateTime.of(localDateTime, timeZone.toZoneId());
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as LocalDateTime. time must have \"yyyy-MM-dd HH:mm:ss[.S]\" format");
                    }

                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:

                    if (raw.startsWith("0000-00-00 00:00:00")) return null;
                    try {
                        return ZonedDateTime.parse(raw, TEXT_ZONED_DATE_TIME);
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as ZonedDateTime. time must have \"yyyy-MM-dd[T/ ]HH:mm:ss[.S]\" "
                                + "with offset and timezone format (example : '2011-12-03 10:15:30+01:00[Europe/Paris]')");
                    }

                default:
                    throw new SQLException("Cannot read " + clazz.getName() + " using a " + columnInfo.getColumnType().getJavaTypeName() + " field");

            }

        } else {

            switch (columnInfo.getColumnType().getSqlType()) {
                case Types.TIMESTAMP:

                    int year = ((rawBytes[0] & 0xff) | (rawBytes[1] & 0xff) << 8);
                    int month = rawBytes[2];
                    int day = rawBytes[3];
                    int hour = 0;
                    int minutes = 0;
                    int seconds = 0;
                    int microseconds = 0;

                    if (rawBytes.length > 4) {
                        hour = rawBytes[4];
                        minutes = rawBytes[5];
                        seconds = rawBytes[6];

                        if (rawBytes.length > 7) {
                            microseconds = ((rawBytes[7] & 0xff)
                                    + ((rawBytes[8] & 0xff) << 8)
                                    + ((rawBytes[9] & 0xff) << 16)
                                    + ((rawBytes[10] & 0xff) << 24));
                        }
                    }

                    return ZonedDateTime.of(year, month, day, hour, minutes, seconds, microseconds * 1000, timeZone.toZoneId());

                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:

                    //string conversion
                    String raw = new String(rawBytes, StandardCharsets.UTF_8);
                    if (raw.startsWith("0000-00-00 00:00:00")) return null;
                    try {
                        return ZonedDateTime.parse(raw, TEXT_ZONED_DATE_TIME);
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as ZonedDateTime. time must have \"yyyy-MM-dd[T/ ]HH:mm:ss[.S]\" "
                                + "with offset and timezone format (example : '2011-12-03 10:15:30+01:00[Europe/Paris]')");
                    }

                default:
                    throw new SQLException("Cannot read " + clazz.getName() + " using a " + columnInfo.getColumnType().getJavaTypeName() + " field");
            }

        }
    }


    /**
     * Get OffsetTime from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return timestamp.
     * @throws ParseException if text value cannot be parse
     */
    private OffsetTime getOffsetTime(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {

        ZoneId zoneId = timeZone.toZoneId().normalized();
        if (ZoneOffset.class.isInstance(zoneId)) {
            ZoneOffset zoneOffset = ZoneOffset.class.cast(zoneId);
            if (!this.isBinaryEncoded) {
                String raw = new String(rawBytes, StandardCharsets.UTF_8);
                switch (columnInfo.getColumnType().getSqlType()) {

                    case Types.TIMESTAMP:
                        if (raw.startsWith("0000-00-00 00:00:00")) return null;
                        try {
                            return ZonedDateTime.parse(raw, TEXT_LOCAL_DATE_TIME.withZone(zoneOffset)).toOffsetDateTime().toOffsetTime();
                        } catch (DateTimeParseException dateParserEx) {
                            throw new SQLException(raw + " cannot be parse as OffsetTime. time must have \"yyyy-MM-dd HH:mm:ss[.S]\" format");
                        }

                    case Types.TIME:
                        try {
                            LocalTime localTime = LocalTime.parse(raw, DateTimeFormatter.ISO_LOCAL_TIME.withZone(zoneOffset));
                            return OffsetTime.of(localTime, zoneOffset);
                        } catch (DateTimeParseException dateParserEx) {
                            throw new SQLException(raw + " cannot be parse as OffsetTime (format is \"HH:mm:ss[.S]\" for data type \""
                                    + columnInfo.getColumnType() + "\")");
                        }

                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CHAR:
                        try {
                            return OffsetTime.parse(raw, DateTimeFormatter.ISO_OFFSET_TIME);
                        } catch (DateTimeParseException dateParserEx) {
                            throw new SQLException(raw + " cannot be parse as OffsetTime (format is \"HH:mm:ss[.S]\" with offset for data type \""
                                    + columnInfo.getColumnType() + "\")");
                        }

                    default:
                        throw new SQLException("Cannot read " + OffsetTime.class.getName() + " using a "
                                + columnInfo.getColumnType().getJavaTypeName() + " field");
                }

            } else {

                int day = 0;
                int hour = 0;
                int minutes = 0;
                int seconds = 0;
                int microseconds = 0;

                switch (columnInfo.getColumnType().getSqlType()) {
                    case Types.TIMESTAMP:
                        int year = ((rawBytes[0] & 0xff) | (rawBytes[1] & 0xff) << 8);
                        int month = rawBytes[2];
                        day = rawBytes[3];

                        if (rawBytes.length > 4) {
                            hour = rawBytes[4];
                            minutes = rawBytes[5];
                            seconds = rawBytes[6];

                            if (rawBytes.length > 7) {
                                microseconds = ((rawBytes[7] & 0xff)
                                        + ((rawBytes[8] & 0xff) << 8)
                                        + ((rawBytes[9] & 0xff) << 16)
                                        + ((rawBytes[10] & 0xff) << 24));
                            }
                        }

                        return ZonedDateTime.of(year, month, day, hour, minutes, seconds, microseconds * 1000, zoneOffset)
                                .toOffsetDateTime().toOffsetTime();

                    case Types.TIME:

                        boolean negate = (rawBytes[0] & 0xff) == 0x01;

                        if (rawBytes.length > 4) {
                            day = ((rawBytes[1] & 0xff)
                                    + ((rawBytes[2] & 0xff) << 8)
                                    + ((rawBytes[3] & 0xff) << 16)
                                    + ((rawBytes[4] & 0xff) << 24));
                        }

                        if (rawBytes.length > 7) {
                            hour = rawBytes[5];
                            minutes = rawBytes[6];
                            seconds = rawBytes[7];
                        }

                        if (rawBytes.length > 8) {
                            microseconds = ((rawBytes[8] & 0xff)
                                    + ((rawBytes[9] & 0xff) << 8)
                                    + ((rawBytes[10] & 0xff) << 16)
                                    + ((rawBytes[11] & 0xff) << 24));
                        }

                        return OffsetTime.of((negate ? -1 : 1) * (day * 24 + hour), minutes, seconds, microseconds * 1000, zoneOffset);

                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CHAR:
                        String raw = new String(rawBytes, StandardCharsets.UTF_8);
                        try {
                            return OffsetTime.parse(raw, DateTimeFormatter.ISO_OFFSET_TIME);
                        } catch (DateTimeParseException dateParserEx) {
                            throw new SQLException(raw + " cannot be parse as OffsetTime (format is \"HH:mm:ss[.S]\" with offset for data type \""
                                    + columnInfo.getColumnType() + "\")");
                        }

                    default:
                        throw new SQLException("Cannot read " + OffsetTime.class.getName() + " using a "
                                + columnInfo.getColumnType().getJavaTypeName() + " field");
                }
            }
        }

        if (options.useLegacyDatetimeCode) {
            //system timezone is not an offset
            throw new SQLException("Cannot return an OffsetTime for a TIME field when default timezone is '" + zoneId
                    + "' (only possible for time-zone offset from Greenwich/UTC, such as +02:00)");
        }

        //server timezone is not an offset
        throw new SQLException("Cannot return an OffsetTime for a TIME field when server timezone '" + zoneId
                + "' (only possible for time-zone offset from Greenwich/UTC, such as +02:00)");

    }

    /**
     * Get LocalTime from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return timestamp.
     * @throws ParseException if text value cannot be parse
     */
    private LocalTime getLocalTime(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {

        if (!this.isBinaryEncoded) {

            String raw = new String(rawBytes, StandardCharsets.UTF_8);

            switch (columnInfo.getColumnType().getSqlType()) {
                case Types.TIME:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                    try {
                        return LocalTime.parse(raw, DateTimeFormatter.ISO_LOCAL_TIME.withZone(timeZone.toZoneId()));
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as LocalTime (format is \"HH:mm:ss[.S]\" for data type \""
                                + columnInfo.getColumnType() + "\")");
                    }

                case Types.TIMESTAMP:
                    ZonedDateTime zonedDateTime = getZonedDateTime(rawBytes, columnInfo, LocalTime.class);
                    return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalTime();

                default:
                    throw new SQLException("Cannot read LocalTime using a " + columnInfo.getColumnType().getJavaTypeName() + " field");
            }

        } else {


            switch (columnInfo.getColumnType().getSqlType()) {
                case Types.TIME:

                    int day = 0;
                    int hour = 0;
                    int minutes = 0;
                    int seconds = 0;
                    int microseconds = 0;

                    boolean negate = (rawBytes[0] & 0xff) == 0x01;

                    if (rawBytes.length > 4) {
                        day = ((rawBytes[1] & 0xff)
                                + ((rawBytes[2] & 0xff) << 8)
                                + ((rawBytes[3] & 0xff) << 16)
                                + ((rawBytes[4] & 0xff) << 24));
                    }

                    if (rawBytes.length > 7) {
                        hour = rawBytes[5];
                        minutes = rawBytes[6];
                        seconds = rawBytes[7];
                    }

                    if (rawBytes.length > 8) {
                        microseconds = ((rawBytes[8] & 0xff)
                                + ((rawBytes[9] & 0xff) << 8)
                                + ((rawBytes[10] & 0xff) << 16)
                                + ((rawBytes[11] & 0xff) << 24));
                    }

                    return LocalTime.of((negate ? -1 : 1) * (day * 24 + hour), minutes, seconds, microseconds * 1000);

                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                    //string conversion
                    String raw = new String(rawBytes, StandardCharsets.UTF_8);
                    try {
                        return LocalTime.parse(raw, DateTimeFormatter.ISO_LOCAL_TIME.withZone(timeZone.toZoneId()));
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as LocalTime (format is \"HH:mm:ss[.S]\" for data type \""
                                + columnInfo.getColumnType() + "\")");
                    }

                case Types.TIMESTAMP:
                    ZonedDateTime zonedDateTime = getZonedDateTime(rawBytes, columnInfo, LocalTime.class);
                    return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalTime();

                default:
                    throw new SQLException("Cannot read LocalTime using a " + columnInfo.getColumnType().getJavaTypeName() + " field");
            }

        }
    }



    /**
     * Get LocalDateTime from raw data.
     *
     * @param rawBytes   bytes
     * @param columnInfo current column information
     * @return timestamp.
     * @throws ParseException if text value cannot be parse
     */
    private LocalDate getLocalDate(byte[] rawBytes, ColumnInformation columnInfo) throws SQLException {

        if (!this.isBinaryEncoded) {

            String raw = new String(rawBytes, StandardCharsets.UTF_8);

            switch (columnInfo.getColumnType().getSqlType()) {
                case Types.DATE:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                    if (raw.startsWith("0000-00-00")) return null;
                    try {
                        return LocalDate.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE.withZone(timeZone.toZoneId()));
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as LocalDate (format is \"yyyy-MM-dd\" for data type \""
                                + columnInfo.getColumnType() + "\")");
                    }

                case Types.TIMESTAMP:
                    ZonedDateTime zonedDateTime = getZonedDateTime(rawBytes, columnInfo, LocalDate.class);
                    return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalDate();

                default:
                    throw new SQLException("Cannot read LocalDate using a " + columnInfo.getColumnType().getJavaTypeName() + " field");

            }

        } else {

            switch (columnInfo.getColumnType().getSqlType()) {

                case Types.DATE:
                    int year = ((rawBytes[0] & 0xff) | (rawBytes[1] & 0xff) << 8);
                    int month = rawBytes[2];
                    int day = rawBytes[3];
                    return LocalDate.of(year, month, day);

                case Types.TIMESTAMP:
                    ZonedDateTime zonedDateTime = getZonedDateTime(rawBytes, columnInfo, LocalDate.class);
                    return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalDate();

                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                    //string conversion
                    String raw = new String(rawBytes, StandardCharsets.UTF_8);
                    if (raw.startsWith("0000-00-00")) return null;
                    try {
                        return LocalDate.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE.withZone(timeZone.toZoneId()));
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as LocalDate. time must have \"yyyy-MM-dd\" format");
                    }

                default:
                    throw new SQLException("Cannot read LocalDate using a " + columnInfo.getColumnType().getJavaTypeName() + " field");
            }

        }
    }

}
