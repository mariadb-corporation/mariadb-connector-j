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

Copyright (c) 2009-2011, Marcus Eriksson, Trond Norbye, Stephane Giron

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

package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.packet.dao.parameters.OffsetTimeParameter;
import org.mariadb.jdbc.internal.packet.dao.parameters.ZonedDateTimeParameter;
import org.mariadb.jdbc.internal.util.ExceptionMapper;

import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;


public abstract class BasePrepareStatement extends CommonPrepareStatement implements PreparedStatement {

    public BasePrepareStatement(MariaDbConnection connection, int resultSetScrollType) {
        super(connection, resultSetScrollType);
    }

    /**
     * The ISO-like date-time formatter that formats or parses a date-time with
     * offset and zone, such as '2011-12-03T10:15:30+01:00[Europe/Paris]'.
     * and without the 'T' time delimiter
     * <p>This returns an immutable formatter capable of formatting and parsing
     * a format that extends the ISO-8601 extended offset date-time format
     * to add the time-zone.</p>
     **/
    private static final DateTimeFormatter SPEC_ISO_ZONED_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral('T')
            .optionalEnd()
            .optionalStart()
            .appendLiteral(' ')
            .optionalEnd()
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .appendOffsetId()
            .optionalStart()
            .appendLiteral('[')
            .parseCaseSensitive()
            .appendZoneRegionId()
            .appendLiteral(']')
            .toFormatter();

    /**
     * Additional java8 String object.
     *
     * @param parameterIndex    current parameter
     * @param str               String value ( must not be null )
     * @param targetSqlType     SqlType
     * @throws SQLException if exception occur
     */
    public void setStringObject(final int parameterIndex, final String str, final int targetSqlType) throws SQLException {
        try {
            switch (targetSqlType) {
                case Types.TIME_WITH_TIMEZONE:
                    setParameter(parameterIndex,
                            new OffsetTimeParameter(
                                    OffsetTime.parse(str),
                                    protocol.getTimeZone().toZoneId(),
                                    useFractionalSeconds,
                                    options));
                    break;
                case Types.TIMESTAMP_WITH_TIMEZONE:

                    setParameter(parameterIndex,
                            new ZonedDateTimeParameter(
                                    ZonedDateTime.parse(str, SPEC_ISO_ZONED_DATE_TIME),
                                    protocol.getTimeZone().toZoneId(),
                                    useFractionalSeconds,
                                    options));
                    break;
                default:
                    throw ExceptionMapper.getSqlException("Could not convert [" + str + "] to " + targetSqlType);
            }
        } catch (DateTimeParseException dateTimeParseException) {
            throw ExceptionMapper.getSqlException("Could not convert [" + str + "] to " + targetSqlType);
        }
    }

    /**
     * Set java 8 date handling.
     *
     * @param parameterIndex parameter index
     * @param obj            Object
     * @return true if parameter is set, false otherwise
     * @throws SQLException if object parsing failed
     */
    public boolean setAdditionalObject(final int parameterIndex, final Object obj) throws SQLException {
        if (LocalDateTime.class.isInstance(obj)) {
            setTimestamp(parameterIndex, Timestamp.valueOf(LocalDateTime.class.cast(obj)));
        } else if (Instant.class.isInstance(obj)) {
            setTimestamp(parameterIndex, Timestamp.from(Instant.class.cast(obj)));
        } else if (LocalDate.class.isInstance(obj)) {
            setDate(parameterIndex, Date.valueOf(LocalDate.class.cast(obj)));
        } else if (OffsetDateTime.class.isInstance(obj)) {
            setParameter(parameterIndex,
                    new ZonedDateTimeParameter(
                            OffsetDateTime.class.cast(obj).toZonedDateTime(),
                            protocol.getTimeZone().toZoneId(),
                            useFractionalSeconds,
                            options));
        } else if (OffsetTime.class.isInstance(obj)) {
            setParameter(parameterIndex,
                    new OffsetTimeParameter(
                            OffsetTime.class.cast(obj),
                            protocol.getTimeZone().toZoneId(),
                            useFractionalSeconds,
                            options));
        } else if (ZonedDateTime.class.isInstance(obj)) {
            setParameter(parameterIndex,
                    new ZonedDateTimeParameter(
                            ZonedDateTime.class.cast(obj),
                            protocol.getTimeZone().toZoneId(),
                            useFractionalSeconds,
                            options));
        } else if (LocalTime.class.isInstance(obj)) {
            setTime(parameterIndex, Time.valueOf(LocalTime.class.cast(obj)));
        } else {
            return false;
        }
        return true;
    }

    @Override
    public void setObject(int parameterIndex, Object obj, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, obj, targetSqlType.getVendorTypeNumber(), scaleOrLength);
    }

    @Override
    public void setObject(int parameterIndex, Object obj, SQLType targetSqlType) throws SQLException {
        setObject(parameterIndex, obj, targetSqlType.getVendorTypeNumber());
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        if (executeInternal(getFetchSize())) {
            return 0;
        }
        return getLargeUpdateCount();
    }

    protected abstract boolean executeInternal(int fetchSize) throws SQLException;
}
