/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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

import org.mariadb.jdbc.internal.*;
import org.mariadb.jdbc.internal.io.output.*;
import org.mariadb.jdbc.util.*;

import java.io.*;
import java.sql.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

public class OffsetTimeParameter implements Cloneable, ParameterHolder {

  private OffsetTime time;
  private boolean fractionalSeconds;

  /**
   * Constructor.
   *
   * @param offsetTime time with offset
   * @param serverZoneId server session zoneId
   * @param fractionalSeconds must fractional Seconds be send to database.
   * @param options session options
   * @throws SQLException if offset cannot be converted to server offset
   */
  public OffsetTimeParameter(
      OffsetTime offsetTime, ZoneId serverZoneId, boolean fractionalSeconds, Options options)
      throws SQLException {
    ZoneId zoneId = options.useLegacyDatetimeCode ? ZoneOffset.systemDefault() : serverZoneId;
    if (zoneId instanceof ZoneOffset) {
      throw new SQLException(
          "cannot set OffsetTime, since server time zone is set to '"
              + serverZoneId.toString()
              + "' (check server variables time_zone and system_time_zone)");
    }
    this.time = offsetTime.withOffsetSameInstant((ZoneOffset) zoneId);
    this.fractionalSeconds = fractionalSeconds;
  }

  /**
   * Write timestamps to outputStream.
   *
   * @param pos the stream to write to
   */
  public void writeTo(final PacketOutputStream pos) throws IOException {
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern(
            fractionalSeconds ? "HH:mm:ss.SSSSSS" : "HH:mm:ss", Locale.ENGLISH);
    pos.write(QUOTE);
    pos.write(formatter.format(time).getBytes());
    pos.write(QUOTE);
  }

  public long getApproximateTextProtocolLength() {
    return 15;
  }

  /**
   * Write data to socket in binary format.
   *
   * @param pos socket output stream
   * @throws IOException if socket error occur
   */
  public void writeBinary(final PacketOutputStream pos) throws IOException {
    if (fractionalSeconds) {
      pos.write((byte) 12); // length
      pos.write((byte) 0);
      pos.writeInt(0);
      pos.write((byte) time.getHour());
      pos.write((byte) time.getMinute());
      pos.write((byte) time.getSecond());
      pos.writeInt(time.getNano() / 1000);
    } else {
      pos.write((byte) 8); // length
      pos.write((byte) 0);
      pos.writeInt(0);
      pos.write((byte) time.getHour());
      pos.write((byte) time.getMinute());
      pos.write((byte) time.getSecond());
    }
  }

  public ColumnType getColumnType() {
    return ColumnType.TIME;
  }

  @Override
  public String toString() {
    return "'" + time.toString() + "'";
  }

  public boolean isNullData() {
    return false;
  }

  public boolean isLongData() {
    return false;
  }
}
