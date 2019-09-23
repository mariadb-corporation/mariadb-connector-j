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

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.util.Options;

/**
 * server doesn't support temporal with timezone (MDEV-10018) for the moment. So driver parse String
 * entry and send it to Server according to server timezone
 */
public class ZonedDateTimeParameter implements Cloneable, ParameterHolder {

  private final ZonedDateTime tz;
  private final boolean fractionalSeconds;

  /**
   * Constructor.
   *
   * @param tz                zone date time
   * @param serverZoneId      server session zoneId
   * @param fractionalSeconds must fractional Seconds be send to database.
   * @param options           session options
   */
  public ZonedDateTimeParameter(ZonedDateTime tz, ZoneId serverZoneId, boolean fractionalSeconds,
      Options options) {
    ZoneId zoneId = options.useLegacyDatetimeCode ? ZoneOffset.systemDefault() : serverZoneId;
    this.tz = tz.withZoneSameInstant(zoneId);
    this.fractionalSeconds = fractionalSeconds;
  }

  /**
   * Write timestamps to outputStream.
   *
   * @param pos the stream to write to
   */
  public void writeTo(final PacketOutputStream pos) throws IOException {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
        fractionalSeconds ? "yyyy-MM-dd HH:mm:ss.SSSSSS" : "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
    pos.write(QUOTE);
    pos.write(formatter.format(tz).getBytes());
    pos.write(QUOTE);
  }

  public long getApproximateTextProtocolLength() {
    return 27;
  }

  /**
   * Write data to socket in binary format.
   *
   * @param pos socket output stream
   * @throws IOException if socket error occur
   */
  public void writeBinary(final PacketOutputStream pos) throws IOException {
    pos.write((byte) (fractionalSeconds ? 11 : 7));//length
    pos.writeShort((short) tz.getYear());
    pos.write((byte) ((tz.getMonth().getValue()) & 0xff));
    pos.write((byte) (tz.getDayOfMonth() & 0xff));
    pos.write((byte) tz.getHour());
    pos.write((byte) tz.getMinute());
    pos.write((byte) tz.getSecond());
    if (fractionalSeconds) {
      pos.writeInt(tz.getNano() / 1000);
    }
  }

  public ColumnType getColumnType() {
    return ColumnType.DATETIME;
  }

  @Override
  public String toString() {
    return "'" + tz.toString() + "'";
  }

  public boolean isNullData() {
    return false;
  }

  public boolean isLongData() {
    return false;
  }
}