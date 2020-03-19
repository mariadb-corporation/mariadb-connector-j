/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
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
 *
 */

package org.mariadb.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.time.LocalTime;
import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;

public class LocalTimeParameter implements Cloneable, ParameterHolder {

  private final LocalTime time;
  private final boolean fractionalSeconds;

  /**
   * Constructor.
   *
   * @param time time to write
   * @param fractionalSeconds must fractional seconds be send.
   */
  public LocalTimeParameter(LocalTime time, boolean fractionalSeconds) {
    this.time = time;
    this.fractionalSeconds = fractionalSeconds;
  }

  /**
   * Write Time parameter to outputStream.
   *
   * @param pos the stream to write to
   */
  public void writeTo(final PacketOutputStream pos) throws IOException {
    StringBuilder dateString = new StringBuilder(15);
    dateString
        .append(time.getHour() < 10 ? "0" : "")
        .append(time.getHour())
        .append(time.getMinute() < 10 ? ":0" : ":")
        .append(time.getMinute())
        .append(time.getSecond() < 10 ? ":0" : ":")
        .append(time.getSecond());
    int microseconds = time.getNano() / 1000;
    if (microseconds > 0 && fractionalSeconds) {
      dateString.append(".");
      if (microseconds % 1000 == 0) {
        dateString.append(Integer.toString(microseconds / 1000 + 1000).substring(1));
      } else {
        dateString.append(Integer.toString(microseconds + 1000000).substring(1));
      }
    }

    pos.write(QUOTE);
    pos.write(dateString.toString().getBytes());
    pos.write(QUOTE);
  }

  public int getApproximateTextProtocolLength() {
    return 15;
  }

  /**
   * Write data to socket in binary format.
   *
   * @param pos socket output stream
   * @throws IOException if socket error occur
   */
  public void writeBinary(final PacketOutputStream pos) throws IOException {
    int nano = time.getNano();
    if (fractionalSeconds && nano > 0) {
      pos.write((byte) 12);
      pos.write((byte) 0);
      pos.writeInt(0);
      pos.write((byte) time.getHour());
      pos.write((byte) time.getMinute());
      pos.write((byte) time.getSecond());
      pos.writeInt(nano / 1000);
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
    return time.toString();
  }

  public boolean isNullData() {
    return false;
  }

  public boolean isLongData() {
    return false;
  }
}
