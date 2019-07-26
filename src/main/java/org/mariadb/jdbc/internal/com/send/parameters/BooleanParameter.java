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
 */

package org.mariadb.jdbc.internal.com.send.parameters;

import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;

public class BooleanParameter implements Cloneable, ParameterHolder {

  private final boolean value;

  public BooleanParameter(boolean value) {
    this.value = value;
  }

  public void writeTo(final PacketOutputStream os) throws IOException {
    os.write(value ? '1' : '0');
  }

  public long getApproximateTextProtocolLength() {
    return 1;
  }

  /**
   * Write data to socket in binary format.
   *
   * @param pos socket output stream
   * @throws IOException if socket error occur
   */
  public void writeBinary(final PacketOutputStream pos) throws IOException {
    pos.write(value ? 1 : 0);
  }

  public ColumnType getColumnType() {
    return ColumnType.TINYINT;
  }

  @Override
  public String toString() {
    return Boolean.toString(value);
  }

  public boolean isNullData() {
    return false;
  }

  public boolean isLongData() {
    return false;
  }

}
