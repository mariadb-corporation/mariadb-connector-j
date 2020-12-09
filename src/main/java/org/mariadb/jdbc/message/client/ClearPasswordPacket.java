/*
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
 */

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.context.Context;

public final class ClearPasswordPacket implements ClientMessage {

  private final CharSequence password;

  public ClearPasswordPacket(CharSequence password) {
    this.password = password;
  }

  @Override
  public void encode(PacketWriter writer, Context context) throws IOException {
    writer.initPacket();
    if (password != null) {
      writer.writeString(password.toString());
      writer.writeByte(0);
    }
    writer.flush();
  }

  public String description() {
    return "-ClearPasswordPacket-";
  }
}
