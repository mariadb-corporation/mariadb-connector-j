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

package org.mariadb.jdbc.plugin.authentication.standard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketReader;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPlugin;

public class ClearPasswordPlugin implements AuthenticationPlugin {

  public static final String TYPE = "mysql_clear_password";

  private String authenticationData;

  @Override
  public String name() {
    return "mysql clear password";
  }

  @Override
  public String type() {
    return TYPE;
  }

  public void initialize(String authenticationData, byte[] authData, Configuration conf) {
    this.authenticationData = authenticationData;
  }

  /**
   * Send password in clear text to server.
   *
   * @param out out stream
   * @param in in stream
   * @param context context
   * @return response packet
   * @throws IOException if socket error
   */
  public ReadableByteBuf process(PacketWriter out, PacketReader in, Context context)
      throws IOException {
    if (authenticationData == null || authenticationData.isEmpty()) {
      out.writeEmptyPacket();
    } else {
      byte[] bytePwd = authenticationData.getBytes(StandardCharsets.UTF_8);
      out.writeBytes(bytePwd);
      out.writeByte(0);
      out.flush();
    }

    return in.readPacket(true);
  }
}
