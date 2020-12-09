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
import java.sql.SQLException;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.PacketReader;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPlugin;

public class SendPamAuthPacket implements AuthenticationPlugin {

  private String authenticationData;
  private Configuration conf;
  private int counter = 0;

  @Override
  public String name() {
    return "PAM client authentication";
  }

  @Override
  public String type() {
    return "dialog";
  }

  /**
   * Initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param conf Connection string options
   */
  public void initialize(String authenticationData, byte[] seed, Configuration conf) {
    this.authenticationData = authenticationData;
    this.conf = conf;
  }

  /**
   * Process PAM plugin authentication. see
   * https://mariadb.com/kb/en/library/authentication-plugin-pam/
   *
   * @param out out stream
   * @param in in stream
   * @param context connection context
   * @return response packet
   * @throws IOException if socket error
   */
  public ReadableByteBuf process(PacketWriter out, PacketReader in, Context context)
      throws SQLException, IOException {

    while (true) {
      counter++;
      String password;
      if (counter == 1) {
        password = authenticationData;
      } else {
        if (!conf.nonMappedOptions().containsKey("password" + counter)) {
          throw new SQLException(
              "PAM authentication request multiple passwords, but "
                  + "'password"
                  + counter
                  + "' is not set");
        }
        password = (String) conf.nonMappedOptions().get("password" + counter);
      }

      byte[] bytePwd = password.getBytes(StandardCharsets.UTF_8);
      out.writeBytes(bytePwd, 0, bytePwd.length);
      out.writeByte(0);
      out.flush();

      ReadableByteBuf buf = in.readPacket(true);

      int type = buf.getUnsignedByte();

      // PAM continue until finish.
      if (type == 0xfe // Switch Request
          || type == 0x00 // OK_Packet
          || type == 0xff) { // ERR_Packet
        return buf;
      }
    }
  }
}
