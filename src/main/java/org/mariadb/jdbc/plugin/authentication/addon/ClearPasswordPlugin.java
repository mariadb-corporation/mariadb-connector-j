// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.addon;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.export.SslMode;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;

/** Clear password plugin. */
public class ClearPasswordPlugin implements AuthenticationPlugin {

  private final String authenticationData;
  private final HostAddress hostAddress;
  private final Configuration conf;

  /**
   * Initialization.
   *
   * @param authenticationData authentication data (password/token)
   */
  public ClearPasswordPlugin(
      String authenticationData, HostAddress hostAddress, Configuration conf) {
    this.authenticationData = authenticationData;
    this.hostAddress = hostAddress;
    this.conf = conf;
  }

  /**
   * Send password in clear text to the server.
   *
   * @param out out stream
   * @param in in stream
   * @param context context
   * @param sslFingerPrintValidation true if SSL certificate fingerprint validation is enabled
   * @return response packet
   * @throws IOException if socket error
   */
  public ReadableByteBuf process(
      Writer out, Reader in, Context context, boolean sslFingerPrintValidation)
      throws IOException, SQLException {

    SslMode sslMode = hostAddress.sslMode == null ? conf.sslMode() : hostAddress.sslMode;
    if (sslMode != SslMode.DISABLE && sslMode != SslMode.TRUST && sslFingerPrintValidation) {
      throw new SQLException(
          "Driver cannot send password in clear when using SSL when certificates are not explicitly"
              + " passed on configuration.",
          "S1010");
    }
    if (authenticationData == null) {
      out.writeEmptyPacket();
    } else {
      byte[] bytePwd = authenticationData.getBytes(StandardCharsets.UTF_8);
      out.writeBytes(bytePwd);
      out.writeByte(0);
      out.flush();
    }

    return in.readReusablePacket();
  }
}
