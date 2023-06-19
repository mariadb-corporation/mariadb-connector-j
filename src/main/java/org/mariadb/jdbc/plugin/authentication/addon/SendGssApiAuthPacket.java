// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.authentication.addon;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;
import org.mariadb.jdbc.plugin.authentication.addon.gssapi.GssUtility;
import org.mariadb.jdbc.plugin.authentication.addon.gssapi.GssapiAuth;
import org.mariadb.jdbc.plugin.authentication.addon.gssapi.StandardGssapiAuthentication;

/** GSSAPI plugin */
public class SendGssApiAuthPacket implements AuthenticationPlugin {

  private static final GssapiAuth gssapiAuth;

  static {
    GssapiAuth init;
    try {
      init = GssUtility.getAuthenticationMethod();
    } catch (Throwable t) {
      init = new StandardGssapiAuthentication();
    }
    gssapiAuth = init;
  }

  private byte[] seed;
  private String optionServicePrincipalName;

  @Override
  public String type() {
    return "auth_gssapi_client";
  }

  /**
   * Initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param conf Connection string options
   */
  public void initialize(String authenticationData, byte[] seed, Configuration conf) {
    this.seed = seed;
    this.optionServicePrincipalName = conf.servicePrincipalName();
  }

  /**
   * Process gssapi plugin authentication. see
   * https://mariadb.com/kb/en/library/authentication-plugin-gssapi/
   *
   * @param out out stream
   * @param in in stream
   * @param context context
   * @return response packet
   * @throws IOException if socket error
   * @throws SQLException if plugin exception
   */
  public ReadableByteBuf process(Writer out, Reader in, Context context)
      throws IOException, SQLException {
    ReadableByteBuf buf = new StandardReadableByteBuf(seed, seed.length);

    final String serverSpn = buf.readStringNullEnd();
    // using provided connection string SPN if set, or if not, using to server information
    final String servicePrincipalName =
        (optionServicePrincipalName != null) ? optionServicePrincipalName : serverSpn;
    String mechanisms = buf.readStringNullEnd();
    if (mechanisms.isEmpty()) {
      mechanisms = "Kerberos";
    }

    gssapiAuth.authenticate(out, in, servicePrincipalName, mechanisms);

    return in.readReusablePacket();
  }
}
