// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.standard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.AuthDialogCallback;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;

/**
 * PAM (dialog) authentication plugin. This is a multi-step exchange password. If more than one
 * step, passwordX (password2, password3, ...) options must be set.
 */
public class SendPamAuthPacket implements AuthenticationPlugin {

  private final String authenticationData;
  private final Configuration conf;
  private int counter = 0;

  /**
   * Initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param conf Connection string options
   */
  public SendPamAuthPacket(String authenticationData, Configuration conf) {
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
   * @param sslFingerPrintValidation true if SSL certificate fingerprint validation is enabled
   * @return response packet
   * @throws IOException if socket error
   */
  public ReadableByteBuf process(
      Writer out, Reader in, Context context, boolean sslFingerPrintValidation)
      throws SQLException, IOException {

    ReadableByteBuf serverPacket = null;
    while (true) {
      counter++;
      String password = resolvePassword(serverPacket);

      byte[] bytePwd = password != null ? password.getBytes(StandardCharsets.UTF_8) : new byte[0];
      out.writeBytes(bytePwd, 0, bytePwd.length);
      out.writeByte(0);
      out.flush();

      serverPacket = in.readReusablePacket();

      int type = serverPacket.getUnsignedByte();

      // PAM continue until finish.
      if (type == 0xfe // Switch Request
          || type == 0x00 // OK_Packet
          || type == 0xff) { // ERR_Packet
        return serverPacket;
      }
    }
  }

  /**
   * Pick the answer for the current PAM/dialog round. Round 1 uses the connection password, later
   * rounds prefer the application-registered {@link AuthDialogCallback} (so a UI can surface the
   * actual server prompt). When no callback is registered, or the callback returns {@code null}, we
   * fall back to the historical {@code passwordN} URL options.
   */
  private String resolvePassword(ReadableByteBuf serverPacket) throws SQLException {
    if (counter == 1) {
      return authenticationData;
    }
    AuthDialogCallback cb = dialogCallback();
    if (serverPacket != null && cb != null) {
      int flags = serverPacket.readUnsignedByte();
      String promptText = serverPacket.readStringEof();
      boolean echo = (flags & 0x02) != 0;
      String answer = cb.prompt(echo, promptText, counter);
      if (answer != null) {
        return answer;
      }
    }
    if (!conf.nonMappedOptions().containsKey("password" + counter)) {
      throw new SQLException(
          "PAM authentication request multiple passwords, but 'password"
              + counter
              + "' is not set");
    }
    return (String) conf.nonMappedOptions().get("password" + counter);
  }

  /**
   * Look up the application-registered {@link AuthDialogCallback} via {@link ServiceLoader}. Java
   * equivalent of the C connector's {@code dlsym(RTLD_DEFAULT, "mariadb_auth_dialog")}. Returns
   * {@code null} when nothing is registered.
   */
  private static AuthDialogCallback dialogCallback() {
    Iterator<AuthDialogCallback> it = ServiceLoader.load(AuthDialogCallback.class).iterator();
    return it.hasNext() ? it.next() : null;
  }
}
