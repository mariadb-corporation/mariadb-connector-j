// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.addon.gssapi;

import com.sun.jna.platform.win32.Sspi;
import com.sun.jna.platform.win32.SspiUtil;
import java.io.IOException;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import waffle.windows.auth.IWindowsSecurityContext;
import waffle.windows.auth.impl.WindowsSecurityContextImpl;

/** Waffle windows GSSAPI implementation */
public class WindowsNativeSspiAuthentication implements GssapiAuth {

  /**
   * Process native windows GSS plugin authentication.
   *
   * @param out out stream
   * @param in in stream
   * @param servicePrincipalName principal name
   * @param mechanisms gssapi mechanism
   * @throws IOException if socket error
   */
  public void authenticate(
      final Writer out, final Reader in, final String servicePrincipalName, final String mechanisms)
      throws IOException {

    // initialize a security context on the client
    IWindowsSecurityContext clientContext =
        WindowsSecurityContextImpl.getCurrent(mechanisms, servicePrincipalName);

    do {

      // Step 1: send token to server
      byte[] tokenForTheServerOnTheClient = clientContext.getToken();
      if (tokenForTheServerOnTheClient != null && tokenForTheServerOnTheClient.length > 0) {
        out.writeBytes(tokenForTheServerOnTheClient);
        out.flush();
      }
      if (!clientContext.isContinue()) {
        break;
      }

      // Step 2: read server response token
      ReadableByteBuf buf = in.readReusablePacket();

      // server cannot allow plugin data packet to start with 0, 255 or 254,
      // as connectors would treat it as an OK, Error or authentication switch packet
      // server then these bytes with 0x001. Consequently, it escaped 0x01 byte too.
      if (buf.getByte() == 0x01) buf.skip();

      byte[] tokenForTheClientOnTheServer = new byte[buf.readableBytes()];
      buf.readBytes(tokenForTheClientOnTheServer);
      Sspi.SecBufferDesc continueToken =
          new SspiUtil.ManagedSecBufferDesc(Sspi.SECBUFFER_TOKEN, tokenForTheClientOnTheServer);
      clientContext.initialize(clientContext.getHandle(), continueToken, servicePrincipalName);

    } while (true);

    clientContext.dispose();
  }
}
