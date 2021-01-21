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

package org.mariadb.jdbc.plugin.authentication.standard.gssapi;

import com.sun.jna.platform.win32.Sspi;
import java.io.IOException;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.PacketReader;
import org.mariadb.jdbc.client.socket.PacketWriter;
import waffle.windows.auth.IWindowsSecurityContext;
import waffle.windows.auth.impl.WindowsSecurityContextImpl;

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
      final PacketWriter out,
      final PacketReader in,
      final String servicePrincipalName,
      final String mechanisms)
      throws IOException {

    // initialize a security context on the client
    IWindowsSecurityContext clientContext =
        WindowsSecurityContextImpl.getCurrent(mechanisms, servicePrincipalName);

    do {

      // Step 1: send token to server
      byte[] tokenForTheServerOnTheClient = clientContext.getToken();
      out.writeBytes(tokenForTheServerOnTheClient);
      out.flush();

      // Step 2: read server response token
      if (clientContext.isContinue()) {
        ReadableByteBuf buf = in.readReadablePacket(true);
        byte[] tokenForTheClientOnTheServer = new byte[buf.readableBytes()];
        buf.readBytes(tokenForTheClientOnTheServer);
        Sspi.SecBufferDesc continueToken =
            new Sspi.SecBufferDesc(Sspi.SECBUFFER_TOKEN, tokenForTheClientOnTheServer);
        clientContext.initialize(clientContext.getHandle(), continueToken, servicePrincipalName);
      }

    } while (clientContext.isContinue());

    clientContext.dispose();
  }
}
