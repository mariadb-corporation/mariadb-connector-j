// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.plugin.authentication.addon.gssapi;

import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.PacketReader;
import com.singlestore.jdbc.client.socket.PacketWriter;
import com.sun.jna.platform.win32.Sspi;
import com.sun.jna.platform.win32.SspiUtil;
import java.io.IOException;
import waffle.windows.auth.IWindowsSecurityContext;
import waffle.windows.auth.impl.WindowsSecurityContextImpl;

public class WindowsNativeSspiAuthentication implements GssapiAuth {

  /**
   * Process native windows GSS plugin authentication.
   *
   * @param out out stream
   * @param in in stream
   * @param servicePrincipalName principal name
   * @param jaasApplicationName entry name in JAAS Login Configuration File
   * @param mechanisms gssapi mechanism
   * @throws IOException if socket error
   */
  public void authenticate(
      final PacketWriter out,
      final PacketReader in,
      final String servicePrincipalName,
      final String jaasApplicationName,
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
        ReadableByteBuf buf = in.readPacket(true);
        byte[] tokenForTheClientOnTheServer = new byte[buf.readableBytes()];
        buf.readBytes(tokenForTheClientOnTheServer);
        Sspi.SecBufferDesc continueToken =
            new SspiUtil.ManagedSecBufferDesc(Sspi.SECBUFFER_TOKEN, tokenForTheClientOnTheServer);
        clientContext.initialize(clientContext.getHandle(), continueToken, servicePrincipalName);
      }

    } while (clientContext.isContinue());

    clientContext.dispose();
  }
}
