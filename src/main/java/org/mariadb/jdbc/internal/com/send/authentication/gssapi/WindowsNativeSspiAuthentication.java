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
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.com.send.authentication.gssapi;

import com.sun.jna.platform.win32.*;
import org.mariadb.jdbc.internal.com.read.*;
import org.mariadb.jdbc.internal.io.input.*;
import org.mariadb.jdbc.internal.io.output.*;
import waffle.windows.auth.*;
import waffle.windows.auth.impl.*;

import java.io.*;
import java.util.concurrent.atomic.*;

public class WindowsNativeSspiAuthentication implements GssapiAuth {

  /**
   * Process native windows GSS plugin authentication.
   *
   * @param out out stream
   * @param in in stream
   * @param sequence packet sequence
   * @param servicePrincipalName principal name
   * @param mechanisms gssapi mechanism
   * @throws IOException if socket error
   */
  public void authenticate(
      final PacketOutputStream out,
      final PacketInputStream in,
      final AtomicInteger sequence,
      final String servicePrincipalName,
      final String mechanisms)
      throws IOException {

    // initialize a security context on the client
    IWindowsSecurityContext clientContext =
        WindowsSecurityContextImpl.getCurrent(mechanisms, servicePrincipalName);

    do {

      // Step 1: send token to server
      byte[] tokenForTheServerOnTheClient = clientContext.getToken();
      out.startPacket(sequence.incrementAndGet());
      out.write(tokenForTheServerOnTheClient);
      out.flush();

      // Step 2: read server response token
      if (clientContext.isContinue()) {
        Buffer buffer = in.getPacket(true);
        sequence.set(in.getLastPacketSeq());
        byte[] tokenForTheClientOnTheServer = buffer.readRawBytes(buffer.remaining());
        Sspi.SecBufferDesc continueToken =
            new Sspi.SecBufferDesc(Sspi.SECBUFFER_TOKEN, tokenForTheClientOnTheServer);
        clientContext.initialize(clientContext.getHandle(), continueToken, servicePrincipalName);
      }

    } while (clientContext.isContinue());

    clientContext.dispose();
  }
}
