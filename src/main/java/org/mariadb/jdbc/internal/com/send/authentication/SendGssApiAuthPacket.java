/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
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

package org.mariadb.jdbc.internal.com.send.authentication;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.com.send.authentication.gssapi.GssUtility;
import org.mariadb.jdbc.internal.com.send.authentication.gssapi.GssapiAuth;
import org.mariadb.jdbc.internal.com.send.authentication.gssapi.StandardGssapiAuthentication;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;

public class SendGssApiAuthPacket implements AuthenticationPlugin {

  private static final GssapiAuth gssapiAuth;
  private byte[] authData;
  private String optionServicePrincipalName;

  static {
    GssapiAuth init;
    try {
      init = GssUtility.getAuthenticationMethod();
    } catch (Throwable t) {
      init = new StandardGssapiAuthentication();
    }
    gssapiAuth = init;
  }


  public SendGssApiAuthPacket(byte[] authData, String servicePrincipalName) {
    this.authData = authData;
    this.optionServicePrincipalName = servicePrincipalName;
  }

  /**
   * Process gssapi plugin authentication.
   * see https://mariadb.com/kb/en/library/authentication-plugin-gssapi/
   *
   * @param out       out stream
   * @param in        in stream
   * @param sequence  packet sequence
   * @return response packet
   * @throws IOException  if socket error
   * @throws SQLException if plugin exception
   */
  public Buffer process(PacketOutputStream out, PacketInputStream in, AtomicInteger sequence) throws IOException, SQLException {
    Buffer buffer = new Buffer(authData);

    //using provided connection string SPN if set, or if not, using to server information
    final String servicePrincipalName = (optionServicePrincipalName != null && !optionServicePrincipalName.isEmpty())
        ? optionServicePrincipalName : buffer.readStringNullEnd(StandardCharsets.UTF_8);
    String mechanisms = buffer.readStringNullEnd(StandardCharsets.UTF_8);
    if (mechanisms.isEmpty()) {
      mechanisms = "Kerberos";
    }

    gssapiAuth.authenticate(out, in, sequence, servicePrincipalName, mechanisms);

    buffer = in.getPacket(true);
    sequence.set(in.getLastPacketSeq());
    return buffer;
  }

}

