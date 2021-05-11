/*
 *
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
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.mariadb.jdbc.authentication.AuthenticationPlugin;
import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.util.Options;

public class SendPamAuthPacket implements AuthenticationPlugin {

  private String authenticationData;
  private String passwordCharacterEncoding;
  private Options options;
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
   * @param options Connection string options
   */
  public void initialize(String authenticationData, byte[] seed, Options options) {
    this.authenticationData = authenticationData;
    this.passwordCharacterEncoding = options.passwordCharacterEncoding;
    this.options = options;
  }

  /**
   * Process PAM plugin authentication. see
   * https://mariadb.com/kb/en/library/authentication-plugin-pam/
   *
   * @param out out stream
   * @param in in stream
   * @param sequence packet sequence
   * @return response packet
   * @throws IOException if socket error
   * @throws SQLException if plugin exception
   */
  public Buffer process(PacketOutputStream out, PacketInputStream in, AtomicInteger sequence)
      throws IOException, SQLException {
    while (true) {
      counter++;
      String password;
      if (counter == 1) {
        password = authenticationData;
      } else {
        if (!options.nonMappedOptions.containsKey("password" + counter)) {
          throw new SQLException(
              "PAM authentication request multiple passwords, but "
                  + "'password"
                  + counter
                  + "' is not set");
        }
        password = (String) options.nonMappedOptions.get("password" + counter);
      }

      out.startPacket(sequence.incrementAndGet());
      byte[] bytePwd = new byte[0];
      if (password != null) {
        if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
          bytePwd = password.getBytes(passwordCharacterEncoding);
        } else {
          bytePwd = password.getBytes();
        }
      }

      out.write(bytePwd, 0, bytePwd.length);
      out.write(0);
      out.flush();

      Buffer buffer = in.getPacket(true);
      sequence.set(in.getLastPacketSeq());
      int type = buffer.getByteAt(0) & 0xff;

      // PAM continue until finish.
      if (type == 0xfe // Switch Request
          || type == 0x00 // OK_Packet
          || type == 0xff) { // ERR_Packet
        return buffer;
      }
    }
  }
}
