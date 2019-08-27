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

import static org.mariadb.jdbc.internal.com.Packet.EOF;
import static org.mariadb.jdbc.internal.com.Packet.ERROR;
import static org.mariadb.jdbc.internal.com.Packet.OK;

import java.awt.HeadlessException;
import java.io.Console;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import javax.swing.JComponent;
import javax.swing.JOptionPane;
import javax.swing.JPasswordField;
import javax.swing.event.AncestorEvent;
import javax.swing.event.AncestorListener;
import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;

public class SendPamAuthPacket implements AuthenticationPlugin {

  private final String password;
  private final String passwordCharacterEncoding;
  private byte[] authData;

  /**
   * Pam plugin contrusctor.
   * @param password                    password
   * @param authData                    authentication data
   * @param passwordCharacterEncoding   password encoding option
   */
  public SendPamAuthPacket(String password, byte[] authData, String passwordCharacterEncoding) {
    this.authData = authData;
    this.password = password;
    this.passwordCharacterEncoding = passwordCharacterEncoding;
  }

  /**
   * Process PAM plugin authentication.
   * see https://mariadb.com/kb/en/library/authentication-plugin-pam/
   *
   * @param out       out stream
   * @param in        in stream
   * @param sequence  packet sequence
   * @return response packet
   * @throws IOException  if socket error
   * @throws SQLException if plugin exception
   */
  public Buffer process(PacketOutputStream out, PacketInputStream in, AtomicInteger sequence)
          throws IOException, SQLException {
    int type = authData.length == 0 ? 0 : authData[0];
    String promptb;
    //conversation is :
    // - first byte is information tell if question is a password or clear text.
    // - other bytes are the question to user

    while (true) {
      promptb = authData.length <= 1 ? null : new String(Arrays.copyOfRange(authData, 1, authData.length));
      if ((promptb == null || "Password: ".equals(promptb)) && password != null && !"".equals(password)) {
        //ask for password
        out.startPacket(sequence.incrementAndGet());
        byte[] bytePwd;
        if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
          bytePwd = password.getBytes(passwordCharacterEncoding);
        } else {
          bytePwd = password.getBytes();
        }
        out.write(bytePwd, 0, bytePwd.length);
        out.write(0);
      } else {
        // 2 means "read the input with the echo enabled"
        // 4 means "password-like input, echo disabled"

        boolean isPassword = type == 4;
        //ask user to answer
        String password = showInputDialog(promptb, isPassword);
        if (password == null) {
          throw new SQLException("Error during PAM authentication : dialog input cancelled");
        }
        out.startPacket(sequence.incrementAndGet());
        byte[] bytePwd;
        if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
          bytePwd = password.getBytes(passwordCharacterEncoding);
        } else {
          bytePwd = password.getBytes();
        }
        out.write(bytePwd, 0, bytePwd.length);
        out.write(0);
      }
      out.flush();

      Buffer buffer = in.getPacket(true);
      sequence.set(in.getLastPacketSeq());
      type = buffer.getByteAt(0) & 0xff;

      //PAM continue until finish.
      if (type == 0xfe //Switch Request
              || type == 0x00 // OK_Packet
              || type == 0xff) { //ERR_Packet
        return buffer;
      }
      authData = buffer.readRawBytes(buffer.remaining());
    }
  }

  private String showInputDialog(String label, boolean isPassword) throws IOException {
    String password;
    try {
      if (isPassword) {
        JPasswordField pwd = new JPasswordField();
        pwd.addAncestorListener(new RequestFocusListener());
        int action = JOptionPane.showConfirmDialog(null, pwd, label, JOptionPane.OK_CANCEL_OPTION);
        if (action == JOptionPane.OK_OPTION) {
          password = new String(pwd.getPassword());
        } else {
          throw new IOException("Error during PAM authentication : dialog input cancelled");
        }
      } else {
        password = JOptionPane.showInputDialog(label);
      }
    } catch (HeadlessException noGraphicalEnvironment) {
      //no graphical environment
      Console console = System.console();
      if (console == null) {
        throw new IOException("Error during PAM authentication : input by console not possible");
      }
      if (isPassword) {
        char[] passwordChar = console.readPassword(label);
        password = new String(passwordChar);
      } else {
        password = console.readLine(label);
      }
    }

    if (password != null) {
      return password;
    } else {
      throw new IOException("Error during PAM authentication : dialog input cancelled");
    }

  }


  /**
   * Force focus to input field.
   */
  public class RequestFocusListener implements AncestorListener {

    private final boolean removeListener;

    public RequestFocusListener() {
      this(true);
    }

    public RequestFocusListener(boolean removeListener) {
      this.removeListener = removeListener;
    }

    @Override
    public void ancestorAdded(AncestorEvent ancestorEvent) {
      JComponent component = ancestorEvent.getComponent();
      component.requestFocusInWindow();
      if (removeListener) {
        component.removeAncestorListener(this);
      }
    }

    @Override
    public void ancestorMoved(AncestorEvent ancestorEvent) {
      //do nothing
    }

    @Override
    public void ancestorRemoved(AncestorEvent ancestorEvent) {
      //do nothing
    }
  }
}
