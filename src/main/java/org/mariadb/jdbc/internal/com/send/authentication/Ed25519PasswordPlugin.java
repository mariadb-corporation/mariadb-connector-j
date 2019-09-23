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
 */

package org.mariadb.jdbc.internal.com.send.authentication;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.mariadb.jdbc.authentication.AuthenticationPlugin;
import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.com.send.authentication.ed25519.math.GroupElement;
import org.mariadb.jdbc.internal.com.send.authentication.ed25519.math.ed25519.ScalarOps;
import org.mariadb.jdbc.internal.com.send.authentication.ed25519.spec.EdDSANamedCurveTable;
import org.mariadb.jdbc.internal.com.send.authentication.ed25519.spec.EdDSAParameterSpec;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.internal.util.Options;

public class Ed25519PasswordPlugin implements AuthenticationPlugin {

  private String authenticationData;
  private String passwordCharacterEncoding;
  private byte[] seed;

  @Override
  public String name() {
    return "Ed25519 authentication plugin";
  }

  @Override
  public String type() {
    return "client_ed25519";
  }

  public void initialize(String authenticationData, byte[] seed, Options options) {
    this.seed = seed;
    this.authenticationData = authenticationData;
    this.passwordCharacterEncoding = options.passwordCharacterEncoding;
  }

  /**
   * Process Ed25519 password plugin authentication. see
   * https://mariadb.com/kb/en/library/authentication-plugin-ed25519/
   *
   * @param out out stream
   * @param in in stream
   * @param sequence packet sequence
   * @return response packet
   * @throws IOException if socket error
   */
  public Buffer process(PacketOutputStream out, PacketInputStream in, AtomicInteger sequence)
      throws IOException, SQLException {
    if (authenticationData == null || authenticationData.isEmpty()) {
      out.writeEmptyPacket(sequence.incrementAndGet());
    } else {
      out.startPacket(sequence.incrementAndGet());
      out.write(ed25519SignWithPassword(authenticationData, seed, passwordCharacterEncoding));
      out.flush();
    }

    Buffer buffer = in.getPacket(true);
    sequence.set(in.getLastPacketSeq());
    return buffer;
  }


  private static byte[] ed25519SignWithPassword(
      final String password, final byte[] seed, String passwordCharacterEncoding)
      throws SQLException {

    try {
      byte[] bytePwd;
      if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
        bytePwd = password.getBytes(passwordCharacterEncoding);
      } else {
        bytePwd = password.getBytes();
      }

      MessageDigest hash = MessageDigest.getInstance("SHA-512");

      int mlen = seed.length;
      final byte[] sm = new byte[64 + mlen];

      byte[] az = hash.digest(bytePwd);
      az[0] &= 248;
      az[31] &= 63;
      az[31] |= 64;

      System.arraycopy(seed, 0, sm, 64, mlen);
      System.arraycopy(az, 32, sm, 32, 32);

      byte[] buff = Arrays.copyOfRange(sm, 32, 96);
      hash.reset();
      byte[] nonce = hash.digest(buff);

      ScalarOps scalar = new ScalarOps();

      EdDSAParameterSpec spec = EdDSANamedCurveTable.getByName("Ed25519");
      GroupElement elementAvalue = spec.getB().scalarMultiply(az);
      byte[] elementAarray = elementAvalue.toByteArray();
      System.arraycopy(elementAarray, 0, sm, 32, elementAarray.length);

      nonce = scalar.reduce(nonce);
      GroupElement elementRvalue = spec.getB().scalarMultiply(nonce);
      byte[] elementRarray = elementRvalue.toByteArray();
      System.arraycopy(elementRarray, 0, sm, 0, elementRarray.length);

      hash.reset();
      byte[] hram = hash.digest(sm);
      hram = scalar.reduce(hram);
      byte[] tt = scalar.multiplyAndAdd(hram, az, nonce);
      System.arraycopy(tt, 0, sm, 32, tt.length);

      return Arrays.copyOfRange(sm, 0, 64);

    } catch (NoSuchAlgorithmException e) {
      throw new SQLException("Could not use SHA-512, failing", e);
    } catch (UnsupportedEncodingException use) {
      throw new SQLException(
          "Unsupported encoding '"
              + passwordCharacterEncoding
              + "' (option passwordCharacterEncoding)",
          use);
    }
  }
}
