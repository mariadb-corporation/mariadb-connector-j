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

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.math.GroupElement;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.math.ed25519.Ed25519ScalarOps;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.spec.EdDSANamedCurveTable;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.spec.EdDSAParameterSpec;

public final class Ed25519PasswordPacket implements ClientMessage {

  private final CharSequence password;
  private final byte[] seed;

  public Ed25519PasswordPacket(CharSequence password, byte[] seed) {
    this.password = password;
    this.seed = seed;
  }

  private static byte[] ed25519SignWithPassword(final CharSequence password, final byte[] seed) {

    try {
      byte[] bytePwd = password.toString().getBytes(StandardCharsets.UTF_8);

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

      Ed25519ScalarOps scalar = new Ed25519ScalarOps();

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
      throw new RuntimeException("Could not use SHA-512, failing", e);
    }
  }

  @Override
  public int encode(PacketWriter writer, Context context) throws IOException, SQLException {
    writer.initPacket();
    if (password != null && !password.toString().isEmpty()) {
      writer.writeBytes(ed25519SignWithPassword(password, seed));
    }
    writer.flush();
    return 0;
  }

  public String description() {
    return "-Ed25519PasswordPacket-";
  }
}
