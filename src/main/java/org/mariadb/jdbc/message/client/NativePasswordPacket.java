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
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.context.Context;

public final class NativePasswordPacket implements ClientMessage {

  private final CharSequence password;
  private final byte[] seed;

  public NativePasswordPacket(CharSequence password, byte[] seed) {
    this.password = password;
    this.seed = seed;
  }

  public static byte[] encrypt(CharSequence authenticationData, byte[] seed) {
    if (authenticationData == null || authenticationData.toString().isEmpty()) {
      return new byte[0];
    }

    try {
      final MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
      byte[] bytePwd = authenticationData.toString().getBytes(StandardCharsets.UTF_8);

      final byte[] stage1 = messageDigest.digest(bytePwd);
      messageDigest.reset();
      final byte[] stage2 = messageDigest.digest(stage1);
      messageDigest.reset();
      messageDigest.update(seed);
      messageDigest.update(stage2);

      final byte[] digest = messageDigest.digest();
      final byte[] returnBytes = new byte[digest.length];
      for (int i = 0; i < digest.length; i++) {
        returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
      }
      return returnBytes;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not use SHA-1, failing", e);
    }
  }

  @Override
  public void encode(PacketWriter writer, Context context) throws IOException {
    writer.initPacket();
    if (password != null) {
      writer.writeBytes(encrypt(password, seed));
    }
    writer.flush();
  }

  public String description() {
    return "-NativePasswordPacket-";
  }
}
