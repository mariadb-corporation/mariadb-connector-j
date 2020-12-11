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
import java.security.PublicKey;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import javax.crypto.Cipher;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;

public final class Sha256PasswordPacket implements ClientMessage {

  private final CharSequence password;
  private final byte[] seed;
  private final PublicKey publicKey;

  public Sha256PasswordPacket(CharSequence password, byte[] seed, PublicKey publicKey) {
    this.password = password;
    byte[] truncatedSeed = new byte[seed.length - 1];
    System.arraycopy(seed, 0, truncatedSeed, 0, seed.length - 1);
    this.seed = truncatedSeed;
    this.publicKey = publicKey;
  }

  /**
   * Encode password with seed and public key.
   *
   * @param publicKey public key
   * @param password password
   * @param seed seed
   * @return encoded password
   * @throws SQLException if cannot encode password
   */
  public static byte[] encrypt(PublicKey publicKey, CharSequence password, byte[] seed)
      throws SQLException {

    byte[] bytePwd = password.toString().getBytes(StandardCharsets.UTF_8);

    byte[] nullFinishedPwd = Arrays.copyOf(bytePwd, bytePwd.length + 1);
    byte[] xorBytes = new byte[nullFinishedPwd.length];
    int seedLength = seed.length;

    for (int i = 0; i < xorBytes.length; i++) {
      xorBytes[i] = (byte) (nullFinishedPwd[i] ^ seed[i % seedLength]);
    }

    try {
      Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
      cipher.init(Cipher.ENCRYPT_MODE, publicKey);
      return cipher.doFinal(xorBytes);
    } catch (Exception ex) {
      throw new SQLFeatureNotSupportedException(
          "Could not connect using SHA256 plugin : " + ex.getMessage(), "S1009", ex);
    }
  }

  @Override
  public int encode(PacketWriter writer, Context context) throws IOException, SQLException {
    if (password != null) {
      writer.writeBytes(encrypt(publicKey, password, seed));
    }
    writer.flush();
    return 1;
  }

  @Override
  public String description() {
    return "Sha256PasswordPacket{" + ", password=*******" + ", seed=" + Arrays.toString(seed) + '}';
  }
}
