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
 */

package org.mariadb.jdbc.plugin.authentication.standard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.sql.SQLException;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.SslMode;
import org.mariadb.jdbc.client.PacketReader;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.message.client.AuthMoreRawPacket;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPlugin;

public class CachingSha2PasswordPlugin implements AuthenticationPlugin {

  public static final String TYPE = "caching_sha2_password";

  private String authenticationData;
  private byte[] seed;
  private Configuration conf;

  /**
   * Send a SHA-2 encrypted password. encryption XOR(SHA256(password), SHA256(seed,
   * SHA256(SHA256(password))))
   *
   * @param password password
   * @param seed seed
   * @return encrypted pwd
   * @throws NoSuchAlgorithmException if SHA-256 algorithm is unknown
   */
  public static byte[] sha256encryptPassword(final String password, final byte[] seed)
      throws NoSuchAlgorithmException {

    if (password == null || password.isEmpty()) {
      return new byte[0];
    }

    final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
    byte[] bytePwd = password.getBytes(StandardCharsets.UTF_8);

    final byte[] stage1 = messageDigest.digest(bytePwd);
    messageDigest.reset();

    final byte[] stage2 = messageDigest.digest(stage1);
    messageDigest.reset();

    messageDigest.update(stage2);
    messageDigest.update(seed);

    final byte[] digest = messageDigest.digest();
    final byte[] returnBytes = new byte[digest.length];
    for (int i = 0; i < digest.length; i++) {
      returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
    }
    return returnBytes;
  }

  /**
   * Send a SHA-2 encrypted password. encryption XOR(SHA256(password), SHA256(seed,
   * SHA256(SHA256(password))))
   *
   * @param password password
   * @param seed seed
   * @return encrypted pwd
   */
  public static byte[] sha256encryptPassword(final CharSequence password, final byte[] seed) {

    if (password == null || password.length() == 0) {
      return new byte[0];
    }
    byte[] truncatedSeed = new byte[seed.length - 1];
    System.arraycopy(seed, 0, truncatedSeed, 0, seed.length - 1);
    try {
      final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
      byte[] bytePwd = password.toString().getBytes(StandardCharsets.UTF_8);

      final byte[] stage1 = messageDigest.digest(bytePwd);
      messageDigest.reset();

      final byte[] stage2 = messageDigest.digest(stage1);
      messageDigest.reset();

      messageDigest.update(stage2);
      messageDigest.update(truncatedSeed);

      final byte[] digest = messageDigest.digest();
      final byte[] returnBytes = new byte[digest.length];
      for (int i = 0; i < digest.length; i++) {
        returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
      }
      return returnBytes;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not use SHA-256, failing", e);
    }
  }

  @Override
  public String name() {
    return "caching sha2 password";
  }

  @Override
  public String type() {
    return TYPE;
  }

  /**
   * Initialized data.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param conf Connection string options
   */
  public void initialize(String authenticationData, byte[] seed, Configuration conf) {
    this.seed = seed;
    this.authenticationData = authenticationData;
    this.conf = conf;
  }

  /**
   * Process native password plugin authentication. see
   * https://mariadb.com/kb/en/library/authentication-plugin-mysql_native_password/
   *
   * @param out out stream
   * @param in in stream
   * @param context connection context
   * @return response packet
   * @throws IOException if socket error
   */
  public ReadableByteBuf process(PacketWriter out, PacketReader in, Context context)
      throws IOException, SQLException {
    if (authenticationData == null || authenticationData.isEmpty()) {
      out.writeEmptyPacket();
    } else {
      try {
        byte[] fastCryptPwd = sha256encryptPassword(authenticationData, seed);
        new AuthMoreRawPacket(fastCryptPwd).encode(out, context);
        out.flush();
      } catch (NoSuchAlgorithmException e) {
        throw new SQLException("Could not use SHA-256, failing", e);
      }
    }

    ReadableByteBuf buf = in.readPacket(true);

    switch (buf.getByte()) {

        // success or error
      case (byte) 0x00:
      case (byte) 0xFF:
        return buf;

        // fast authentication result
      default:
        byte[] authResult = new byte[buf.readLengthNotNull()];
        buf.readBytes(authResult);
        switch (authResult[0]) {
          case 3:
            return in.readPacket(true);
          case 4:
            if (conf.sslMode() != SslMode.DISABLE) {
              // send clear password

              byte[] bytePwd = authenticationData.getBytes();
              byte[] nullEndedValue = new byte[bytePwd.length + 1];
              System.arraycopy(bytePwd, 0, nullEndedValue, 0, bytePwd.length);
              new AuthMoreRawPacket(nullEndedValue).encode(out, context);
              out.flush();

            } else {
              // retrieve public key from configuration or from server
              PublicKey publicKey;
              if (conf.serverRsaPublicKeyFile() != null
                  && !conf.serverRsaPublicKeyFile().isEmpty()) {
                publicKey =
                    Sha256PasswordPlugin.readPublicKeyFromFile(conf.serverRsaPublicKeyFile());
              } else {
                if (!conf.allowPublicKeyRetrieval()) {
                  throw new SQLException(
                      "RSA public key is not available client side (option serverRsaPublicKeyFile not set)",
                      "S1009");
                }

                // ask public Key Retrieval
                out.writeByte(2);
                out.flush();

                publicKey = Sha256PasswordPlugin.readPublicKeyFromSocket(in, context);
              }

              try {
                byte[] cipherBytes =
                    Sha256PasswordPlugin.encrypt(publicKey, authenticationData, seed);
                out.writeBytes(cipherBytes);
                out.flush();
              } catch (Exception ex) {
                throw new SQLException(
                    "Could not connect using SHA256 plugin : " + ex.getMessage(), "S1009", ex);
              }
            }

            return in.readPacket(true);

          default:
            throw new SQLException(
                "Protocol exchange error. Expect login success or RSA login request message",
                "S1009");
        }
    }
  }
}
