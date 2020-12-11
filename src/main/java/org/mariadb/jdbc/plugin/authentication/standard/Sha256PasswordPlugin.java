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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.SslMode;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketReader;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPlugin;

public class Sha256PasswordPlugin implements AuthenticationPlugin {

  private String authenticationData;
  private Configuration conf;
  private byte[] seed;

  /**
   * Read public Key from file.
   *
   * @param serverRsaPublicKeyFile RSA public key file
   * @return public key
   * @throws SQLException if cannot read file or file content is not a public key.
   */
  public static PublicKey readPublicKeyFromFile(String serverRsaPublicKeyFile) throws SQLException {
    byte[] keyBytes;
    try {
      keyBytes = Files.readAllBytes(Paths.get(serverRsaPublicKeyFile));
    } catch (IOException ex) {
      throw new SQLException(
          "Could not read server RSA public key from file : "
              + "serverRsaPublicKeyFile="
              + serverRsaPublicKeyFile,
          "S1009",
          ex);
    }
    return generatePublicKey(keyBytes);
  }

  /**
   * Read public Key from socket.
   *
   * @param reader input stream reader
   * @param context connection context
   * @return public key
   * @throws SQLException if server return an Error packet or public key cannot be parsed.
   * @throws IOException if error reading socket
   */
  public static PublicKey readPublicKeyFromSocket(PacketReader reader, Context context)
      throws SQLException, IOException {
    ReadableByteBuf buf = reader.readPacket(true);

    switch (buf.getByte(0)) {
      case (byte) 0xFF:
        ErrorPacket ep = new ErrorPacket(buf, context);
        String message = ep.getMessage();
        throw new SQLException(
            "Could not connect: " + message, ep.getSqlState(), ep.getErrorCode());

      case (byte) 0xFE:
        // Erroneous AuthSwitchRequest packet when security exception
        throw new SQLException(
            "Could not connect: receive AuthSwitchRequest in place of RSA public key. "
                + "Did user has the rights to connect to database ?");
      default:
        // AuthMoreData packet
        buf.skip();
        byte[] authMoreData = new byte[buf.readableBytes()];
        buf.readBytes(authMoreData);
        return generatePublicKey(authMoreData);
    }
  }

  /**
   * Read public pem key from String.
   *
   * @param publicKeyBytes public key bytes value
   * @return public key
   * @throws SQLException if key cannot be parsed
   */
  public static PublicKey generatePublicKey(byte[] publicKeyBytes) throws SQLException {
    try {
      String publicKey =
          new String(publicKeyBytes)
              .replaceAll("(-+BEGIN PUBLIC KEY-+\\r?\\n|\\n?-+END PUBLIC KEY-+\\r?\\n?)", "");
      byte[] keyBytes = Base64.getMimeDecoder().decode(publicKey);
      X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
      KeyFactory kf = KeyFactory.getInstance("RSA");
      return kf.generatePublic(spec);
    } catch (Exception ex) {
      throw new SQLException("Could read server RSA public key: " + ex.getMessage(), "S1009", ex);
    }
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
  public static byte[] encrypt(PublicKey publicKey, String password, byte[] seed)
      throws SQLException {

    byte[] correctedSeed;
    if (seed.length > 0) {
      // Seed is ended with a null byte value.
      correctedSeed = Arrays.copyOfRange(seed, 0, seed.length - 1);
    } else {
      correctedSeed = new byte[0];
    }

    byte[] bytePwd = password.getBytes(StandardCharsets.UTF_8);

    byte[] nullFinishedPwd = Arrays.copyOf(bytePwd, bytePwd.length + 1);
    byte[] xorBytes = new byte[nullFinishedPwd.length];
    int seedLength = correctedSeed.length;

    for (int i = 0; i < xorBytes.length; i++) {
      xorBytes[i] = (byte) (nullFinishedPwd[i] ^ correctedSeed[i % seedLength]);
    }

    try {
      Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
      cipher.init(Cipher.ENCRYPT_MODE, publicKey);
      return cipher.doFinal(xorBytes);
    } catch (Exception ex) {
      throw new SQLException(
          "Could not connect using SHA256 plugin : " + ex.getMessage(), "S1009", ex);
    }
  }

  @Override
  public String name() {
    return "Sha256 authentication plugin";
  }

  @Override
  public String type() {
    return "sha256_password";
  }

  /**
   * Initialization.
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
   * Process SHA 256 password plugin authentication. see
   * https://mariadb.com/kb/en/library/authentication-plugin-ed25519/
   *
   * @param out out stream
   * @param in in stream
   * @param context connection context
   * @return response packet
   * @throws IOException if socket error
   */
  public ReadableByteBuf process(PacketWriter out, PacketReader in, Context context)
      throws SQLException, IOException {

    if (authenticationData == null || authenticationData.isEmpty()) {
      out.writeEmptyPacket();
    } else if (SslMode.DISABLE != conf.sslMode()) {
      // send clear password

      byte[] bytePwd = authenticationData.getBytes(StandardCharsets.UTF_8);
      out.writeBytes(bytePwd);
      out.writeByte(0);
      out.flush();
    } else {
      // retrieve public key from configuration or from server
      PublicKey publicKey;
      if (conf.serverRsaPublicKeyFile() != null && !conf.serverRsaPublicKeyFile().isEmpty()) {
        publicKey = readPublicKeyFromFile(conf.serverRsaPublicKeyFile());
      } else {
        if (!conf.allowPublicKeyRetrieval()) {
          throw new SQLException(
              "RSA public key is not available client side (option " + "serverRsaPublicKeyFile)",
              "S1009");
        }

        // ask public Key Retrieval
        out.writeByte(1);
        out.flush();
        publicKey = readPublicKeyFromSocket(in, context);
      }

      try {
        byte[] cipherBytes = encrypt(publicKey, authenticationData, seed);
        out.writeBytes(cipherBytes);
        out.flush();
      } catch (Exception ex) {
        throw new SQLException(
            "Could not connect using SHA256 plugin : " + ex.getMessage(), "S1009", ex);
      }
    }

    return in.readPacket(true);
  }
}
