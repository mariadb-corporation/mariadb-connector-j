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

import org.mariadb.jdbc.authentication.AuthenticationPlugin;
import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.com.read.ErrorPacket;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.util.Options;

import javax.crypto.Cipher;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

public class Sha256PasswordPlugin implements AuthenticationPlugin {

  private String authenticationData;
  private Options options;
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
   * @param sequence current exchange sequence
   * @return public key
   * @throws SQLException if server return an Error packet or public key cannot be parsed.
   * @throws IOException if error reading socket
   */
  public static PublicKey readPublicKeyFromSocket(PacketInputStream reader, AtomicInteger sequence)
      throws SQLException, IOException {
    Buffer buffer = reader.getPacket(true);
    sequence.set(reader.getLastPacketSeq());
    switch (buffer.getByteAt(0)) {
      case (byte) 0xFF:
        ErrorPacket ep = new ErrorPacket(buffer);
        String message = ep.getMessage();
        throw new SQLException(
            "Could not connect: " + message, ep.getSqlState(), ep.getErrorNumber());

      case (byte) 0xFE:
        // Erroneous AuthSwitchRequest packet when security exception
        throw new SQLException(
            "Could not connect: receive AuthSwitchRequest in place of RSA public key. "
                + "Did user has the rights to connect to database ?");
      default:
        // AuthMoreData packet
        buffer.skipByte();
        return generatePublicKey(buffer.readRawBytes(buffer.remaining()));
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
   * @param passwordCharacterEncoding password encoding
   * @return encoded password
   * @throws SQLException if cannot encode password
   * @throws UnsupportedEncodingException if password encoding is unknown
   */
  public static byte[] encrypt(
      PublicKey publicKey, String password, byte[] seed, String passwordCharacterEncoding)
      throws SQLException, UnsupportedEncodingException {

    byte[] correctedSeed;
    if (seed.length > 0) {
      // Seed is ended with a null byte value.
      correctedSeed = Arrays.copyOfRange(seed, 0, seed.length - 1);
    } else {
      correctedSeed = new byte[0];
    }

    byte[] bytePwd;
    if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
      bytePwd = password.getBytes(passwordCharacterEncoding);
    } else {
      bytePwd = password.getBytes();
    }

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
   * @param options Connection string options
   */
  public void initialize(String authenticationData, byte[] seed, Options options) {
    this.seed = seed;
    this.authenticationData = authenticationData;
    this.options = options;
  }

  /**
   * Process SHA 256 password plugin authentication. see
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
    } else if (Boolean.TRUE.equals(options.useSsl)) {
      // send clear password
      out.startPacket(sequence.incrementAndGet());
      byte[] bytePwd;
      if (options.passwordCharacterEncoding != null
          && !options.passwordCharacterEncoding.isEmpty()) {
        bytePwd = authenticationData.getBytes(options.passwordCharacterEncoding);
      } else {
        bytePwd = authenticationData.getBytes();
      }

      out.write(bytePwd);
      out.write(0);
      out.flush();
    } else {
      // retrieve public key from configuration or from server
      PublicKey publicKey;
      if (options.serverRsaPublicKeyFile != null && !options.serverRsaPublicKeyFile.isEmpty()) {
        publicKey = readPublicKeyFromFile(options.serverRsaPublicKeyFile);
      } else {
        if (!options.allowPublicKeyRetrieval) {
          throw new SQLException(
              "RSA public key is not available client side (option " + "serverRsaPublicKeyFile)",
              "S1009");
        }

        // ask public Key Retrieval
        out.startPacket(sequence.incrementAndGet());
        out.write((byte) 1);
        out.flush();
        publicKey = readPublicKeyFromSocket(in, sequence);
      }

      try {
        byte[] cipherBytes =
            encrypt(publicKey, authenticationData, seed, options.passwordCharacterEncoding);
        out.startPacket(sequence.incrementAndGet());
        out.write(cipherBytes);
        out.flush();
      } catch (Exception ex) {
        throw new SQLException(
            "Could not connect using SHA256 plugin : " + ex.getMessage(), "S1009", ex);
      }
    }

    Buffer buffer = in.getPacket(true);
    sequence.set(in.getLastPacketSeq());
    return buffer;
  }
}
