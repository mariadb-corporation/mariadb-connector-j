// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.standard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.export.SslMode;
import org.mariadb.jdbc.message.client.AuthMoreRawPacket;
import org.mariadb.jdbc.message.server.AuthSwitchPacket;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;

/** Mysql caching sha2 password plugin */
public class CachingSha2PasswordPlugin implements AuthenticationPlugin {

  /** plugin name */
  public static final String TYPE = "caching_sha2_password";

  private String authenticationData;
  private byte[] seed;
  private Configuration conf;
  private HostAddress hostAddress;

  public CachingSha2PasswordPlugin(
      String authenticationData, byte[] seed, Configuration conf, HostAddress hostAddress) {
    this.authenticationData = authenticationData;
    this.seed = seed;
    this.conf = conf;
    this.hostAddress = hostAddress;
  }

  /**
   * Send an SHA-2 encrypted password. encryption XOR(SHA256(password), SHA256(seed,
   * SHA256(SHA256(password))))
   *
   * @param password password
   * @param seed seed
   * @return encrypted pwd
   */
  public static byte[] sha256encryptPassword(final CharSequence password, final byte[] seed) {
    if (password == null) return new byte[0];
    byte[] truncatedSeed = AuthSwitchPacket.getTruncatedSeed(seed);
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

  /**
   * Read public Key from file.
   *
   * @param serverRsaPublicKeyFile RSA public key file
   * @return public key
   * @throws SQLException if having an error reading file or file content is not a public key.
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
   * Read public pem key from String.
   *
   * @param publicKeyBytes public key bytes value
   * @return public key
   * @throws SQLException if key cannot be parsed
   */
  public static PublicKey generatePublicKey(byte[] publicKeyBytes) throws SQLException {
    try {
      String publicKey =
          new String(publicKeyBytes, StandardCharsets.US_ASCII)
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

    byte[] correctedSeed = Arrays.copyOfRange(seed, 0, seed.length - 1);
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
          "Error encoding password with public key : " + ex.getMessage(), "S1009", ex);
    }
  }

  /**
   * Initialized data.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param conf Connection string options
   * @param hostAddress host information
   */
  public void initialize(
      String authenticationData, byte[] seed, Configuration conf, HostAddress hostAddress) {
    this.seed = seed;
    this.authenticationData = authenticationData;
    this.conf = conf;
    this.hostAddress = hostAddress;
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
  public ReadableByteBuf process(Writer out, Reader in, Context context)
      throws IOException, SQLException {
    byte[] fastCryptPwd = sha256encryptPassword(authenticationData, seed);
    new AuthMoreRawPacket(fastCryptPwd).encode(out, context);

    ReadableByteBuf buf = in.readReusablePacket();

    switch (buf.getByte()) {
      case (byte) 0x00:
      case (byte) 0xFF:
        // success or error
        return buf;

      default:
        // fast authentication result
        byte[] authResult = new byte[buf.readIntLengthEncodedNotNull()];
        buf.readBytes(authResult);
        switch (authResult[0]) {
          case 3:
            return in.readReusablePacket();
          case 4:
            SslMode sslMode = hostAddress.sslMode == null ? conf.sslMode() : hostAddress.sslMode;
            if (sslMode != SslMode.DISABLE) {
              // send clear password

              byte[] bytePwd = authenticationData.getBytes();
              byte[] nullEndedValue = new byte[bytePwd.length + 1];
              System.arraycopy(bytePwd, 0, nullEndedValue, 0, bytePwd.length);
              new AuthMoreRawPacket(nullEndedValue).encode(out, context);
              out.flush();

            } else {
              // retrieve public key from configuration or from server
              PublicKey publicKey;
              if (conf.serverRsaPublicKeyFile() != null) {
                if (conf.serverRsaPublicKeyFile().contains("BEGIN PUBLIC KEY")) {
                  publicKey = generatePublicKey(conf.serverRsaPublicKeyFile().getBytes());
                } else {
                  publicKey = readPublicKeyFromFile(conf.serverRsaPublicKeyFile());
                }
              } else {
                // read public key from socket
                if (!conf.allowPublicKeyRetrieval()) {
                  throw new SQLException(
                      "RSA public key is not available client side (option serverRsaPublicKeyFile"
                          + " not set)",
                      "S1009");
                }

                // ask public Key Retrieval
                out.writeByte(2);
                out.flush();

                buf = in.readReusablePacket();
                switch (buf.getByte(0)) {
                  case (byte) 0xFF:
                  case (byte) 0xFE:
                    return buf;

                  default:
                    // AuthMoreData packet
                    buf.skip();
                    byte[] authMoreData = new byte[buf.readableBytes()];
                    buf.readBytes(authMoreData);
                    publicKey = generatePublicKey(authMoreData);
                }
              }

              byte[] cipherBytes = encrypt(publicKey, authenticationData, seed);
              out.writeBytes(cipherBytes);
              out.flush();
            }

            return in.readReusablePacket();

          default:
            throw new SQLException(
                "Protocol exchange error. Expect login success or RSA login request message",
                "S1009");
        }
    }
  }
}
