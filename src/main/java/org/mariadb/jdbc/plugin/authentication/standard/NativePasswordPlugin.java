// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.standard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.message.server.AuthSwitchPacket;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;
import org.mariadb.jdbc.plugin.Credential;

/** Native password implementation */
public class NativePasswordPlugin implements AuthenticationPlugin {

  /** plugin name */
  public static final String TYPE = "mysql_native_password";

  private String authenticationData;
  private byte[] seed;

  /**
   * Encrypts a password.
   *
   * <p>protocol for authentication is like this:
   *
   * <ul>
   *   <li>Server sends a random array of bytes (the seed)
   *   <li>client makes a sha1 digest of the password
   *   <li>client hashes the output of 2
   *   <li>client digests the seed
   *   <li>client updates the digest with the output from 3
   *   <li>an xor of the output of 5 and 2 is sent to server
   *   <li>server does the same thing and verifies that the scrambled passwords match
   * </ul>
   *
   * @param password the password to encrypt
   * @param seed the seed to use
   * @return a scrambled password
   */
  public static byte[] encryptPassword(final CharSequence password, final byte[] seed) {
    try {
      if (password == null) return new byte[0];

      final MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
      byte[] bytePwd = password.toString().getBytes(StandardCharsets.UTF_8);

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
  public ReadableByteBuf process(Writer out, Reader in, Context context) throws IOException {
    if (authenticationData == null) {
      out.writeEmptyPacket();
    } else {
      byte[] truncatedSeed = AuthSwitchPacket.getTruncatedSeed(seed);
      out.writeBytes(encryptPassword(authenticationData, truncatedSeed));
      out.flush();
    }

    return in.readReusablePacket();
  }

  public boolean isMitMProof() {
    return true;
  }

  /**
   * Return Hash
   *
   * @param credential Credential
   * @return hash
   */
  public byte[] hash(Credential credential) {
    try {
      final MessageDigest messageDigestSHA1 = MessageDigest.getInstance("SHA-1");
      byte[] bytePwd = credential.getPassword().getBytes(StandardCharsets.UTF_8);
      final byte[] stage1 = messageDigestSHA1.digest(bytePwd);
      messageDigestSHA1.reset();
      final byte[] stage2 = messageDigestSHA1.digest(stage1);
      messageDigestSHA1.reset();
      return stage2;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not use SHA-1, failing", e);
    }
  }
}
