//  SPDX-License-Identifier: LGPL-2.1-or-later
//  Copyright (c) 2012-2014 Monty Program Ab
//  Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.standard;

import java.io.IOException;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.SQLException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;

/** Parsec password plugin */
public class ParsecPasswordPlugin implements AuthenticationPlugin {

  private static byte[] pkcs8Ed25519header =
      new byte[] {
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20
      };
  private String authenticationData;
  private byte[] seed;

  @Override
  public String type() {
    return "parsec";
  }

  /**
   * Initialization.
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
  }

  /**
   * Process parsec password plugin authentication. see
   * https://mariadb.com/kb/en/connection/#parsec-plugin
   *
   * @param out out stream
   * @param in in stream
   * @param context connection context
   * @return response packet
   * @throws IOException if socket error
   */
  public ReadableByteBuf process(Writer out, Reader in, Context context)
      throws SQLException, IOException {

    // request ext-salt
    out.writeEmptyPacket();

    ReadableByteBuf buf = in.readReusablePacket();

    byte firstByte = 0;
    int iterations = 100;

    if (buf.readableBytes() > 2) {
      firstByte = buf.readByte();
      iterations = buf.readByte();
    }

    if (firstByte != 0x50 || iterations > 3) {
      // expected 'P' for KDF algorithm (PBKDF2) and maximum iteration of 8192
      throw new SQLException("Wrong parsec authentication format", "S1009");
    }

    byte[] salt = new byte[buf.readableBytes()];
    buf.readBytes(salt);
    char[] password =
        this.authenticationData == null ? new char[0] : this.authenticationData.toCharArray();

    KeyFactory ed25519KeyFactory;
    Signature ed25519Signature;

    try {
      // in case using java 15+
      ed25519KeyFactory = KeyFactory.getInstance("Ed25519");
      ed25519Signature = Signature.getInstance("Ed25519");
    } catch (NoSuchAlgorithmException e) {
      try {
        // java before 15, try using BouncyCastle if present
        ed25519KeyFactory = KeyFactory.getInstance("Ed25519", "BC");
        ed25519Signature = Signature.getInstance("Ed25519", "BC");
      } catch (NoSuchAlgorithmException | NoSuchProviderException ee) {
        throw new SQLException(
            "Parsec authentication not available. Either use Java 15+ or add BouncyCastle"
                + " dependency",
            e);
      }
    }

    try {
      // hash password with PBKDF2
      PBEKeySpec spec = new PBEKeySpec(password, salt, 1024 << iterations, 256);
      SecretKey key = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512").generateSecret(spec);
      byte[] derivedKey = key.getEncoded();

      // create a PKCS8 ED25519 private key with raw secret
      PKCS8EncodedKeySpec keySpec =
          new PKCS8EncodedKeySpec(combineArray(pkcs8Ed25519header, derivedKey));
      PrivateKey privateKey = ed25519KeyFactory.generatePrivate(keySpec);

      // generate client nonce
      byte[] clientScramble = new byte[32];
      SecureRandom.getInstanceStrong().nextBytes(clientScramble);

      // sign concatenation of server nonce + client nonce with private key

      ed25519Signature.initSign(privateKey);
      ed25519Signature.update(combineArray(seed, clientScramble));
      byte[] signature = ed25519Signature.sign();

      // send result to server
      out.writeBytes(clientScramble);
      out.writeBytes(signature);
      out.flush();

      return in.readReusablePacket();

    } catch (NoSuchAlgorithmException
        | InvalidKeySpecException
        | InvalidKeyException
        | SignatureException e) {
      // not expected
      throw new SQLException("Error during parsec authentication", e);
    }
  }

  private byte[] combineArray(byte[] arr1, byte[] arr2) {
    byte[] combined = new byte[arr1.length + arr2.length];
    System.arraycopy(arr1, 0, combined, 0, arr1.length);
    System.arraycopy(arr2, 0, combined, arr1.length, arr2.length);
    return combined;
  }
}
