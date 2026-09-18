// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.authentication.standard;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.math.ed25519.Ed25519ScalarOps;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.spec.EdDSANamedCurveTable;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.spec.EdDSAParameterSpec;

/**
 * Ed25519 signing (RFC 8032) on top of the vendored ref10 implementation, shared by the
 * client_ed25519 and parsec authentication plugins so that neither depends on a JCA provider.
 *
 * <p>The secret is expanded once with SHA-512 as in RFC 8032 section 5.1.5. parsec passes the
 * 32-byte PBKDF2-derived seed, which yields standard signatures; client_ed25519 passes the raw
 * password in place of the seed, which is the only difference of that scheme with RFC 8032.
 */
final class Ed25519Signer {

  private static final EdDSAParameterSpec SPEC =
      Objects.requireNonNull(
          EdDSANamedCurveTable.getByName("Ed25519"), "Ed25519 curve not registered");
  private static final Ed25519ScalarOps SCALAR_OPS = new Ed25519ScalarOps();

  private Ed25519Signer() {}

  /**
   * Expand a secret into the 64-byte private key material: SHA-512 of the secret, whose first 32
   * bytes are clamped into the scalar and whose last 32 bytes are the nonce prefix.
   *
   * @param secret seed (RFC 8032) or raw password (client_ed25519)
   * @return 64-byte expanded key
   */
  static byte[] expand(byte[] secret) {
    byte[] expanded = sha512().digest(secret);
    expanded[0] &= (byte) 248;
    expanded[31] &= 63;
    expanded[31] |= 64;
    return expanded;
  }

  /**
   * Public key A = a·B for the expanded key.
   *
   * @param expandedKey expanded key from {@link #expand(byte[])}
   * @return 32-byte encoded public key
   */
  static byte[] publicKey(byte[] expandedKey) {
    return SPEC.getB().scalarMultiply(expandedKey).toByteArray();
  }

  /**
   * Sign a message.
   *
   * @param expandedKey expanded key from {@link #expand(byte[])}
   * @param publicKey public key from {@link #publicKey(byte[])}
   * @param message message to sign
   * @return 64-byte signature R || S
   */
  static byte[] sign(byte[] expandedKey, byte[] publicKey, byte[] message) {
    MessageDigest hash = sha512();
    hash.update(expandedKey, 32, 32);
    hash.update(message);
    byte[] r = SCALAR_OPS.reduce(hash.digest());
    byte[] encodedR = SPEC.getB().scalarMultiply(r).toByteArray();

    hash.update(encodedR);
    hash.update(publicKey);
    hash.update(message);
    byte[] k = SCALAR_OPS.reduce(hash.digest());
    byte[] s = SCALAR_OPS.multiplyAndAdd(k, expandedKey, r);

    byte[] signature = new byte[64];
    System.arraycopy(encodedR, 0, signature, 0, 32);
    System.arraycopy(s, 0, signature, 32, 32);
    return signature;
  }

  private static MessageDigest sha512() {
    try {
      return MessageDigest.getInstance("SHA-512");
    } catch (NoSuchAlgorithmException e) {
      // SHA-512 is mandatory for every Java platform implementation
      throw new IllegalStateException("Could not use SHA-512, failing", e);
    }
  }
}
