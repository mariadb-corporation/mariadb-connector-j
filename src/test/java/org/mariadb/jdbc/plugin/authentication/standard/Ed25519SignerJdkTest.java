// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.authentication.standard;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Cross-checks the ref10 based signer against the JDK Ed25519 provider (JEP 339, Java 15+) on
 * random seeds and message sizes, including the 64-byte server nonce + client nonce message that
 * parsec signs. Skipped on Java versions without an Ed25519 provider.
 */
public class Ed25519SignerJdkTest {

  /** DER prefix of a PKCS#8 Ed25519 private key, followed by the 32-byte seed. */
  private static final byte[] PKCS8_PREFIX = {
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20
  };

  /** DER prefix of a SubjectPublicKeyInfo Ed25519 public key, followed by the 32-byte point. */
  private static final byte[] SPKI_PREFIX = {
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00
  };

  @Test
  public void matchesJdkProvider() throws Exception {
    KeyFactory keyFactory;
    Signature jdk;
    try {
      keyFactory = KeyFactory.getInstance("Ed25519");
      jdk = Signature.getInstance("Ed25519");
    } catch (NoSuchAlgorithmException e) {
      Assumptions.abort("no Ed25519 provider on this Java version");
      return;
    }

    SecureRandom random = new SecureRandom();
    int[] messageLengths = {0, 1, 31, 32, 63, 64, 65, 127, 128, 1000};
    for (int length : messageLengths) {
      byte[] seed = new byte[32];
      random.nextBytes(seed);
      byte[] message = new byte[length];
      random.nextBytes(message);

      byte[] expandedKey = Ed25519Signer.expand(seed);
      byte[] publicKey = Ed25519Signer.publicKey(expandedKey);
      byte[] signature = Ed25519Signer.sign(expandedKey, publicKey, message);

      // the JDK accepts our signature for our public key
      PublicKey jdkPublic =
          keyFactory.generatePublic(new X509EncodedKeySpec(concat(SPKI_PREFIX, publicKey)));
      jdk.initVerify(jdkPublic);
      jdk.update(message);
      assertTrue(jdk.verify(signature), "JDK rejected signature for message length " + length);

      // Ed25519 is deterministic: the JDK produces the very same bytes from the same seed
      PrivateKey jdkPrivate =
          keyFactory.generatePrivate(new PKCS8EncodedKeySpec(concat(PKCS8_PREFIX, seed)));
      jdk.initSign(jdkPrivate);
      jdk.update(message);
      assertArrayEquals(jdk.sign(), signature, "signature differs for message length " + length);

      // and a tampered message is rejected, so the check above is not vacuous
      if (length > 0) {
        message[0] ^= 1;
        jdk.initVerify(jdkPublic);
        jdk.update(message);
        assertTrue(!jdk.verify(signature));
      }
    }
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] out = new byte[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
