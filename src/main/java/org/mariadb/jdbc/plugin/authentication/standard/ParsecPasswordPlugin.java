// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.standard;

import java.io.IOException;
import java.security.*;
import java.security.spec.*;
import java.sql.SQLException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;
import org.mariadb.jdbc.plugin.Credential;

/** Parsec password plugin */
public class ParsecPasswordPlugin implements AuthenticationPlugin {

  private static final byte[] pkcs8Ed25519header =
      new byte[] {
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20
      };

  /**
   * Deliberately conservative PBKDF2-HMAC-SHA512 throughput reference: 262144 rounds measured in
   * 225ms. Used to convert the connection time budget into a maximum iteration factor.
   */
  private static final long PBKDF2_REFERENCE_ROUNDS = 262_144L;

  private static final long PBKDF2_REFERENCE_MS = 225L;

  /** Time budget used when connectTimeout is disabled (zero). */
  private static final int DEFAULT_CONNECT_BUDGET_MS = 10_000;

  /**
   * Absolute ceiling, whatever the time budget: a factor of 22 would overflow the {@code 1024 <<
   * iterations} round count.
   */
  private static final int MAX_ITERATION_FACTOR = 20;

  private final String authenticationData;
  private final byte[] seed;
  private byte[] hash;

  /**
   * Initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   */
  public ParsecPasswordPlugin(String authenticationData, byte[] seed) {
    this.seed = seed;
    this.authenticationData = authenticationData;
  }

  /**
   * Process parsec password plugin authentication. see <a
   * href="https://mariadb.com/kb/en/connection/#parsec-plugin">parsec-plugin</a>
   *
   * @param out out stream
   * @param in in stream
   * @param context connection context
   * @param sslFingerPrintValidation true if SSL certificate fingerprint validation is enabled
   * @return response packet
   * @throws IOException if socket error
   */
  public ReadableByteBuf process(
      Writer out, Reader in, Context context, boolean sslFingerPrintValidation)
      throws SQLException, IOException {

    // request ext-salt
    out.writeEmptyPacket();

    ReadableByteBuf buf = in.readReusablePacket();

    byte firstByte = 0;
    int iterations = 100;

    if (buf.getByte() == 0x01) buf.skip();

    if (buf.readableBytes() > 2) {
      firstByte = buf.readByte();
      iterations = buf.readUnsignedByte();
    }

    if (firstByte != 0x50) {
      // expected 'P' for KDF algorithm (PBKDF2)
      throw new SQLException("Wrong parsec authentication format", "S1009");
    }

    // a rogue server must not be able to pin a client core for minutes: the PBKDF2 cost is
    // computed before authentication completes, without any interruptible socket read.
    int budgetMs = connectBudgetMs(context.getConf().connectTimeout());
    int maxIterations = maxIterationFactor(budgetMs);
    if (iterations > maxIterations) {
      throw new SQLException(
          String.format(
              "Wrong parsec authentication format: server requested iteration factor %s (%s PBKDF2"
                  + " rounds), maximum permitted is %s for a connection time budget of %sms",
              iterations, 1024L << iterations, maxIterations, budgetMs),
          "S1009");
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

      byte[] rawPublicKey = ParsecPasswordPluginTool.process(derivedKey);

      hash =
          combineArray(
              combineArray(new byte[] {(byte) 'P', (byte) iterations}, salt), rawPublicKey);

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
        | InvalidAlgorithmParameterException
        | SignatureException e) {
      // not expected
      throw new SQLException("Error during parsec authentication", e);
    }
  }

  /**
   * Connection time budget the PBKDF2 hashing must fit in: the configured connectTimeout, or a
   * default budget when connectTimeout is disabled (zero).
   *
   * @param connectTimeout configured connect timeout, in milliseconds
   * @return time budget in milliseconds
   */
  static int connectBudgetMs(int connectTimeout) {
    return connectTimeout > 0 ? connectTimeout : DEFAULT_CONNECT_BUDGET_MS;
  }

  /**
   * Maximum iteration factor a server may request, derived from the connection time budget. The
   * factor is an exponent: hashing costs {@code 1024 << factor} PBKDF2-HMAC-SHA512 rounds, which
   * are computed before authentication completes, with no interruptible socket read in between. The
   * bound therefore scales with connectTimeout: a client declaring a longer budget permits a larger
   * factor (100ms&rarr;6, 2500ms&rarr;11, 10s&rarr;13, 30s&rarr;15).
   *
   * @param budgetMs connection time budget, in milliseconds
   * @return maximum permitted iteration factor
   */
  static int maxIterationFactor(int budgetMs) {
    long maxRounds = PBKDF2_REFERENCE_ROUNDS * budgetMs / PBKDF2_REFERENCE_MS;
    long factorBase = maxRounds / 1024L;
    if (factorBase < 1) return 0;
    // floor(log2(factorBase))
    int factor = 63 - Long.numberOfLeadingZeros(factorBase);
    return Math.min(factor, MAX_ITERATION_FACTOR);
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
    return hash;
  }

  private byte[] combineArray(byte[] arr1, byte[] arr2) {
    byte[] combined = new byte[arr1.length + arr2.length];
    System.arraycopy(arr1, 0, combined, 0, arr1.length);
    System.arraycopy(arr2, 0, combined, arr1.length, arr2.length);
    return combined;
  }
}
