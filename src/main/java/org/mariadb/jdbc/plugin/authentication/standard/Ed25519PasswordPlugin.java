// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.authentication.standard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;
import org.mariadb.jdbc.plugin.Credential;

/** ED25519 password plugin */
public class Ed25519PasswordPlugin implements AuthenticationPlugin {

  private final String authenticationData;
  private final byte[] seed;

  /**
   * Sign the server seed with the password. This is an RFC 8032 signature where the raw password
   * takes the place of the 32-byte secret seed.
   *
   * @param password password
   * @param seed server seed
   * @return 64-byte signature
   */
  private static byte[] ed25519SignWithPassword(final String password, final byte[] seed) {
    byte[] expandedKey = Ed25519Signer.expand(password.getBytes(StandardCharsets.UTF_8));
    return Ed25519Signer.sign(expandedKey, Ed25519Signer.publicKey(expandedKey), seed);
  }

  public Ed25519PasswordPlugin(String authenticationData, byte[] seed) {
    this.seed = seed;
    this.authenticationData = authenticationData;
  }

  /**
   * Process Ed25519 password plugin authentication. see <a
   * href="https://mariadb.com/kb/en/library/authentication-plugin-ed25519/">authentication-plugin-ed25519</a>
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
    // the server expects a 64-byte signature whatever the account password: an account created
    // with PASSWORD('') is authenticated by a signature over the empty password, as Connector/C
    out.writeBytes(
        ed25519SignWithPassword(authenticationData == null ? "" : authenticationData, seed));
    out.flush();

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
    String password = credential.getPassword() == null ? "" : credential.getPassword();
    return Ed25519Signer.publicKey(Ed25519Signer.expand(password.getBytes(StandardCharsets.UTF_8)));
  }
}
