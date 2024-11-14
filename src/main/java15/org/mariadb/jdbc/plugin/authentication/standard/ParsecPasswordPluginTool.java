// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.standard;

import java.io.IOException;
import java.security.*;
import java.security.spec.NamedParameterSpec;
import java.sql.SQLException;
import java.util.Arrays;

/** Parsec password plugin utility*/
public class ParsecPasswordPluginTool {

  public static byte[] process(byte[] rawPrivateKey)
          throws SQLException, IOException, InvalidAlgorithmParameterException, NoSuchAlgorithmException {

    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
    keyPairGenerator.initialize(NamedParameterSpec.ED25519, new StaticSecureRandom(rawPrivateKey));

    // public key in SPKI format; the last 32 bytes are the raw public key
    byte[] spki =
            keyPairGenerator
                    .generateKeyPair()
                    .getPublic()
                    .getEncoded();
    byte[] rawPublicKey =
            Arrays.copyOfRange(spki, spki.length - 32, spki.length);
    return rawPublicKey;
  }

  private static class StaticSecureRandom extends SecureRandom {
    private byte[] privateKey;

    public StaticSecureRandom(byte[] privateKey) {
      this.privateKey = privateKey;
    }

    public void nextBytes(byte[] bytes) {
      System.arraycopy(privateKey, 0, bytes, 0, privateKey.length);
    }
  }
}
