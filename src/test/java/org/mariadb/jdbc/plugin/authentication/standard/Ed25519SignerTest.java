// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.authentication.standard;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.Utils;

public class Ed25519SignerTest {

  /** RFC 8032 section 7.1 test vectors: secret seed, public key, message, signature. */
  private static final String[][] RFC_8032_VECTORS = {
    {
      "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
      "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
      "",
      "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
    },
    {
      "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb",
      "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c",
      "72",
      "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00"
    },
    {
      "c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7",
      "fc51cd8e6218a1a38da47ed00230f0580816ed13ba3303ac5deb911548908025",
      "af82",
      "6291d657deec24024827e69c3abe01a30ce548a284743a445e3680d7db5ac3ac18ff9b538d16f290ae67f760984dc6594a7c15e9716ed28dc027beceea1ec40a"
    }
  };

  /** parsec: a 32-byte seed must yield standard RFC 8032 keys and signatures. */
  @Test
  public void rfc8032TestVectors() {
    for (String[] vector : RFC_8032_VECTORS) {
      byte[] expandedKey = Ed25519Signer.expand(Utils.hexToBytes(vector[0]));
      byte[] publicKey = Ed25519Signer.publicKey(expandedKey);
      assertEquals(vector[1], Utils.bytesToHex(publicKey));
      byte[] signature = Ed25519Signer.sign(expandedKey, publicKey, Utils.hexToBytes(vector[2]));
      assertEquals(vector[3], Utils.bytesToHex(signature));
    }
  }

  /**
   * client_ed25519: the public key of a raw password must match the server's ed25519_password().
   */
  @Test
  public void clientEd25519PasswordHash() {
    byte[] expandedKey = Ed25519Signer.expand("secret".getBytes(StandardCharsets.UTF_8));
    // SELECT ed25519_password('secret') on a server with the auth_ed25519 plugin
    assertEquals(
        "ZIgUREUg5PVgQ6LskhXmO+eZLS0nC8be6HPjYWR4YJY",
        Base64.getEncoder().withoutPadding().encodeToString(Ed25519Signer.publicKey(expandedKey)));
  }
}
