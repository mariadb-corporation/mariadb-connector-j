// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.standard;

import java.io.IOException;
import java.security.*;
import java.sql.SQLException;
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters;

/** Parsec password plugin utility */
public class ParsecPasswordPluginTool {

  public static byte[] process(byte[] rawPrivateKey)
      throws SQLException,
          IOException,
          InvalidAlgorithmParameterException,
          NoSuchAlgorithmException {
    Ed25519PrivateKeyParameters privateKeyRebuild =
        new Ed25519PrivateKeyParameters(rawPrivateKey, 0);
    Ed25519PublicKeyParameters publicKeyRebuild = privateKeyRebuild.generatePublicKey();
    return publicKeyRebuild.getEncoded();
  }
}
