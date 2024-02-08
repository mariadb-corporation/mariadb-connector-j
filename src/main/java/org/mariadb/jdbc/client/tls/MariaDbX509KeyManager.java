// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client.tls;

import java.net.Socket;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.*;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.security.auth.x500.X500Principal;

/**
 * Key manager implementation that implement only client verification and rely only on private key
 * for mutual authentication, without Server Name Indication (SNI) verification.
 */
public class MariaDbX509KeyManager extends X509ExtendedKeyManager {

  private final Hashtable<String, KeyStore.PrivateKeyEntry> privateKeyHash = new Hashtable<>();

  /**
   * Creates Key manager.
   *
   * @param keyStore keyStore (must have been initialized)
   * @param pwd keyStore password
   * @throws KeyStoreException if keyStore hasn't been initialized.
   */
  public MariaDbX509KeyManager(KeyStore keyStore, char[] pwd) throws KeyStoreException {
    super();
    Enumeration<String> aliases = keyStore.aliases();
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement();
      if (keyStore.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class)) {
        try {
          privateKeyHash.put(
              alias,
              (KeyStore.PrivateKeyEntry)
                  keyStore.getEntry(alias, new KeyStore.PasswordProtection(pwd)));
        } catch (UnrecoverableEntryException | NoSuchAlgorithmException unrecoverableEntryEx) {
          // password invalid | algorithm error
        }
      }
    }
  }

  @Override
  public String[] getClientAliases(String keyType, Principal[] issuers) {
    List<String> accurateAlias = searchAccurateAliases(new String[] {keyType}, issuers);
    if (accurateAlias.size() == 0) {
      return null;
    }
    return accurateAlias.toArray(new String[0]);
  }

  @Override
  public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
    List<String> accurateAlias = searchAccurateAliases(keyType, issuers);
    return accurateAlias == null || accurateAlias.isEmpty() ? null : accurateAlias.get(0);
  }

  @Override
  public X509Certificate[] getCertificateChain(String alias) {
    KeyStore.PrivateKeyEntry keyEntry = privateKeyHash.get(alias);
    if (keyEntry == null) {
      return null;
    }

    Certificate[] certs = keyEntry.getCertificateChain();
    if (certs.length > 0 && certs[0] instanceof X509Certificate) {
      return Arrays.copyOf(certs, certs.length, X509Certificate[].class);
    }

    return null;
  }

  @Override
  public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
    return chooseClientAlias(keyType, issuers, null);
  }

  @Override
  public PrivateKey getPrivateKey(String alias) {
    KeyStore.PrivateKeyEntry keyEntry = privateKeyHash.get(alias);
    if (keyEntry == null) {
      return null;
    }
    return keyEntry.getPrivateKey();
  }

  /**
   * Search aliases corresponding to algorithms and issuers.
   *
   * @param keyTypes list of algorithms
   * @param issuers list of issuers;
   * @return list of corresponding aliases
   */
  private ArrayList<String> searchAccurateAliases(String[] keyTypes, Principal[] issuers) {
    if (keyTypes == null || keyTypes.length == 0) {
      return null;
    }

    ArrayList<String> accurateAliases = new ArrayList<>();
    for (Map.Entry<String, KeyStore.PrivateKeyEntry> mapEntry : privateKeyHash.entrySet()) {

      Certificate[] certs = mapEntry.getValue().getCertificateChain();
      String alg = certs[0].getPublicKey().getAlgorithm();

      for (String keyType : keyTypes) {
        if (alg.equals(keyType)) {
          if (issuers != null && issuers.length != 0) {
            checkLoop:
            for (Certificate cert : certs) {
              if (cert instanceof X509Certificate) {
                X500Principal certificateIssuer = ((X509Certificate) cert).getIssuerX500Principal();
                for (Principal issuer : issuers) {
                  if (certificateIssuer.equals(issuer)) {
                    accurateAliases.add(mapEntry.getKey());
                    break checkLoop;
                  }
                }
              }
            }
          } else {
            accurateAliases.add(mapEntry.getKey());
          }
        }
      }
    }
    return accurateAliases;
  }

  @Override
  public String[] getServerAliases(String keyType, Principal[] issuers) {
    return null; // Driver use only client side
  }

  @Override
  public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
    return null; // Driver use only client side
  }

  @Override
  public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
    return null; // Driver use only client side
  }
}
