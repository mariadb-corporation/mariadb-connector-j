/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.protocol.tls;

import javax.net.ssl.*;
import javax.security.auth.x500.*;
import java.net.*;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.*;
import java.util.*;

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
    return accurateAlias.toArray(new String[accurateAlias.size()]);
  }

  @Override
  public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
    List<String> accurateAlias = searchAccurateAliases(keyType, issuers);
    return accurateAlias.size() > 0 ? accurateAlias.get(0) : null;
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
