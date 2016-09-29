/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2015 Avaya Inc.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:


Copyright (c) 2009-2011, Marcus Eriksson, Stephane Giron, Marc Isambart, Trond Norbye

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.protocol.tls;

import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.SqlStates;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.*;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.UUID;

public class MariaDbX509TrustManager implements X509TrustManager {
    private X509TrustManager trustManager;

    /**
     * MyX509TrustManager.
     * @param options parsed url options
     * @throws QueryException exception
     */
    public MariaDbX509TrustManager(Options options) throws QueryException {
        //if trustServerCertificate is true, will have a X509TrustManager implementation that validate all.
        if (options.trustServerCertificate) return;

        KeyStore ks;
        try {
            ks = KeyStore.getInstance(KeyStore.getDefaultType());
        } catch (GeneralSecurityException generalSecurityEx) {
            throw new QueryException("Failed to create keystore instance", -1, SqlStates.CONNECTION_EXCEPTION, generalSecurityEx);
        }

        InputStream inStream = null;
        try {
            if (options.trustStore != null) {
                // use the provided keyStore
                try {

                    inStream = new URL(options.trustStore).openStream();
                    ks.load(inStream,
                            options.trustStorePassword == null ? null : options.trustStorePassword.toCharArray());

                } catch (GeneralSecurityException generalSecurityEx) {
                    throw new QueryException("Failed to create trustStore instance", -1, SqlStates.CONNECTION_EXCEPTION, generalSecurityEx);
                } catch (FileNotFoundException fileNotFoundEx) {
                    throw new QueryException("Failed to find trustStore file. trustStore=" + options.trustStore,
                            -1, SqlStates.CONNECTION_EXCEPTION, fileNotFoundEx);
                } catch (IOException ioEx) {
                    throw new QueryException("Failed to read trustStore file. trustStore=" + options.trustStore,
                            -1, SqlStates.CONNECTION_EXCEPTION, ioEx);
                }
            } else {
                // generate a keyStore from the provided cert
                if (options.serverSslCert.startsWith("-----BEGIN CERTIFICATE-----")) {
                    inStream = new ByteArrayInputStream(options.serverSslCert.getBytes());
                } else if (options.serverSslCert.startsWith("classpath:")) {
                    // Load it from a classpath relative file
                    String classpathFile = options.serverSslCert.substring("classpath:".length());
                    inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(classpathFile);
                } else {
                    try {
                        inStream = new FileInputStream(options.serverSslCert);
                    } catch (FileNotFoundException fileNotFoundEx) {
                        throw new QueryException("Failed to find serverSslCert file. serverSslCert=" + options.serverSslCert, -1,
                                SqlStates.CONNECTION_EXCEPTION, fileNotFoundEx);
                    }
                }

                try {
                    // Note: KeyStore requires it be loaded even if you don't load anything into it
                    // (will be initialized with "javax.net.ssl.trustStore") values.
                    ks.load(null);
                    CertificateFactory cf = CertificateFactory.getInstance("X.509");
                    Collection<? extends Certificate> caList = cf.generateCertificates(inStream);
                    for (Certificate ca : caList) {
                        ks.setCertificateEntry(UUID.randomUUID().toString(), ca);
                    }
                } catch (IOException ioEx) {
                    throw new QueryException("Failed load keyStore", -1, SqlStates.CONNECTION_EXCEPTION, ioEx);
                } catch (GeneralSecurityException generalSecurityEx) {
                    throw new QueryException("Failed to store certificate from serverSslCert into a keyStore", -1,
                            SqlStates.CONNECTION_EXCEPTION, generalSecurityEx);
                }

            }
        } finally {
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (IOException ioEx) {
                    //ignore error
                }
            }
        }
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            for (TrustManager tm : tmf.getTrustManagers()) {
                if (tm instanceof X509TrustManager) {
                    trustManager = (X509TrustManager) tm;
                    break;
                }
            }
        } catch (NoSuchAlgorithmException noSuchAlgorithmEx) {
            throw new QueryException("Failed to create TrustManagerFactory default instance", -1, SqlStates.CONNECTION_EXCEPTION, noSuchAlgorithmEx);
        } catch (GeneralSecurityException generalSecurityEx) {
            throw new QueryException("Failed to initialize trust manager", -1, SqlStates.CONNECTION_EXCEPTION, generalSecurityEx);
        }

        if (trustManager == null) {
            throw new RuntimeException("No X509TrustManager found");
        }
    }

    /**
     * Check client trusted.
     * @param x509Certificates certificate
     * @param string string
     * @throws CertificateException exception
     */
    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String string) throws CertificateException {
    }

    /**
     * Check server trusted.
     * @param x509Certificates certificate
     * @param string string
     * @throws CertificateException exception
     */
    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String string) throws CertificateException {
        if (trustManager == null) {
            return;
        }
        trustManager.checkServerTrusted(x509Certificates, string);
    }

    public X509Certificate[] getAcceptedIssuers() {
        return null;
    }
}
