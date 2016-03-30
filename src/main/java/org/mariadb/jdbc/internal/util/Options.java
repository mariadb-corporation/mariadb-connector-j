/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.
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

Copyright (c) 2009-2011, Marcus Eriksson

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

package org.mariadb.jdbc.internal.util;

public class Options {
    //standard options
    public String user;
    public String password;

    //divers
    public boolean trustServerCertificate;
    public String serverSslCert;
    public String trustCertificateKeyStoreUrl;
    public String trustCertificateKeyStorePassword;
    public String clientCertificateKeyStoreUrl;
    public String clientCertificateKeyStorePassword;
    public boolean useFractionalSeconds;
    public boolean pinGlobalTxToPhysicalConnection;
    public String socketFactory;
    public Integer connectTimeout;
    public String pipe;
    public String localSocket;
    public String sharedMemory;
    public boolean tcpNoDelay;
    public boolean tcpKeepAlive;
    public Integer tcpRcvBuf;
    public Integer tcpSndBuf;
    public boolean tcpAbortiveClose;
    public String localSocketAddress;
    public Integer socketTimeout;
    public boolean allowMultiQueries;
    public boolean rewriteBatchedStatements;
    public boolean useCompression;
    public boolean interactiveClient;
    /*public boolean useSSL;*/
    public boolean useSsl;
    public String sessionVariables;
    public boolean tinyInt1isBit;
    public boolean yearIsDateType;
    public boolean createDatabaseIfNotExist;
    public String serverTimezone;
    public boolean nullCatalogMeansCurrent;
    public boolean dumpQueriesOnException;
    public boolean useOldAliasMetadataBehavior;
    public boolean allowLocalInfile;
    public boolean cachePrepStmts;
    public Integer prepStmtCacheSize;
    public Integer prepStmtCacheSqlLimit;
    public boolean useLegacyDatetimeCode;
    public boolean maximizeMysqlCompatibility;
    public boolean useServerPrepStmts;
    public boolean continueBatchOnError;
    public boolean jdbcCompliantTruncation;
    public boolean cacheCallableStmts;
    public Integer callableStmtCacheSize;
    public String connectionAttributes;

    //HA options
    public boolean assureReadOnly;
    public boolean autoReconnect;
    public boolean failOnReadOnly;
    public int retriesAllDown;
    public int validConnectionTimeout;
    public int loadBalanceBlacklistTimeout;
    public int failoverLoopRetries;

    @Override
    public String toString() {
        return "Options{"
                + "user='" + user + '\''
                + ", assureReadOnly=" + assureReadOnly
                + ", password='" + password + '\''
                + ", trustServerCertificate=" + trustServerCertificate
                + ", serverSslCert='" + serverSslCert + '\''
                + ", useFractionalSeconds=" + useFractionalSeconds
                + ", pinGlobalTxToPhysicalConnection=" + pinGlobalTxToPhysicalConnection
                + ", trustCertificateKeyStoreUrl='" + trustCertificateKeyStoreUrl + '\''
                + ", trustCertificateKeyStorePassword='" + trustCertificateKeyStorePassword + '\''
                + ", clientCertificateKeyStoreUrl='" + clientCertificateKeyStoreUrl + '\''
                + ", clientCertificateKeyStorePassword='" + clientCertificateKeyStorePassword + '\''
                + ", socketFactory='" + socketFactory + '\''
                + ", connectTimeout=" + connectTimeout
                + ", pipe='" + pipe + '\''
                + ", localSocket='" + localSocket + '\''
                + ", sharedMemory='" + sharedMemory + '\''
                + ", tcpNoDelay=" + tcpNoDelay
                + ", tcpKeepAlive=" + tcpKeepAlive
                + ", tcpRcvBuf=" + tcpRcvBuf
                + ", tcpSndBuf=" + tcpSndBuf
                + ", tcpAbortiveClose=" + tcpAbortiveClose
                + ", localSocketAddress='" + localSocketAddress + '\''
                + ", socketTimeout=" + socketTimeout
                + ", allowMultiQueries=" + allowMultiQueries
                + ", rewriteBatchedStatements=" + rewriteBatchedStatements
                + ", useCompression=" + useCompression
                + ", interactiveClient=" + interactiveClient
                + ", useSsl=" + useSsl
                + ", sessionVariables='" + sessionVariables + '\''
                + ", tinyInt1isBit=" + tinyInt1isBit
                + ", yearIsDateType=" + yearIsDateType
                + ", createDatabaseIfNotExist=" + createDatabaseIfNotExist
                + ", serverTimezone='" + serverTimezone + '\''
                + ", nullCatalogMeansCurrent=" + nullCatalogMeansCurrent
                + ", dumpQueriesOnException=" + dumpQueriesOnException
                + ", useOldAliasMetadataBehavior=" + useOldAliasMetadataBehavior
                + ", allowLocalInfile=" + allowLocalInfile
                + ", cachePrepStmts=" + cachePrepStmts
                + ", prepStmtCacheSize=" + prepStmtCacheSize
                + ", prepStmtCacheSqlLimit=" + prepStmtCacheSqlLimit
                + ", autoReconnect=" + autoReconnect
                + ", failOnReadOnly=" + failOnReadOnly
                + ", retriesAllDown=" + retriesAllDown
                + ", validConnectionTimeout=" + validConnectionTimeout
                + ", loadBalanceBlacklistTimeout=" + loadBalanceBlacklistTimeout
                + ", failoverLoopRetries=" + failoverLoopRetries
                + ", useLegacyDatetimeCode=" + useLegacyDatetimeCode
                + ", maximizeMysqlCompatibility=" + maximizeMysqlCompatibility
                + ", continueBatchOnError=" + continueBatchOnError
                + ", jdbcCompliantTruncation=" + jdbcCompliantTruncation
                + ", cacheCallableStmts=" + cacheCallableStmts
                + ", callableStmtCacheSize=" + callableStmtCacheSize
                + ", connectionAttributes=" + connectionAttributes
                + "}";
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Options options = (Options) obj;

        if (trustServerCertificate != options.trustServerCertificate) {
            return false;
        }
        if (useFractionalSeconds != options.useFractionalSeconds) {
            return false;
        }
        if (pinGlobalTxToPhysicalConnection != options.pinGlobalTxToPhysicalConnection) {
            return false;
        }
        if (tcpNoDelay != options.tcpNoDelay) {
            return false;
        }
        if (tcpKeepAlive != options.tcpKeepAlive) {
            return false;
        }
        if (tcpAbortiveClose != options.tcpAbortiveClose) {
            return false;
        }
        if (allowMultiQueries != options.allowMultiQueries) {
            return false;
        }
        if (rewriteBatchedStatements != options.rewriteBatchedStatements) {
            return false;
        }
        if (useCompression != options.useCompression) {
            return false;
        }
        if (interactiveClient != options.interactiveClient) {
            return false;
        }
        if (useSsl != options.useSsl) {
            return false;
        }
        if (tinyInt1isBit != options.tinyInt1isBit) {
            return false;
        }
        if (yearIsDateType != options.yearIsDateType) {
            return false;
        }
        if (createDatabaseIfNotExist != options.createDatabaseIfNotExist) {
            return false;
        }
        if (nullCatalogMeansCurrent != options.nullCatalogMeansCurrent) {
            return false;
        }
        if (dumpQueriesOnException != options.dumpQueriesOnException) {
            return false;
        }
        if (useOldAliasMetadataBehavior != options.useOldAliasMetadataBehavior) {
            return false;
        }
        if (allowLocalInfile != options.allowLocalInfile) {
            return false;
        }
        if (cachePrepStmts != options.cachePrepStmts) {
            return false;
        }
        if (useLegacyDatetimeCode != options.useLegacyDatetimeCode) {
            return false;
        }
        if (maximizeMysqlCompatibility != options.maximizeMysqlCompatibility) {
            return false;
        }
        if (useServerPrepStmts != options.useServerPrepStmts) {
            return false;
        }
        if (assureReadOnly != options.assureReadOnly) {
            return false;
        }
        if (autoReconnect != options.autoReconnect) {
            return false;
        }
        if (failOnReadOnly != options.failOnReadOnly) {
            return false;
        }
        if (retriesAllDown != options.retriesAllDown) {
            return false;
        }
        if (validConnectionTimeout != options.validConnectionTimeout) {
            return false;
        }
        if (loadBalanceBlacklistTimeout != options.loadBalanceBlacklistTimeout) {
            return false;
        }
        if (failoverLoopRetries != options.failoverLoopRetries) {
            return false;
        }
        if (user != null ? !user.equals(options.user) : options.user != null) {
            return false;
        }
        if (password != null ? !password.equals(options.password) : options.password != null) {
            return false;
        }
        if (serverSslCert != null ? !serverSslCert.equals(options.serverSslCert) : options.serverSslCert != null) {
            return false;
        }
        if (socketFactory != null ? !socketFactory.equals(options.socketFactory) : options.socketFactory != null) {
            return false;
        }
        if (connectTimeout != null ? !connectTimeout.equals(options.connectTimeout) : options.connectTimeout != null) {
            return false;
        }
        if (pipe != null ? !pipe.equals(options.pipe) : options.pipe != null) {
            return false;
        }
        if (localSocket != null ? !localSocket.equals(options.localSocket) : options.localSocket != null) {
            return false;
        }
        if (sharedMemory != null ? !sharedMemory.equals(options.sharedMemory) : options.sharedMemory != null) {
            return false;
        }
        if (tcpRcvBuf != null ? !tcpRcvBuf.equals(options.tcpRcvBuf) : options.tcpRcvBuf != null) {
            return false;
        }
        if (tcpSndBuf != null ? !tcpSndBuf.equals(options.tcpSndBuf) : options.tcpSndBuf != null) {
            return false;
        }
        if (localSocketAddress != null ? !localSocketAddress.equals(options.localSocketAddress) : options.localSocketAddress != null) {
            return false;
        }
        if (socketTimeout != null ? !socketTimeout.equals(options.socketTimeout) : options.socketTimeout != null) {
            return false;
        }
        if (sessionVariables != null ? !sessionVariables.equals(options.sessionVariables) : options.sessionVariables != null) {
            return false;
        }
        if (serverTimezone != null ? !serverTimezone.equals(options.serverTimezone) : options.serverTimezone != null) {
            return false;
        }
        if (prepStmtCacheSize != null ? !prepStmtCacheSize.equals(options.prepStmtCacheSize) : options.prepStmtCacheSize != null) {
            return false;
        }
        if (continueBatchOnError != options.continueBatchOnError) {
            return false;
        }
        if (jdbcCompliantTruncation != options.jdbcCompliantTruncation) {
            return false;
        }
        if (cacheCallableStmts != options.cacheCallableStmts) {
            return false;
        }

        if (callableStmtCacheSize != null ? !callableStmtCacheSize.equals(options.callableStmtCacheSize) : options.callableStmtCacheSize != null) {
            return false;
        }
        if (connectionAttributes != null ? !connectionAttributes.equals(options.connectionAttributes) : options.connectionAttributes != null) {
            return false;
        }
        return !(prepStmtCacheSqlLimit != null ? !prepStmtCacheSqlLimit.equals(options.prepStmtCacheSqlLimit)
                : options.prepStmtCacheSqlLimit != null);

    }

}
