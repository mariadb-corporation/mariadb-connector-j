/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

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

package org.mariadb.jdbc.internal.common;

public class Options {
    //standard options
    public String user;
    public String password;

    //divers
    public boolean trustServerCertificate;
    public String serverSslCert;
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
    public boolean useSSL;
    public String sessionVariables;
    public boolean tinyInt1isBit;
    public boolean yearIsDateType;
    public boolean createDatabaseIfNotExist;
    public String serverTimezone;
    public boolean nullCatalogMeansCurrent;
    public boolean dumpQueriesOnException;
    public boolean useOldAliasMetadataBehavior;

    //HA options
    public boolean autoReconnect;
    public boolean failOnReadOnly;
    public int initialTimeout;
    public int secondsBeforeRetryMaster;
    public int queriesBeforeRetryMaster;
    public int retriesAllDown;
    public int validConnectionTimeout;
    public int loadBalanceBlacklistTimeout;
    public int failoverLoopRetries;

    @Override
    public String toString() {
        return "Options{" +
                "user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", trustServerCertificate=" + trustServerCertificate +
                ", serverSslCert='" + serverSslCert + '\'' +
                ", useFractionalSeconds=" + useFractionalSeconds +
                ", pinGlobalTxToPhysicalConnection=" + pinGlobalTxToPhysicalConnection +
                ", socketFactory='" + socketFactory + '\'' +
                ", connectTimeout=" + connectTimeout +
                ", pipe='" + pipe + '\'' +
                ", localSocket='" + localSocket + '\'' +
                ", sharedMemory='" + sharedMemory + '\'' +
                ", tcpNoDelay=" + tcpNoDelay +
                ", tcpKeepAlive=" + tcpKeepAlive +
                ", tcpRcvBuf=" + tcpRcvBuf +
                ", tcpSndBuf=" + tcpSndBuf +
                ", tcpAbortiveClose=" + tcpAbortiveClose +
                ", localSocketAddress='" + localSocketAddress + '\'' +
                ", socketTimeout=" + socketTimeout +
                ", allowMultiQueries=" + allowMultiQueries +
                ", rewriteBatchedStatements=" + rewriteBatchedStatements +
                ", useCompression=" + useCompression +
                ", interactiveClient=" + interactiveClient +
                ", useSSL=" + useSSL +
                ", sessionVariables='" + sessionVariables + '\'' +
                ", tinyInt1isBit=" + tinyInt1isBit +
                ", yearIsDateType=" + yearIsDateType +
                ", createDatabaseIfNotExist=" + createDatabaseIfNotExist +
                ", serverTimezone='" + serverTimezone + '\'' +
                ", nullCatalogMeansCurrent=" + nullCatalogMeansCurrent +
                ", dumpQueriesOnException=" + dumpQueriesOnException +
                ", useOldAliasMetadataBehavior=" + useOldAliasMetadataBehavior +
                ", autoReconnect=" + autoReconnect +
                ", failOnReadOnly=" + failOnReadOnly +
                ", initialTimeout=" + initialTimeout +
                ", secondsBeforeRetryMaster=" + secondsBeforeRetryMaster +
                ", queriesBeforeRetryMaster=" + queriesBeforeRetryMaster +
                ", retriesAllDown=" + retriesAllDown +
                ", validConnectionTimeout=" + validConnectionTimeout +
                ", loadBalanceBlacklistTimeout=" + loadBalanceBlacklistTimeout +
                ", failoverLoopRetries=" + failoverLoopRetries +
                '}';
    }
}
