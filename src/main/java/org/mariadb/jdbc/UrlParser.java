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

package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.common.DefaultOptions;
import org.mariadb.jdbc.internal.common.Options;
import org.mariadb.jdbc.internal.common.ParameterConstant;
import org.mariadb.jdbc.internal.common.HaMode;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * <p>parse and verification of URL.</p>
 * <p>basic syntax :<br>
 * {@code jdbc:(mysql|mariadb):[replication:|failover|loadbalance:|aurora:]//<hostDescription>[,<hostDescription>]/[database>]
 * [?<key1>=<value1>[&<key2>=<value2>]] }
 * </p>
 * <p>
 * hostDescription:<br>
 * - simple :<br>
 * {@code <host>:<portnumber>}<br>
 * (for example localhost:3306)<br><br>
 * - complex :<br>
 * {@code address=[(type=(master|slave))][(port=<portnumber>)](host=<host>)}<br>
 * <br><br>
 * type is by default master<br>
 * port is by default 3306<br>
 * </p>
 * <p>
 * host can be dns name, ipv4 or ipv6.<br>
 * in case of ipv6 and simple host description, the ip must be written inside bracket.<br>
 * exemple : {@code jdbc:mysql://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306}<br>
 * </p>
 * <p>
 * Some examples :<br>
 * {@code jdbc:mysql://localhost:3306/database?user=greg&password=pass}<br>
 * {@code jdbc:mysql://address=(type=master)(host=master1),address=(port=3307)(type=slave)(host=slave1)/database?user=greg&password=pass}<br>
 * </p>
 */
public class UrlParser {

    private String database;
    private Options options;
    private List<HostAddress> addresses;
    private HaMode haMode;

    private UrlParser() {
    }

    protected UrlParser(String database, List<HostAddress> addresses, Options options, HaMode haMode) throws SQLException {
        this.options = options;
        this.database = database;
        this.addresses = addresses;
        this.haMode = haMode;
        if (haMode == HaMode.AURORA) {
            for (HostAddress hostAddress : addresses) {
                hostAddress.type = null;
            }
        } else {
            for (HostAddress hostAddress : addresses) {
                if (hostAddress.type == null) {
                    hostAddress.type = ParameterConstant.TYPE_MASTER;
                }
            }
        }
    }


    public static boolean acceptsUrl(String url) {
        return (url != null) && (url.startsWith("jdbc:mariadb:") || url.startsWith("jdbc:mysql:"));

    }

    public static UrlParser parse(final String url) throws SQLException {
        return parse(url, new Properties());
    }

    /**
     * Parse url connection string with additional properties.
     * @param url connection string
     * @param prop properties
     * @return UrlParser instance
     * @throws SQLException if parsing exception occur
     */
    public static UrlParser parse(final String url, Properties prop) throws SQLException {
        if (url != null) {
            if (prop == null) {
                prop = new Properties();
            }
            if (url.startsWith("jdbc:mysql:")) {
                UrlParser urlParser = new UrlParser();
                parseInternal(urlParser, url, prop);
                return urlParser;
            } else {
                if (url.startsWith("jdbc:mariadb:")) {
                    UrlParser urlParser = new UrlParser();
                    parseInternal(urlParser, "jdbc:mysql:" + url.substring(12), prop);
                    return urlParser;
                }
            }
        }
        return null;
    }

    /*
        Parse ConnectorJ compatible urls
        jdbc:mysql://host:port/database
        Example: jdbc:mysql://localhost:3306/test?user=root&password=passwd
         */
    private static void parseInternal(UrlParser urlParser, String url, Properties properties) throws SQLException {
        try {
            int separator = url.indexOf("//");
            if (separator == -1) {
                throw new IllegalArgumentException("url parsing error : '//' is not present in the url " + url);
            }
            String[] baseTokens = url.substring(0, separator).split(":");

            //parse HA mode
            urlParser.haMode = HaMode.NONE;
            if (baseTokens.length > 2) {
                try {
                    urlParser.haMode = HaMode.valueOf(baseTokens[2].toUpperCase());
                } catch (IllegalArgumentException i) {
                    throw new IllegalArgumentException("url parameter error '" + baseTokens[2] + "' is a unknown parameter in the url " + url);
                }
            }

            url = url.substring(separator + 2);
            String[] tokens = url.split("/");
            String hostAddressesString = tokens[0];
            String additionalParameters = (tokens.length > 1) ? url.substring(tokens[0].length() + 1) : null;

            urlParser.addresses = HostAddress.parse(hostAddressesString, urlParser.haMode);

            if (additionalParameters == null) {
                urlParser.database = null;
                urlParser.options = DefaultOptions.parse(urlParser.haMode, "", properties);
            } else {
                int ind = additionalParameters.indexOf('?');
                if (ind > -1) {
                    urlParser.database = additionalParameters.substring(0, ind);
                    urlParser.options = DefaultOptions.parse(urlParser.haMode, additionalParameters.substring(ind + 1), properties);
                } else {
                    urlParser.database = additionalParameters;
                    urlParser.options = DefaultOptions.parse(urlParser.haMode, "", properties);
                }
            }

            if (urlParser.haMode == HaMode.AURORA) {
                for (HostAddress hostAddress : urlParser.addresses) {
                    hostAddress.type = null;
                }
            } else {
                for (HostAddress hostAddress : urlParser.addresses) {
                    if (hostAddress.type == null) {
                        hostAddress.type = ParameterConstant.TYPE_MASTER;
                    }
                }
            }
        } catch (IllegalArgumentException i) {
            throw new SQLException(i.getMessage());
        }
    }

    /**
     * Parse url connection string.
     * @param url connection string
     * @throws SQLException if url format is incorrect
     */
    public void parseUrl(String url) throws SQLException {
        if (url.startsWith("jdbc:mysql:")) {
            parseInternal(this, url, new Properties());
            return;
        }
        String[] arr = new String[]{"jdbc:mysql:thin:", "jdbc:mariadb:"};
        for (String prefix : arr) {
            if (url.startsWith(prefix)) {
                parseInternal(this, url, new Properties());
                break;
            }
        }
    }

    public String getUsername() {
        return options.user;
    }

    protected void setUsername(String username) {
        options.user = username;
    }

    public String getPassword() {
        return options.password;
    }

    protected void setPassword(String password) {
        options.password = password;
    }

    public String getDatabase() {
        return database;
    }

    protected void setDatabase(String database) {
        this.database = database;
    }

    public List<HostAddress> getHostAddresses() {
        return this.addresses;
    }

    public Options getOptions() {
        return options;
    }

    protected void setProperties(String urlParameters) {
        DefaultOptions.parse(this.haMode, urlParameters, this.options);
    }

    /**
     * ToString implementation.
     * @return String value
     */
    public String toString() {
        String str = "jdbc:mysql://";
        if (!haMode.equals(HaMode.NONE)) {
            str = "jdbc:mysql:" + haMode.toString().toLowerCase() + "://";
        }
        if (addresses != null) {
            str += HostAddress.toString(addresses);
        }
        if (database != null) {
            str += "/" + database;
        }
        return str;
    }

    public HaMode getHaMode() {
        return haMode;
    }

}
