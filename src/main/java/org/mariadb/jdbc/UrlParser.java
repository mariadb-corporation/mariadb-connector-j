/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
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

package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.util.DefaultOptions;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.mariadb.jdbc.internal.util.constant.ParameterConstant;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * exemple : {@code jdbc:mariadb://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306}<br>
 * </p>
 * <p>
 * Some examples :<br>
 * {@code jdbc:mariadb://localhost:3306/database?user=greg&password=pass}<br>
 * {@code jdbc:mariadb://address=(type=master)(host=master1),address=(port=3307)(type=slave)(host=slave1)/database?user=greg&password=pass}<br>
 * </p>
 */
public class UrlParser {

    private static final String DISABLE_MYSQL_URL = "disableMariaDbDriver";
    private String database;
    private Options options = null;
    private List<HostAddress> addresses;
    private HaMode haMode;
    private String initialUrl;
    private boolean multiMaster = isMultiMaster();

    private UrlParser() {
    }

    protected UrlParser(String database, List<HostAddress> addresses, Options options, HaMode haMode) {
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
        multiMaster = loadMultiMasterValue();
    }

    /**
     * Tell if mariadb driver accept url string.
     * (Correspond to interface java.jdbc.Driver.acceptsURL() method)
     *
     * @param url url String
     * @return true if url string correspond.
     */
    public static boolean acceptsUrl(String url) {
        return (url != null) && (url.startsWith("jdbc:mariadb:")
                || (url.startsWith("jdbc:mysql:") && !url.contains(DISABLE_MYSQL_URL)));
    }

    public static UrlParser parse(final String url) throws SQLException {
        return parse(url, new Properties());
    }

    /**
     * Parse url connection string with additional properties.
     *
     * @param url  connection string
     * @param prop properties
     * @return UrlParser instance
     * @throws SQLException if parsing exception occur
     */
    public static UrlParser parse(final String url, Properties prop) throws SQLException {
        if (url != null) {

            if (prop == null) prop = new Properties();

            if (url.startsWith("jdbc:mariadb:") || url.startsWith("jdbc:mysql:") && !url.contains(DISABLE_MYSQL_URL)) {
                UrlParser urlParser = new UrlParser();
                parseInternal(urlParser, url, prop);
                return urlParser;
            }
        }
        return null;
    }

    /*
        Parse ConnectorJ compatible urls
        jdbc:[mariadb|mysql]://host:port/database
        Example: jdbc:mariadb://localhost:3306/test?user=root&password=passwd
         */

    /**
     * Parses the connection URL in order to set the UrlParser instance with all the information provided through the URL.
     *
     * @param urlParser  object instance in which all data from the connection url is stored
     * @param url        connection URL
     * @param properties properties
     * @throws SQLException if format is incorrect
     */
    private static void parseInternal(UrlParser urlParser, String url, Properties properties) throws SQLException {
        try {
            urlParser.initialUrl = url;
            int separator = url.indexOf("//");
            if (separator == -1) {
                throw new IllegalArgumentException("url parsing error : '//' is not present in the url " + url);
            }

            setHaMode(urlParser, url, separator);

            String urlSecondPart = url.substring(separator + 2);
            int dbIndex = urlSecondPart.indexOf("/");
            int paramIndex = urlSecondPart.indexOf("?");

            String hostAddressesString;
            String additionalParameters;
            if ((dbIndex < paramIndex && dbIndex < 0) || (dbIndex > paramIndex && paramIndex > -1)) {
                hostAddressesString = urlSecondPart.substring(0, paramIndex);
                additionalParameters = urlSecondPart.substring(paramIndex);
            } else if ((dbIndex < paramIndex && dbIndex > -1) || (dbIndex > paramIndex && paramIndex < 0)) {
                hostAddressesString = urlSecondPart.substring(0, dbIndex);
                additionalParameters = urlSecondPart.substring(dbIndex);
            } else {
                hostAddressesString = urlSecondPart;
                additionalParameters = null;
            }

            defineUrlParserParameters(urlParser, properties, hostAddressesString, additionalParameters);
            setDefaultHostAddressType(urlParser);

        } catch (IllegalArgumentException i) {
            throw new SQLException(i.getMessage());
        }
    }

    /**
     * Sets the parameters of the UrlParser instance: addresses, database and options.
     * It parses through the additional parameters given in order to extract the database and the options for the connection.
     *
     * @param urlParser            object instance in which all data from the connection URL is stored
     * @param properties           properties
     * @param hostAddressesString  string that holds all the host addresses
     * @param additionalParameters string that holds all parameters defined for the connection
     */
    private static void defineUrlParserParameters(UrlParser urlParser, Properties properties, String hostAddressesString,
                                                  String additionalParameters) {

        if (additionalParameters != null) {
            //noinspection Annotator
            String regex = "(\\/([^\\?]*))?(\\?(.+))*";
            Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
            Matcher matcher = pattern.matcher(additionalParameters);

            if (matcher.find()) {

                urlParser.database = matcher.group(2);
                urlParser.options = DefaultOptions.parse(urlParser.haMode, matcher.group(4), properties, urlParser.options);
                if (urlParser.database != null && urlParser.database.isEmpty()) urlParser.database = null;

            } else {

                urlParser.database = null;
                urlParser.options = DefaultOptions.parse(urlParser.haMode, "", properties, urlParser.options);

            }

        } else {

            urlParser.database = null;
            urlParser.options = DefaultOptions.parse(urlParser.haMode, "", properties, urlParser.options);

        }

        LoggerFactory.init(urlParser.options.log
                || urlParser.options.profileSql
                || urlParser.options.slowQueryThresholdNanos != null);
        urlParser.addresses = HostAddress.parse(hostAddressesString, urlParser.haMode);
    }

    /**
     * Permit to set parameters not forced.
     * if options useBatchMultiSend and usePipelineAuth are not explicitly set in connection string,
     * value will default to true or false according if aurora detection.
     *
     * @return UrlParser for easy testing
     */
    public UrlParser auroraPipelineQuirks() {

        //Aurora has issue with pipelining, depending on network speed.
        //Driver must rely on information provided by user : hostname if dns, and HA mode.</p>
        boolean disablePipeline = isAurora();

        if (options.useBatchMultiSend == null) {
            options.useBatchMultiSend = disablePipeline ? Boolean.FALSE : Boolean.TRUE;
        }

        if (options.usePipelineAuth == null) {
            options.usePipelineAuth = disablePipeline ? Boolean.FALSE : Boolean.TRUE;
        }
        return this;
    }

    /**
     * Detection of Aurora.
     * <p>
     * Aurora rely on MySQL, then cannot be identified by protocol.
     * But Aurora doesn't permit some behaviour normally working with MySQL : pipelining.
     * So Driver must identified if server is Aurora to disable pipeline options that are enable by default.
     * </p>
     *
     * @return true if aurora.
     */
    public boolean isAurora() {
        if (haMode == HaMode.AURORA) return true;
        if (addresses != null) {
            Pattern clusterPattern = Pattern.compile("(.+)\\.([a-z0-9\\-]+\\.rds\\.amazonaws\\.com)", Pattern.CASE_INSENSITIVE);
            for (HostAddress hostAddress : addresses) {
                Matcher matcher = clusterPattern.matcher(hostAddress.host);
                if (matcher.find()) return true;
            }
        }
        return false;
    }

    private static void setHaMode(UrlParser urlParser, String url, int separator) {
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
    }

    private static void setDefaultHostAddressType(UrlParser urlParser) {
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
    }

    /**
     * Parse url connection string.
     *
     * @param url connection string
     * @throws SQLException if url format is incorrect
     */
    public void parseUrl(String url) throws SQLException {
        if (acceptsUrl(url)) {
            parseInternal(this, url, new Properties());
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

    public void setHostAddresses(List<HostAddress> addresses) {
        this.addresses = addresses;
    }

    public Options getOptions() {
        return options;
    }

    protected void setProperties(String urlParameters) {
        DefaultOptions.parse(this.haMode, urlParameters, this.options);
    }

    /**
     * ToString implementation.
     *
     * @return String value
     */
    public String toString() {
        return initialUrl;
    }

    public String getInitialUrl() {
        return initialUrl;
    }

    public HaMode getHaMode() {
        return haMode;
    }

    @Override
    public boolean equals(Object parser) {
        if (this == parser) return true;
        if (!(parser instanceof UrlParser)) return false;

        UrlParser urlParser = (UrlParser) parser;
        return (initialUrl != null ? initialUrl.equals(urlParser.getInitialUrl()) : urlParser.getInitialUrl() == null)
                && (getUsername() != null ? getUsername().equals(urlParser.getUsername()) : urlParser.getUsername() == null)
                && (getPassword() != null ? getPassword().equals(urlParser.getPassword()) : urlParser.getPassword() == null);
    }

    private boolean loadMultiMasterValue() {
        if (haMode == HaMode.SEQUENTIAL
                || haMode == HaMode.REPLICATION
                || haMode == HaMode.FAILOVER) {
            boolean firstMaster = false;
            for (HostAddress host : addresses) {
                if (host.type.equals(ParameterConstant.TYPE_MASTER)) {
                    if (firstMaster) {
                        return true;
                    } else {
                        firstMaster = true;
                    }
                }
            }
        }
        return false;
    }

    public boolean isMultiMaster() {
        return multiMaster;
    }
}