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

package org.mariadb.jdbc.internal.util;

import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.lang.invoke.WrongMethodTypeException;
import java.util.Properties;

public enum DefaultOptions {
    /**
     * Database user name.
     */
    USER("user", "1.0.0"),
    /**
     * Password of database user.
     */
    PASSWORD("password", "1.0.0"),

    CONNECT_TIMOUT("connectTimeout", (Integer) null, new Integer(0), Integer.MAX_VALUE, "1.1.8"),

    /**
     * On Windows, specify named pipe name to connect to mysqld.exe.
     */
    PIPE("pipe", "1.1.3"),

    /**
     * Allows to connect to database via Unix domain socket, if server allows it. The value is the path of Unix domain socket, i.e "socket"
     * database parameter.
     */
    LOCAL_SOCKET("localSocket", "1.1.4"),

    /**
     * Allowed to connect database via shared memory, if server allows it. The value is base name of the shared memory.
     */
    SHARED_MEMORY("sharedMemory", "1.1.4"),

    /**
     * Sets corresponding option on the connection socket.
     */
    TCP_NO_DELAY("tcpNoDelay", Boolean.FALSE, "1.0.0"),

    /**
     * Sets corresponding option on the connection socket.
     */
    TCP_ABORTIVE_CLOSE("tcpAbortiveClose", Boolean.FALSE, "1.1.1"),

    /**
     * Hostname or IP address to bind the connection socket to a local (UNIX domain) socket.
     */
    LOCAL_SOCKET_ADDRESS("localSocketAddress", "1.1.8"),

    /**
     * Defined the network socket timeout (SO_TIMEOUT) in milliseconds.
     * 0 (default) disable this timeout
     */
    SOCKET_TIMEOUT("socketTimeout", new Integer[]{10000, null, null, null, null, null}, new Integer(0), Integer.MAX_VALUE, "1.1.8"),

    /**
     * Session timeout is defined by the wait_timeout server variable.
     * Setting interactiveClient to true will tell server to use the interactive_timeout server variable
     */
    INTERACTIVE_CLIENT("interactiveClient", Boolean.FALSE, "1.1.8"),

    /**
     * If set to 'true', exception thrown during query execution contain query string.
     */
    DUMP_QUERY_ON_EXCEPTION("dumpQueriesOnException", Boolean.FALSE, "1.1.0"),

    /**
     * Metadata ResultSetMetaData.getTableName() return the physical table name.
     * "useOldAliasMetadataBehavior" permit to activate the legacy code that send the table alias if set.
     */
    USE_OLD_ALIAS_METADATA_BEHAVIOR("useOldAliasMetadataBehavior", Boolean.FALSE, "1.1.9"),

    /**
     * If set to 'false', exception thrown during LOCAL INFILE if no InputStream has already been set.
     */
    ALLOW_LOCAL_INFILE("allowLocalInfile", Boolean.TRUE, "1.2.1"),

    /**
     * var=value pairs separated by comma, mysql session variables, set upon establishing successful connection.
     */
    SESSION_VARIABLES("sessionVariables", "1.1.0"),

    /**
     * The database precised in url will be created if doesn't exist.
     */
    CREATE_DATABASE_IF_NOT_EXISTS("createDatabaseIfNotExist", Boolean.FALSE, "1.1.8"),

    /**
     * Defined the server time zone.
     * to use only if jre server as a different time implementation of the server.
     * (best to have the same server time zone when possible)
     */
    SERVER_TIMEZONE("serverTimezone", "1.1.8"),
    /**
     * DatabaseMetaData use current catalog if null.
     */
    NULL_CATALOG_MEANS_CURRENT("nullCatalogMeansCurrent", Boolean.TRUE, "1.1.8"),

    /**
     * Datatype mapping flag, handle MySQL Tiny as BIT(boolean).
     */
    TINY_INT_IS_BIT("tinyInt1isBit", Boolean.TRUE, "1.0.0"),

    /**
     * Year is date type, rather than numerical.
     */
    YEAR_IS_DATE_TYPE("yearIsDateType", Boolean.TRUE, "1.0.0"),

    /**
     * Force SSL on connection.
     */
    USE_SSL("useSsl", Boolean.FALSE, "1.1.0"),

    /**
     * allow compression in MySQL Protocol.
     */
    USER_COMPRESSION("useCompression", Boolean.FALSE, "1.0.0"),

    /**
     * Allows multiple statements in single executeQuery.
     */
    ALLOW_MULTI_QUERIES("allowMultiQueries", Boolean.FALSE, "1.0.0"),

    /**
     * rewrite batchedStatement to have only one server call.
     */
    REWRITE_BATCHED_STATEMENTS("rewriteBatchedStatements", Boolean.FALSE, "1.1.8"),

    /**
     * Sets corresponding option on the connection socket.
     */
    TCP_KEEP_ALIVE("tcpKeepAlive", Boolean.FALSE, "1.0.0"),

    /**
     * set buffer size for TCP buffer (SO_RCVBUF).
     */
    TCP_RCV_BUF("tcpRcvBuf", (Integer) null, new Integer(0), Integer.MAX_VALUE, "1.0.0"),

    /**
     * set buffer size for TCP buffer (SO_SNDBUF).
     */
    TCP_SND_BUF("tcpSndBuf", (Integer) null, new Integer(0), Integer.MAX_VALUE, "1.0.0"),

    /**
     * to use custom socket factory, set it to full name of the class that implements javax.net.SocketFactory.
     */
    SOCKET_FACTORY("socketFactory", "1.0.0"),
    PIN_GLOBAL_TX_TO_PHYSICAL_CONNECTION("pinGlobalTxToPhysicalConnection", Boolean.FALSE, "1.1.8"),

    /**
     * When using SSL, do not check server's certificate.
     */
    TRUST_SERVER_CERTIFICATE("trustServerCertificate", Boolean.FALSE, "1.1.1"),

    /**
     * Server's certificatem in DER form, or server's CA certificate. Can be used in one of 3 forms, sslServerCert=/path/to/cert.pem
     * (full path to certificate), sslServerCert=classpath:relative/cert.pem (relative to current classpath), or as verbatim DER-encoded certificate
     * string "------BEGING CERTIFICATE-----".
     */
    SERVER_SSL_CERT("serverSslCert", "1.1.3"),

    /**
     * Correctly handle subsecond precision in timestamps (feature available with MariaDB 5.3 and later).
     * May confuse 3rd party components (Hibernated).
     */
    USE_FRACTIONAL_SECONDS("useFractionalSeconds", Boolean.TRUE, "1.0.0"),

    /**
     * Driver must recreateConnection after a failover.
     */
    AUTO_RECONNECT("autoReconnect", Boolean.FALSE, "1.2.0"),

    /**
     * After a master failover and no other master found, back on a read-only host ( throw exception if not).
     */
    FAIL_ON_READ_ONLY("failOnReadOnly", Boolean.FALSE, "1.2.0"),

    /**
     * When using loadbalancing, the number of times the driver should cycle through available hosts, attempting to connect.
     * Between cycles, the driver will pause for 250ms if no servers are available.
     */
    RETRY_ALL_DOWN("retriesAllDown", new Integer(120), new Integer(0), Integer.MAX_VALUE, "1.2.0"),

    /**
     * When using failover, the number of times the driver should cycle silently through available hosts, attempting to connect.
     * Between cycles, the driver will pause for 250ms if no servers are available.
     * if set to 0, there will be no silent reconnection
     */
    FAILOVER_LOOP_RETRIES("failoverLoopRetries", new Integer(120), new Integer(0), Integer.MAX_VALUE, "1.2.0"),


    /**
     * When in multiple hosts, after this time in second without used, verification that the connections havn't been lost.
     * When 0, no verification will be done. Defaults to 120
     */
    VALID_CONNECTION_TIMEOUT("validConnectionTimeout", new Integer(120), new Integer(0), Integer.MAX_VALUE, "1.2.0"),

    /**
     * time in second a server is blacklisted after a connection failure.  default to 50s
     */
    LOAD_BALANCE_BLACKLIST_TIMEOUT("loadBalanceBlacklistTimeout", new Integer(50), new Integer(0), Integer.MAX_VALUE, "1.2.0"),

    /**
     * enable/disable prepare Statement cache, default true.
     */
    CACHEPREPSTMTS("cachePrepStmts", Boolean.TRUE, "1.3.0"),

    /**
     * This sets the number of prepared statements that the driver will cache per VM if "cachePrepStmts" is enabled.
     * default to 250.
     */
    PREPSTMTCACHESIZE("prepStmtCacheSize", new Integer(250), new Integer(0), Integer.MAX_VALUE, "1.3.0"),

    /**
     * This is the maximum length of a prepared SQL statement that the driver will cache  if "cachePrepStmts" is enabled.
     * default to 2048.
     */
    PREPSTMTCACHESQLLIMIT("prepStmtCacheSqlLimit", new Integer(2048), new Integer(0), Integer.MAX_VALUE, "1.3.0"),

    /**
     * when in high availalability, and switching to a read-only host, assure that this host is in read-only mode by
     * setting session read-only.
     * default to false
     */
    ASSUREREADONLY("assureReadOnly", Boolean.FALSE, "1.3.0"),


    /**
     * if true (default) store date/timestamps according to client time zone.
     * if false, store all date/timestamps in DB according to server time zone, and time information (that is a time difference), doesn't take
     * timezone in account.
     */
    USELEGACYDATETIMECODE("useLegacyDatetimeCode", Boolean.TRUE, "1.3.0"),

    /**
     * maximize Mysql compatibility.
     * when using jdbc setDate(), will store date in client timezone, not in server timezone when useLegacyDatetimeCode = false.
     * default to false.
     */
    MAXIMIZEMYSQLCOMPATIBILITY("maximizeMysqlCompatibility", Boolean.FALSE, "1.3.0"),

    /**
     * alwaysAutoGeneratedKeys can be set to true to have previous compatibility.
     * default to false.
     */
    ALWAYSAUTOGENERATEKEYS("alwaysAutoGeneratedKeys", Boolean.FALSE, "1.3.0"),

    /**
     * useServerPrepStmts must prepared statements be prepared on server side, or just faked on client side.
     * if allowMultiQueries or rewriteBatchedStatements is set to true, this options will be set to false.
     * default to true.
     */
    USESERVERPREPSTMTS("useServerPrepStmts", Boolean.TRUE, "1.3.0");


    protected final String name;
    protected final Object objType;
    protected final Object defaultValue;
    protected final Object minValue;
    protected final Object maxValue;
    protected final String implementationVersion;
    protected Object value = null;

    DefaultOptions(String name, String implementationVersion) {
        this.name = name;
        this.implementationVersion = implementationVersion;
        objType = String.class;
        defaultValue = null;
        minValue = null;
        maxValue = null;
    }

    DefaultOptions(String name, Boolean defaultValue, String implementationVersion) {
        this.name = name;
        this.objType = Boolean.class;
        this.defaultValue = defaultValue;
        this.implementationVersion = implementationVersion;
        minValue = null;
        maxValue = null;
    }

    DefaultOptions(String name, Integer defaultValue, Integer minValue, Integer maxValue, String implementationVersion) {
        this.name = name;
        this.objType = Integer.class;
        this.defaultValue = defaultValue;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.implementationVersion = implementationVersion;
    }


    DefaultOptions(String name, Integer[] defaultValue, Integer minValue, Integer maxValue, String implementationVersion) {
        this.name = name;
        this.objType = Integer.class;
        this.defaultValue = defaultValue;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.implementationVersion = implementationVersion;
    }

    public static Options defaultValues(HaMode haMode) {
        return parse(haMode, "", new Properties());
    }

    /**
     * Add a property (name + value) to current session options
     * @param haMode current haMode.
     * @param name property name
     * @param value new property value
     * @param options session options.
     * @return new session options.
     */
    public static Options addProperty(HaMode haMode, String name, String value, Options options) {
        Properties additionnalProperties = new Properties();
        additionnalProperties.put(name, value);
        return parse(haMode, additionnalProperties, options);
    }

    public static Options addProperty(HaMode haMode, Properties additionnalProperties, Options options) {
        return parse(haMode, additionnalProperties, options);
    }

    public static Options parse(HaMode haMode, String urlParameters, Options options) {
        Properties prop = new Properties();
        return parse(haMode, urlParameters, prop, options);
    }

    public static Options parse(HaMode haMode, String urlParameters, Properties properties) {
        return parse(haMode, urlParameters, properties, null);
    }

    /**
     * Parse additional properties .
     * @param haMode current haMode.
     * @param urlParameters options defined in url
     * @param properties options defined by properties
     * @param options initial options
     * @return options
     */
    public static Options parse(HaMode haMode, String urlParameters, Properties properties, Options options) {
        if (!"".equals(urlParameters)) {
            String[] parameters = urlParameters.split("&");
            for (String parameter : parameters) {
                int pos = parameter.indexOf('=');
                if (pos == -1) {
                    throw new IllegalArgumentException("Invalid connection URL, expected key=value pairs, found " + parameter);
                }
                if (!properties.containsKey(parameter.substring(0, pos))) {
                    properties.setProperty(parameter.substring(0, pos), parameter.substring(pos + 1));
                }
            }
        }
        return parse(haMode, properties, options);
    }

    private static Options parse(HaMode haMode, Properties properties, Options options) {
        boolean initial = false;
        if (options == null) {
            options = new Options();
            initial = true;
        }

        try {
            for (DefaultOptions o : DefaultOptions.values()) {

                String propertyValue = properties.getProperty(o.name);
                if (propertyValue == null) {
                    if (o.name.equals("createDatabaseIfNotExist")) {
                        propertyValue = properties.getProperty("createDB");
                    } else if (o.name.equals("useSsl")) {
                        propertyValue = properties.getProperty("useSSL");
                    }
                }

                if (propertyValue != null) {
                    if (o.objType.equals(String.class)) {
                        Options.class.getField(o.name).set(options, propertyValue);
                    } else if (o.objType.equals(Boolean.class)) {
                        String value = propertyValue.toLowerCase();
                        if ("1".equals(value)) {
                            value = "true";
                        } else if ("0".equals(value)) {
                            value = "false";
                        }
                        if (!"true".equals(value) && !"false".equals(value)) {
                            throw new IllegalArgumentException("Optional parameter " + o.name + " must be boolean (true/false or 0/1) was \""
                                    + propertyValue + "\"");
                        }
                        Options.class.getField(o.name).set(options, Boolean.valueOf(value));
                    } else if (o.objType.equals(Integer.class)) {
                        try {
                            Integer value = Integer.parseInt(propertyValue);
                            if (value.compareTo((Integer) o.minValue) < 0 || value.compareTo((Integer) o.maxValue) > 0) {
                                throw new IllegalArgumentException("Optional parameter " + o.name + " must be greater or equal to " + o.minValue
                                        + ((((Integer) o.maxValue).intValue() != Integer.MAX_VALUE) ? " and smaller than " + o.maxValue : " ")
                                        + ", was \"" + propertyValue + "\"");
                            }
                            Options.class.getField(o.name).set(options, value);
                        } catch (NumberFormatException n) {
                            throw new IllegalArgumentException("Optional parameter " + o.name + " must be Integer, was \"" + propertyValue + "\"");
                        }
                    }
                } else {
                    if (initial) {
                        if (o.defaultValue instanceof Integer[]) {
                            Options.class.getField(o.name).set(options, ((Integer[]) o.defaultValue)[haMode.ordinal()]);
                        } else {
                            Options.class.getField(o.name).set(options, o.defaultValue);
                        }
                    }
                }
            }
        } catch (NoSuchFieldException | IllegalAccessException n) {
            n.printStackTrace();
        } catch (SecurityException s) {
            //only for jws, so never thrown
            throw new IllegalArgumentException("Security too restrictive : " + s.getMessage());
        }
        return options;
    }

    /**
     * Get properties from session options.
     * @param options session options.
     * @return properties
     */
    public static Properties getProperties(Options options) {
        Properties prop = new Properties();
        try {
            for (DefaultOptions o : DefaultOptions.values()) {
                try {
                    Object obj = Options.class.getField(o.name).get(options);
                    if (obj != null) {
                        prop.put(o.name, String.valueOf(Options.class.getField(o.name).get(options)).toString());
                    }
                } catch (NoSuchFieldException exe) {
                    //eat exception
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return prop;
    }

    /**
     * Get properties value by name.
     * @param optionName String name
     * @param options session options.
     * @return string value of option if exists.
     */
    public static String getProperties(String optionName, Options options) {
        try {
            return String.valueOf(Options.class.getField(optionName).get(options));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Set a int value.
     * @param value int value to set.
     */
    private void setIntValue(Integer value) {
        this.value = value;
    }

    /**
     * Set a boolean value.
     * @param value boolean value to set.
     */
    private void setBooleanValue(Boolean value) {
        this.value = value;
    }

    /**
     * Return an integer value.
     * @return an integer
     */
    public int intValue() {
        if (objType.equals(Integer.class)) {
            if (value != null) {
                return ((Integer) value).intValue();
            } else {
                return ((Integer) defaultValue).intValue();
            }
        } else {
            throw new WrongMethodTypeException("Method " + name + " is of type " + objType + " intValue() does not apply");
        }
    }

    /**
     * Return boolean value.
     * @return a boolean.
     */
    public boolean boolValue() {
        if (objType.equals(Boolean.class)) {
            if (value != null) {
                return ((Boolean) value).booleanValue();
            } else {
                return ((Boolean) defaultValue).booleanValue();
            }
        } else {
            throw new WrongMethodTypeException("Method " + name + " is of type " + objType + " intValue() does not apply");
        }
    }

    /**
     * Return String value.
     * @return string
     */
    public String stringValue() {
        if (objType.equals(String.class)) {
            return ((String) value);
        } else {
            throw new WrongMethodTypeException("Method " + name + " is of type " + objType + " intValue() does not apply");
        }
    }
}