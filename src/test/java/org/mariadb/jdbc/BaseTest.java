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


import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mariadb.jdbc.failover.TcpProxy;
import org.mariadb.jdbc.internal.failover.AbstractMastersListener;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.Options;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * Base util class.
 * For testing
 * mvn test -DdbUrl=jdbc:mariadb://localhost:3306/testj?user=root -DlogLevel=FINEST
 */
@Ignore
public class BaseTest {
    protected static final String mDefUrl = "jdbc:mariadb://localhost:3306/testj?user=root";
    protected static String connU;
    protected static String connUri;
    protected static String hostname;
    protected static int port;
    protected static String database;
    protected static String username;
    protected static String password;
    protected static String parameters;
    protected static boolean testSingleHost;
    protected static Connection sharedConnection;
    protected static boolean runLongTest = false;
    protected static boolean doPrecisionTest = true;
    private static Deque<String> tempTableList = new ArrayDeque<String>();
    private static Deque<String> tempViewList = new ArrayDeque<String>();
    private static Deque<String> tempProcedureList = new ArrayDeque<String>();
    private static Deque<String> tempFunctionList = new ArrayDeque<String>();
    private static TcpProxy proxy = null;
    private static UrlParser urlParser;
    private static final NumberFormat numberFormat = DecimalFormat.getInstance();

    @Rule
    public TestRule watcher = new TestWatcher() {
        private long ttime;
        protected void starting(Description description) {
            if (testSingleHost) {
                System.out.println("start test : " + description.getClassName() + "." + description.getMethodName());
                ttime = System.nanoTime();
            }
        }

        //execute another query to ensure connection is stable
        protected void finished(Description description) {
            if (testSingleHost) {
                Random random = new Random();
                int randInt = random.nextInt();
                PreparedStatement preparedStatement = null;
                try {
                    preparedStatement = sharedConnection.prepareStatement("SELECT " + randInt);
                    ResultSet rs = preparedStatement.executeQuery();
                    assertTrue(rs.next());
                    assertEquals(randInt, rs.getInt(1));
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Prepare after test fail for " + description.getClassName() + "." + description.getMethodName());
                } finally {
                    try {
                        if (preparedStatement != null) preparedStatement.close();
                    } catch (SQLException sqle) {
                        //eat
                    }
                }
            }
        }

        protected void succeeded(Description description) {
            if (testSingleHost) {
                System.out.println("finished test success : " + description.getClassName() + "." + description.getMethodName()
                        + " after " + numberFormat.format(((double) System.nanoTime() - ttime) / 1000000) + " ms");
            }
        }

        protected void failed(Throwable throwable, Description description) {
            if (testSingleHost) {
                System.out.println("finished test failed : " + description.getClassName() + "." + description.getMethodName()
                        + " after " + numberFormat.format(((double) System.nanoTime() - ttime) / 1000000) + " ms");
            }
        }
    };

    /**
     * Initialization.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClassBaseTest() throws SQLException {
        String url = System.getProperty("dbUrl", mDefUrl);
        runLongTest = Boolean.getBoolean(System.getProperty("runLongTest", "false"));
        testSingleHost = Boolean.parseBoolean(System.getProperty("testSingleHost", "true"));

        if (testSingleHost) {
            urlParser = UrlParser.parse(url);
            if (urlParser.getHostAddresses().size() > 0) {
                hostname = urlParser.getHostAddresses().get(0).host;
                port = urlParser.getHostAddresses().get(0).port;
            } else {
                hostname = null;
                port = 3306;
            }
            database = urlParser.getDatabase();
            username = urlParser.getUsername();
            password = urlParser.getPassword();
            int separator = url.indexOf("//");
            String urlSecondPart = url.substring(separator + 2);
            int dbIndex = urlSecondPart.indexOf("/");
            int paramIndex = urlSecondPart.indexOf("?");

            String additionalParameters;
            if ((dbIndex < paramIndex && dbIndex < 0) || (dbIndex > paramIndex && paramIndex > -1)) {
                additionalParameters = urlSecondPart.substring(paramIndex);
            } else if ((dbIndex < paramIndex && dbIndex > -1) || (dbIndex > paramIndex && paramIndex < 0)) {
                additionalParameters = urlSecondPart.substring(dbIndex);
            } else {
                additionalParameters = null;
            }
            if (additionalParameters != null) {
                String regex = "(\\/[^\\?]*)(\\?.+)*|(\\?[^\\/]*)(\\/.+)*";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(additionalParameters);
                if (matcher.find()) {
                    String options1 = (matcher.group(2) != null) ? matcher.group(2).substring(1) : "";
                    String options2 = (matcher.group(3) != null) ? matcher.group(3).substring(1) : "";
                    parameters = (!options1.equals("")) ? options1 : options2;
                }
            } else {
                parameters = null;
            }


            setUri();
            urlParser.auroraPipelineQuirks();
            sharedConnection = DriverManager.getConnection(url);

            String dbVersion = sharedConnection.getMetaData().getDatabaseProductVersion();
            doPrecisionTest = isMariadbServer() || !dbVersion.startsWith("5.5"); //MySQL 5.5 doesn't support precision
        }
    }

    private static void setUri() {
        connU = "jdbc:mariadb://" + ((hostname == null) ? "localhost" : hostname) + ":" + port + "/" + database;
        connUri = connU + "?user=" + username
                + (password != null && !"".equals(password) ? "&password=" + password : "")
                + (parameters != null ? "&" + parameters : "");
    }

    /**
     * Destroy the test tables.
     *
     * @throws SQLException exception
     */
    @AfterClass
    public static void afterClassBaseTest() throws SQLException {
        if (testSingleHost) {
            if (sharedConnection != null && !sharedConnection.isClosed()) {
                if (!tempViewList.isEmpty()) {
                    Statement stmt = sharedConnection.createStatement();
                    String viewName;
                    while ((viewName = tempViewList.poll()) != null) {
                        try {
                            stmt.execute("DROP VIEW IF EXISTS " + viewName);
                        } catch (SQLException e) {
                            //eat exception
                        }
                    }
                }
                if (!tempTableList.isEmpty()) {
                    Statement stmt = sharedConnection.createStatement();
                    String tableName;
                    while ((tableName = tempTableList.poll()) != null) {
                        try {
                            stmt.execute("DROP TABLE IF EXISTS " + tableName);
                        } catch (SQLException e) {
                            //eat exception
                        }
                    }
                }
                if (!tempProcedureList.isEmpty()) {
                    Statement stmt = sharedConnection.createStatement();
                    String procedureName;
                    while ((procedureName = tempProcedureList.poll()) != null) {
                        try {
                            stmt.execute("DROP procedure IF EXISTS " + procedureName);
                        } catch (SQLException e) {
                            //eat exception
                        }
                    }
                }
                if (!tempFunctionList.isEmpty()) {
                    Statement stmt = sharedConnection.createStatement();
                    String functionName;
                    while ((functionName = tempFunctionList.poll()) != null) {
                        try {
                            stmt.execute("DROP FUNCTION IF EXISTS " + functionName);
                        } catch (SQLException e) {
                            //eat exception
                        }
                    }
                }

                try {
                    sharedConnection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // common function for logging information
    static void logInfo(String message) {
        System.out.println(message);
    }

    /**
     * Create a table that will be detroyed a the end of tests.
     *
     * @param tableName    table name
     * @param tableColumns table columns
     * @throws SQLException exception
     */
    public static void createTable(String tableName, String tableColumns) throws SQLException {
        createTable(tableName, tableColumns, null);
    }

    /**
     * Create a table that will be detroyed a the end of tests.
     *
     * @param tableName    table name
     * @param tableColumns table columns
     * @param engine       engine type
     * @throws SQLException exception
     */
    public static void createTable(String tableName, String tableColumns, String engine) throws SQLException {
        if (testSingleHost) {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("drop table if exists " + tableName);
            stmt.execute("create table " + tableName + " (" + tableColumns + ") " + ((engine != null) ? engine : ""));
            if (!tempFunctionList.contains(tableName)) tempTableList.add(tableName);
        }
    }

    /**
     * Create a view that will be detroyed a the end of tests.
     *
     * @param viewName     table name
     * @param tableColumns table columns
     * @throws SQLException exception
     */
    public static void createView(String viewName, String tableColumns) throws SQLException {
        if (testSingleHost) {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("drop view if exists " + viewName);
            stmt.execute("create view " + viewName + " AS (" + tableColumns + ") ");
            if (!tempViewList.contains(viewName)) tempViewList.add(viewName);
        }
    }

    /**
     * Create procedure that will be delete on end of test.
     *
     * @param name procedure name
     * @param body procecure body
     * @throws SQLException exception
     */
    public static void createProcedure(String name, String body) throws SQLException {
        if (testSingleHost) {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("drop procedure IF EXISTS " + name);
            stmt.execute("create  procedure " + name + body);
            if (!tempProcedureList.contains(name)) tempProcedureList.add(name);
        }
    }

    /**
     * Create function that will be delete on end of test.
     *
     * @param name function name
     * @param body function body
     * @throws SQLException exception
     */
    public static void createFunction(String name, String body) throws SQLException {
        if (testSingleHost) {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("drop function IF EXISTS " + name);
            stmt.execute("create function " + name + body);
            if (!tempFunctionList.contains(name)) tempFunctionList.add(name);
        }
    }

    /**
     * Check if current DB server is MariaDB.
     *
     * @return true if DB is mariadb
     * @throws SQLException exception
     */
    static boolean isMariadbServer() throws SQLException {
        DatabaseMetaData md = sharedConnection.getMetaData();
        return md.getDatabaseProductVersion().indexOf("MariaDB") != -1;
    }

    /**
     * Create a connection with proxy.
     *
     * @param info additionnal properties
     * @return a proxyfied connection
     * @throws SQLException if any error occur
     */
    public Connection createProxyConnection(Properties info) throws SQLException {
        UrlParser tmpUrlParser = UrlParser.parse(connUri);
        username = tmpUrlParser.getUsername();
        hostname = tmpUrlParser.getHostAddresses().get(0).host;
        String sockethosts = "";
        HostAddress hostAddress;
        try {
            hostAddress = tmpUrlParser.getHostAddresses().get(0);
            proxy = new TcpProxy(hostAddress.host, hostAddress.port);
            sockethosts += "address=(host=localhost)(port=" + proxy.getLocalPort() + ")"
                    + ((hostAddress.type != null) ? "(type=" + hostAddress.type + ")" : "");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return openConnection("jdbc:mariadb://" + sockethosts + "/" + connUri.split("/")[3], info);

    }

    /**
     * Stop proxy, and restart it after a certain amount of time.
     *
     * @param millissecond milliseconds
     */
    public void stopProxy(long millissecond) {
        proxy.restart(millissecond);
    }

    /**
     * Stop proxy.
     */
    public void stopProxy() {
        proxy.stop();
    }

    /**
     * Restart proxy.
     */
    public void restartProxy() {
        proxy.restart();
    }

    /**
     * Clean proxies.
     *
     * @throws SQLException exception
     */
    public void closeProxy() throws SQLException {
        try {
            proxy.stop();
        } catch (Exception e) {
            //Eat exception
        }
    }

    @Before
    public void init() throws SQLException {
        Assume.assumeTrue(testSingleHost);
    }

    /**
     * Permit to assure that host are not in a blacklist after a test.
     *
     * @param connection connection
     */
    public void assureBlackList(Connection connection) {
        AbstractMastersListener.clearBlacklist();
    }

    protected Protocol getProtocolFromConnection(Connection conn) throws Throwable {

        Method getProtocol = MariaDbConnection.class.getDeclaredMethod("getProtocol", new Class[0]);
        getProtocol.setAccessible(true);
        Object obj = getProtocol.invoke(conn);
        return (Protocol) obj;
    }

    protected void setHostname(String hostname) throws SQLException {
        BaseTest.hostname = hostname;
        setUri();
        setConnection();
    }

    protected void setPort(int port) throws SQLException {
        BaseTest.port = port;
        setUri();
        setConnection();
    }

    protected void setDatabase(String database) throws SQLException {
        BaseTest.database = database;
        BaseTest.setUri();
        setConnection();
    }

    protected void setUsername(String username) throws SQLException {
        BaseTest.username = username;
        setUri();
        setConnection();
    }

    protected void setPassword(String password) throws SQLException {
        BaseTest.password = password;
        setUri();
        setConnection();
    }

    protected Connection setBlankConnection(String parameters) throws SQLException {
        return openConnection(connU
                + "?user=" + username
                + (password != null && !"".equals(password) ? "&password=" + password : "")
                + parameters, null);
    }

    protected Connection setConnection() throws SQLException {
        return openConnection(connUri, null);
    }

    protected Connection setConnection(Map<String, String> props) throws SQLException {
        Properties info = new Properties();
        for (String key : props.keySet()) {
            info.setProperty(key, props.get(key));
        }
        return openConnection(connU, info);
    }

    protected Connection setConnection(Properties info) throws SQLException {
        return openConnection(connUri, info);
    }

    protected Connection setConnection(String parameters) throws SQLException {
        return openConnection(connUri + parameters, null);
    }

    protected Connection setConnection(String additionnallParameters, String database) throws SQLException {
        String connU = "jdbc:mariadb://" + ((hostname == null) ? "localhost" : hostname) + ":" + port + "/" + database;
        String connUri = connU + "?user=" + username
                + (password != null && !"".equals(password) ? "&password=" + password : "")
                + (parameters != null ? "&" + parameters : "");
        return openConnection(connUri + additionnallParameters, null);
    }

    /**
     * Permit to reconstruct a connection.
     *
     * @param uri  base uri
     * @param info additionnal properties
     * @return A connection
     * @throws SQLException is any error occur
     */
    public Connection openConnection(String uri, Properties info) throws SQLException {
        if (info == null) {
            return DriverManager.getConnection(uri);
        } else {
            return DriverManager.getConnection(uri, info);
        }
    }

    protected Connection openNewConnection(String url) throws SQLException {
        return DriverManager.getConnection(url);
    }

    protected Connection openNewConnection(String url, Properties info) throws SQLException {
        return DriverManager.getConnection(url, info);
    }

    boolean checkMaxAllowedPacketMore8m(String testName) throws SQLException {
        Statement st = sharedConnection.createStatement();
        ResultSet rs = st.executeQuery("select @@max_allowed_packet");
        rs.next();
        long maxAllowedPacket = rs.getLong(1);

        rs = st.executeQuery("select @@innodb_log_file_size");
        rs.next();
        long innodbLogFileSize = rs.getLong(1);

        if (maxAllowedPacket < 8 * 1024 * 1024L) {

            System.out.println("test '" + testName + "' skipped  due to server variable max_allowed_packet < 8M");
            return false;
        }
        if (innodbLogFileSize < 80 * 1024 * 1024L) {
            System.out.println("test '" + testName + "' skipped  due to server variable innodb_log_file_size < 80M");
            return false;
        }
        return true;
    }

    boolean checkMaxAllowedPacketMore20m(String testName) throws SQLException {
        return checkMaxAllowedPacketMore20m(testName, true);
    }

    boolean checkMaxAllowedPacketMore20m(String testName, boolean displayMessage) throws SQLException {
        Statement st = sharedConnection.createStatement();
        ResultSet rs = st.executeQuery("select @@max_allowed_packet");
        rs.next();
        long maxAllowedPacket = rs.getLong(1);

        rs = st.executeQuery("select @@innodb_log_file_size");
        rs.next();
        long innodbLogFileSize = rs.getLong(1);

        if (maxAllowedPacket < 20 * 1024 * 1024L) {

            if (displayMessage) {
                System.out.println("test '" + testName + "' skipped  due to server variable max_allowed_packet < 20M");
            }
            return false;
        }
        if (innodbLogFileSize < 200 * 1024 * 1024L) {
            if (displayMessage) {
                System.out.println("test '" + testName + "' skipped  due to server variable innodb_log_file_size < 200M");
            }
            return false;
        }
        return true;
    }

    boolean checkMaxAllowedPacketMore40m(String testName) throws SQLException {
        return checkMaxAllowedPacketMore40m(testName, true);
    }

    boolean checkMaxAllowedPacketMore40m(String testName, boolean displayMsg) throws SQLException {
        Statement st = sharedConnection.createStatement();
        ResultSet rs = st.executeQuery("select @@max_allowed_packet");
        rs.next();
        long maxAllowedPacket = rs.getLong(1);

        rs = st.executeQuery("select @@innodb_log_file_size");
        rs.next();
        long innodbLogFileSize = rs.getLong(1);


        if (maxAllowedPacket < 40 * 1024 * 1024L) {
            if (displayMsg) {
                System.out.println("test '" + testName + "' skipped  due to server variable max_allowed_packet < 40M");
            }
            return false;
        }
        if (innodbLogFileSize < 400 * 1024 * 1024L) {
            if (displayMsg) {
                System.out.println("test '" + testName + "' skipped  due to server variable innodb_log_file_size < 400M");
            }
            return false;
        }

        return true;
    }

    //does the user have super privileges or not?
    boolean hasSuperPrivilege(String testName) throws SQLException {
        boolean superPrivilege = false;
        Statement st = sharedConnection.createStatement();
        // first test for specific user and host combination
        ResultSet rs = st.executeQuery("SELECT Super_Priv FROM mysql.user WHERE user = '" + username + "' AND host = '" + hostname + "'");
        if (rs.next()) {
            superPrivilege = (rs.getString(1).equals("Y"));
        } else {
            // then check for user on whatever (%) host
            ResultSet rs2 = st.executeQuery("SELECT Super_Priv FROM mysql.user WHERE user = '" + username + "' AND host = '%'");
            if (rs2.next()) superPrivilege = (rs2.getString(1).equals("Y"));
        }

        if (!superPrivilege) {
            System.out.println("test '" + testName + "' skipped because user '" + username + "' doesn't have SUPER privileges");
        }

        return superPrivilege;
    }

    //is the connection local?
    boolean isLocalConnection(String testName) {
        boolean isLocal = false;

        try {
            if (InetAddress.getByName(hostname).isAnyLocalAddress() || InetAddress.getByName(hostname).isLoopbackAddress()) {
                isLocal = true;
            }
        } catch (UnknownHostException e) {
            // for some reason it wasn't possible to parse the hostname
            // do nothing
        }

        if (!isLocal) {
            System.out.println("test '" + testName + "' skipped because connection is not local");
        }

        return isLocal;
    }

    boolean haveSsl(Connection connection) {
        try {
            ResultSet rs = connection.createStatement().executeQuery("select @@have_ssl");
            rs.next();
            String value = rs.getString(1);
            return value.equals("YES");
        } catch (Exception e) {
            return false; /* maybe 4.x ? */
        }
    }

    /**
     * Check if version if at minimum the version asked.
     *
     * @param major database major version
     * @param minor database minor version
     * @throws SQLException exception
     */
    public boolean minVersion(int major, int minor) throws SQLException {
        DatabaseMetaData md = sharedConnection.getMetaData();
        int dbMajor = md.getDatabaseMajorVersion();
        int dbMinor = md.getDatabaseMinorVersion();
        return (dbMajor > major
                || (dbMajor == major && dbMinor >= minor));

    }

    /**
     * Check if version if before the version asked.
     *
     * @param major database major version
     * @param minor database minor version
     * @throws SQLException exception
     */
    public boolean strictBeforeVersion(int major, int minor) throws SQLException {
        DatabaseMetaData md = sharedConnection.getMetaData();
        int dbMajor = md.getDatabaseMajorVersion();
        int dbMinor = md.getDatabaseMinorVersion();
        return (dbMajor < major || (dbMajor == major && dbMinor < minor));

    }

    /**
     * Cancel if database version match.
     *
     * @param major db major version
     * @param minor db minor version
     * @throws SQLException exception
     */
    public void cancelForVersion(int major, int minor) throws SQLException {

        String dbVersion = sharedConnection.getMetaData().getDatabaseProductVersion();
        Assume.assumeFalse(dbVersion.startsWith(major + "." + minor));

    }

    /**
     * Cancel if database version match.
     *
     * @param major db major version
     * @param minor db minor version
     * @param patch db patch version
     * @throws SQLException exception
     */
    public void cancelForVersion(int major, int minor, int patch) throws SQLException {
        String dbVersion = sharedConnection.getMetaData().getDatabaseProductVersion();
        Assume.assumeFalse(dbVersion.startsWith(major + "." + minor + "." + patch));
    }

    void requireMinimumVersion(int major, int minor) throws SQLException {
        Assume.assumeTrue(minVersion(major, minor));

    }

    /**
     * Change session time zone.
     *
     * @param connection connection
     * @param timeZone   timezone to set
     * @throws SQLException exception
     */
    public void setSessionTimeZone(Connection connection, String timeZone) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("set @@session.time_zone = '" + timeZone + "'");
    }

    /**
     * Get row number.
     *
     * @param tableName table name
     * @return resultset number in this table
     * @throws SQLException if error occur
     */
    public int getRowCount(String tableName) throws SQLException {
        ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
        if (rs.next()) {
            return rs.getInt(1);
        }
        throw new SQLException("No table " + tableName + " found");
    }

    /**
     * Permit to know if sharedConnection will use Prepare.
     * (in case dbUrl modify default options)
     *
     * @return true if PreparedStatement will use Prepare.
     */
    public boolean sharedUsePrepare() {
        return urlParser.getOptions().useServerPrepStmts
                && !urlParser.getOptions().rewriteBatchedStatements;
    }

    /**
     * Permit access to current sharedConnection options.
     *
     * @return Options
     */
    public Options sharedOptions() {
        return urlParser.getOptions();
    }

    /**
     * Permit to know if sharedConnection use rewriteBatchedStatements.
     *
     * @return true if option rewriteBatchedStatements is set to true
     */
    public boolean sharedIsRewrite() {
        return urlParser.getOptions().rewriteBatchedStatements;
    }

    /**
     * Has server bulk capacity.
     *
     * @return true if server has bulk capacity and option not disabled
     */
    public boolean sharedBulkCapacity() {
        return urlParser.getOptions().useBatchMultiSend;
    }

    /**
     * Permit to know if sharedConnection use compression.
     *
     * @return true if option compression is set to true
     */
    public boolean sharedUseCompression() {
        return urlParser.getOptions().useCompression;
    }

    public boolean sharedIsAurora() {
        return urlParser.isAurora();
    }
}
