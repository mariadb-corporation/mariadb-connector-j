package org.mariadb.jdbc;


import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mariadb.jdbc.internal.mysql.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Base util class.
 * For testing
 * mvn test -DdbUrl=jdbc:mysql://localhost:3306/test?user=root -DlogLevel=FINEST
 */
@Ignore
public class BaseTest {
    protected static final Logger log = LoggerFactory.getLogger(BaseTest.class);
    protected static final String mDefUrl = "jdbc:mysql://localhost:3306/test?user=root";
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
    private static List<String> tempTableList = new ArrayList<>();
    private static List<String> tempProcedureList = new ArrayList<>();

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void succeeded(Description description) {
            log.trace("finished test success : " + description.getClassName() + "." + description.getMethodName());
        }

        protected void failed(Throwable throwable, Description description) {
            log.trace("finished test failed : " + description.getClassName() + "." + description.getMethodName());
        }
    };

    /**
     * Initialization.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClassBaseTest() throws SQLException {
        String url = System.getProperty("dbUrl", mDefUrl);
        testSingleHost = Boolean.parseBoolean(System.getProperty("testSingleHost", "true"));
        UrlParser urlParser = UrlParser.parse(url);

        hostname = urlParser.getHostAddresses().get(0).host;
        port = urlParser.getHostAddresses().get(0).port;
        database = urlParser.getDatabase();
        username = urlParser.getUsername();
        password = urlParser.getPassword();

        log.debug("Properties parsed from JDBC URL - hostname: " + hostname + ", port: " + port + ", database: "
                + database + ", username: " + username + ", password: " + password);

        setUri();

        sharedConnection = DriverManager.getConnection(connUri);
    }


    private static void setUri() {
        connU = "jdbc:mysql://" + hostname + ":" + port + "/" + database;
        connUri = connU + "?user=" + username
                + (password != null && !"".equals(password) ? "&password=" + password : "")
                + (parameters != null ? parameters : "");
    }

    /**
     * Destroy the test tables.
     * @throws SQLException exception
     */
    @AfterClass
    public static void afterClassBaseTest() throws SQLException {
        if (!sharedConnection.isClosed()) {
            if (!tempTableList.isEmpty()) {
                Statement stmt = sharedConnection.createStatement();
                for (String tableName : tempTableList) {
                    try {
                        stmt.execute("DROP TABLE IF EXISTS " + tableName);
                    } catch (SQLException e) {
                        //eat exception
                    }
                }
                tempTableList.clear();
            }
            if (!tempProcedureList.isEmpty()) {
                Statement stmt = sharedConnection.createStatement();
                for (String procedureName : tempProcedureList) {
                    try {
                        stmt.execute("DROP procedure IF EXISTS " + procedureName);
                    } catch (SQLException e) {
                        //eat exception
                    }
                }
                tempTableList.clear();
            }
        }
        try {
            sharedConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // common function for logging information
    static void logInfo(String message) {
        log.info(message);
    }

    /**
     * Create a table that will be detroyed a the end of tests.
     * @param tableName table name
     * @param tableColumns table columns
     * @throws SQLException exception
     */
    public static void createTable(String tableName, String tableColumns) throws SQLException {
        createTable(tableName, tableColumns, null);
    }

    /**
     * Create a table that will be detroyed a the end of tests.
     * @param tableName table name
     * @param tableColumns table columns
     * @param engine engine type
     * @throws SQLException exception
     */
    public static void createTable(String tableName, String tableColumns, String engine) throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop table  if exists " + tableName);
        stmt.execute("create table " + tableName + " (" + tableColumns + ") " + ((engine != null) ? engine : ""));
        tempTableList.add(tableName);
    }

    /**
     * Create procedure that will be delete on end of test.
     * @param name procedure name
     * @param body procecure body
     * @throws SQLException exception
     */
    public static void createProcedure(String name, String body) throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop procedure IF EXISTS " + name);
        stmt.execute("create  procedure " + name + body);
        tempProcedureList.add(name);

    }

    @Before
    public void init() throws SQLException {
        Assume.assumeTrue(testSingleHost);
    }

    /**
     * Permit to assure that host are not in a blacklist after a test.
     * @param connection connection
     */
    public void assureBlackList(Connection connection) {
        try {
            Protocol protocol = getProtocolFromConnection(connection);
            protocol.getProxy().getListener().getBlacklist().clear();
        } catch (Throwable e) {
            //eat exception.
        }
    }

    protected Protocol getProtocolFromConnection(Connection conn) throws Throwable {

        Method getProtocol = MariaDbConnection.class.getDeclaredMethod("getProtocol", new Class[0]);
        getProtocol.setAccessible(true);
        return (Protocol) getProtocol.invoke(conn);
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
        String connU = "jdbc:mysql://" + hostname + ":" + port + "/" + database;
        String connUri = connU + "?user=" + username
                + (password != null && !"".equals(password) ? "&password=" + password : "")
                + (parameters != null ? parameters : "");
        return openConnection(connUri + additionnallParameters, null);
    }

    private Connection openConnection(String uri, Properties info) throws SQLException {
        if (info == null) {
            return DriverManager.getConnection(uri);
        } else {
            return DriverManager.getConnection(uri, info);
        }
    }

    protected Connection openNewConnection() throws SQLException {
        Properties info = sharedConnection.getClientInfo();
        return openNewConnection(connUri, info);
    }

    protected Connection openNewConnection(String url) throws SQLException {
        return DriverManager.getConnection(url);
    }

    protected Connection openNewConnection(String url, Properties info) throws SQLException {
        return DriverManager.getConnection(url, info);
    }

    boolean checkMaxAllowedPacket(String testName) throws SQLException {
        Statement st = sharedConnection.createStatement();
        ResultSet rs = st.executeQuery("select @@max_allowed_packet");
        rs.next();
        int maxAllowedPacket = rs.getInt(1);

        rs = st.executeQuery("select @@innodb_log_file_size");
        rs.next();
        int innodbLogFileSize = rs.getInt(1);

        if (maxAllowedPacket < 16 * 1024 * 1024) {
            log.info("test '" + testName + "' skipped  due to server variable max_allowed_packet < 16M");
            return false;
        }
        if (innodbLogFileSize < 16 * 1024 * 1024) {
            log.info("test '" + testName + "' skipped  due to server variable innodb_log_file_size < 16M");
            return false;
        }
        return true;
    }

    boolean checkMaxAllowedPacketMore40m(String testName) throws SQLException {
        Statement st = sharedConnection.createStatement();
        ResultSet rs = st.executeQuery("select @@max_allowed_packet");
        rs.next();
        int maxAllowedPacket = rs.getInt(1);

        rs = st.executeQuery("select @@innodb_log_file_size");
        rs.next();
        int innodbLogFileSize = rs.getInt(1);


        if (maxAllowedPacket < 40 * 1024 * 1024) {
            log.info("test '" + testName + "' skipped  due to server variable max_allowed_packet < 40M");
            return false;
        }
        if (innodbLogFileSize < 160 * 1024 * 1024) {
            log.info("test '" + testName + "' skipped  due to server variable innodb_log_file_size < 160M");
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
            superPrivilege = (rs.getString(1).equals("Y") ? true : false);
        } else {
            // then check for user on whatever (%) host
            rs = st.executeQuery("SELECT Super_Priv FROM mysql.user WHERE user = '" + username + "' AND host = '%'");
            if (rs.next()) {
                superPrivilege = (rs.getString(1).equals("Y") ? true : false);
            }
        }

        rs.close();

        if (!superPrivilege) {
            log.info("test '" + testName + "' skipped because user '" + username + "' doesn't have SUPER privileges");
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

        if (isLocal == false) {
            log.info("test '" + testName + "' skipped because connection is not local");
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

    void requireMinimumVersion(int major, int minor) throws SQLException {
        org.junit.Assume.assumeTrue(minVersion(major, minor));

    }

    /**
     * Check if current DB server is MariaDB.
     * @return true if DB is mariadb
     * @throws SQLException exception
     */
    boolean isMariadbServer() throws SQLException {
        DatabaseMetaData md = sharedConnection.getMetaData();
        return md.getDatabaseProductVersion().indexOf("MariaDB") != -1;
    }

    /**
     * Change session time zone.
     * @param connection connection
     * @param timeZone timezone to set
     * @throws SQLException exception
     */
    public void setSessionTimeZone(Connection connection, String timeZone) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("set @@session.time_zone = '" + timeZone + "'");
        statement.close();
    }

}
