package org.mariadb.jdbc.failover;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.FailoverDBClusterRequest;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.MariaDbServerPreparedStatement;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.failover.AbstractMastersListener;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.HashMap;
import java.util.List;

/**
 * Base util class.
 * For testing
 * example mvn test -DdbUrl=jdbc:mysql://localhost:3306,localhost:3307/test?user=root -DlogLevel=FINEST
 * specific parameters :
 * defaultMultiHostUrl :
 */
@Ignore
public class BaseMultiHostTest {

    protected static String initialGaleraUrl;
    protected static String initialAuroraUrl;
    protected static String initialReplicationUrl;
    protected static String initialSequentialUrl;
    protected static String initialLoadbalanceUrl;
    protected static String initialUrl;
    protected String defaultUrl;

    protected static String proxyGaleraUrl;
    protected static String proxySequentialUrl;
    protected static String proxyAuroraUrl;
    protected static String proxyReplicationUrl;
    protected static String proxyLoadbalanceUrl;
    protected static String proxyUrl;
    protected static String jobId;


    protected static AmazonRDSClient amazonRDSClient;
    private static String auroraClusterIdentifier;

    protected static String username;
    private static String hostname;
    //hosts
    private static HashMap<HaMode, TcpProxy[]> proxySet = new HashMap<>();
    public HaMode currentType;

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void succeeded(Description description) {
            System.out.println("finished test success : " + description.getClassName() + "." + description.getMethodName());
        }

        protected void failed(Throwable throwable, Description description) {
            System.out.println("finished test failed : " + description.getClassName() + "." + description.getMethodName());
        }
    };

    /**
     * Initialize parameters.
     * @throws SQLException exception
     * @throws IOException exception
     */
    @BeforeClass
    public static void beforeClass() throws SQLException, IOException {

        initialUrl = System.getProperty("dbFailoverUrl");
        initialGaleraUrl = System.getProperty("defaultGaleraUrl");
        initialReplicationUrl = System.getProperty("defaultReplicationUrl");
        initialLoadbalanceUrl = System.getProperty("defaultLoadbalanceUrl");
        initialAuroraUrl = System.getProperty("defaultAuroraUrl");
        jobId = System.getProperty("jobId", "_0");

        if (initialUrl != null) {
            proxyUrl = createProxies(initialUrl, HaMode.NONE);
        }
        if (initialReplicationUrl != null) {
            proxyReplicationUrl = createProxies(initialReplicationUrl, HaMode.REPLICATION);
        }
        if (initialLoadbalanceUrl != null) {
            proxyLoadbalanceUrl = createProxies(initialLoadbalanceUrl, HaMode.LOADBALANCE);
        }
        if (initialGaleraUrl != null) {
            proxyGaleraUrl = createProxies(initialGaleraUrl, HaMode.FAILOVER);
        }
        if (initialGaleraUrl != null) {
            proxySequentialUrl = createProxies(initialGaleraUrl, HaMode.SEQUENTIAL);
        }
        if (initialAuroraUrl != null) {
            proxyAuroraUrl = createProxies(initialAuroraUrl, HaMode.AURORA);
            String auroraAccessKey = System.getProperty("AURORA_ACCESS_KEY");
            String auroraSecretKey = System.getProperty("AURORA_SECRET_KEY");
            auroraClusterIdentifier = System.getProperty("AURORA_CLUSTER_IDENTIFIER");
            if (auroraAccessKey != null && auroraSecretKey != null && auroraClusterIdentifier != null) {
                BasicAWSCredentials awsCreds = new BasicAWSCredentials(auroraAccessKey, auroraSecretKey);
                amazonRDSClient = new AmazonRDSClient(awsCreds);
            }
        }
    }

    /**
     * Delete table and procedure if created.
     * Close connection if needed
     * @throws SQLException exception
     */
    @After
    public void afterBaseTest() throws SQLException {
        assureProxy();
        assureBlackList();
    }

    /**
     * Check server minimum version.
     * @param connection connection to use
     * @param major major minimal number
     * @param minor minor minimal number
     * @return is server compatible
     * @throws SQLException exception
     */
    public static boolean requireMinimumVersion(Connection connection, int major, int minor) throws SQLException {
        DatabaseMetaData md = connection.getMetaData();
        int dbMajor = md.getDatabaseMajorVersion();
        int dbMinor = md.getDatabaseMinorVersion();
        return (dbMajor > major
                || (dbMajor == major && dbMinor >= minor));
    }

    private static String createProxies(String tmpUrl, HaMode proxyType) throws SQLException {
        UrlParser tmpUrlParser = UrlParser.parse(tmpUrl);
        TcpProxy[] tcpProxies = new TcpProxy[tmpUrlParser.getHostAddresses().size()];
        username = tmpUrlParser.getUsername();
        hostname = tmpUrlParser.getHostAddresses().get(0).host;
        String sockethosts = "";
        HostAddress hostAddress;
        for (int i = 0; i < tmpUrlParser.getHostAddresses().size(); i++) {
            try {
                hostAddress = tmpUrlParser.getHostAddresses().get(i);
                tcpProxies[i] = new TcpProxy(hostAddress.host, hostAddress.port);
                sockethosts += ",address=(host=localhost)(port=" + tcpProxies[i].getLocalPort() + ")"
                        + ((hostAddress.type != null) ? "(type=" + hostAddress.type + ")" : "");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        proxySet.put(proxyType, tcpProxies);
        if (tmpUrlParser.getHaMode().equals(HaMode.NONE)) {
            return "jdbc:mysql://" + sockethosts.substring(1) + "/" + tmpUrl.split("/")[3];
        } else {
            return "jdbc:mysql:" + tmpUrlParser.getHaMode().toString().toLowerCase() + "://" + sockethosts.substring(1)
                    + "/" + tmpUrl.split("/")[3];
        }

    }

    /**
     * Clean proxies.
     * @throws SQLException exception
     */
    @AfterClass
    public static void afterClass() throws SQLException {
        if (proxySet != null) {
            for (TcpProxy[] tcpProxies : proxySet.values()) {
                for (TcpProxy tcpProxy : tcpProxies) {
                    try {
                        tcpProxy.stop();
                    } catch (Exception e) {
                        //Eat exception
                    }
                }
            }
        }
    }

    protected Connection getNewConnection() throws SQLException {
        return getNewConnection(null, false);
    }

    protected Connection getNewConnection(boolean proxy) throws SQLException {
        return getNewConnection(null, proxy);
    }

    protected Connection getNewConnection(String additionnalConnectionData, boolean proxy) throws SQLException {
        return getNewConnection(additionnalConnectionData, proxy, false);
    }

    protected Connection getNewConnection(String additionnalConnectionData, boolean proxy, boolean forceNewProxy)
            throws SQLException {
        if (proxy) {
            String tmpProxyUrl = proxyUrl;
            if (forceNewProxy) {
                tmpProxyUrl = createProxies(defaultUrl, currentType);
            }
            if (additionnalConnectionData == null) {
                return DriverManager.getConnection(tmpProxyUrl);
            } else {
                return DriverManager.getConnection(tmpProxyUrl + additionnalConnectionData);
            }
        } else {
            if (additionnalConnectionData == null) {
                return DriverManager.getConnection(defaultUrl);
            } else {
                return DriverManager.getConnection(defaultUrl + additionnalConnectionData);
            }
        }
    }

    /**
     * Will launch an aurora failover.
     * (by using AWS api)
     */
    public void launchAuroraFailover() {
        FailoverDBClusterRequest request = new FailoverDBClusterRequest();
        request.setDBClusterIdentifier(auroraClusterIdentifier);
        amazonRDSClient.failoverDBCluster(request);
    }

    /**
     * Stop proxy, and restart it after a certain amount of time.
     * @param hostNumber hostnumber (first is one)
     * @param millissecond milliseconds
     */
    public void stopProxy(int hostNumber, long millissecond) {
        proxySet.get(currentType)[hostNumber - 1].restart(millissecond);
    }

    /**
     * Stop proxy.
     * @param hostNumber host number (first is 1)
     */
    public void stopProxy(int hostNumber) {
        proxySet.get(currentType)[hostNumber - 1].stop();
    }

    /**
     * Stop all proxy but the one in parameter.
     * @param hostNumber the proxy to not close
     */
    public void stopProxyButParameter(int hostNumber) {
        TcpProxy[] proxies = proxySet.get(currentType);
        for (int i = 0 ; i < proxies.length ; i++) {
            if (i != hostNumber - 1) {
                proxies[i].stop();
            }
        }
    }

    /**
     * Restart proxy.
     * @param hostNumber host number (first is  1)
     */
    public void restartProxy(int hostNumber) {
        if (hostNumber != -1) {
            proxySet.get(currentType)[hostNumber - 1].restart();
        }
    }

    /**
     * Assure that proxies are reset after each test.
     */
    public void assureProxy() {
        for (TcpProxy[] tcpProxies : proxySet.values()) {
            for (TcpProxy tcpProxy : tcpProxies) {
                tcpProxy.assureProxyOk();
            }
        }
    }

    /**
     * Assure that blacklist is reset after each test.
     */
    public void assureBlackList() {
        AbstractMastersListener.clearBlacklist();
    }

    /**
     * Does the user have super privileges or not.
     */
    public boolean hasSuperPrivilege(Connection connection, String testName) throws SQLException {
        boolean superPrivilege = false;
        Statement st = connection.createStatement();

        // first test for specific user and host combination
        ResultSet rs = st.executeQuery("SELECT Super_Priv FROM mysql.user WHERE user = '" + username + "' AND host = '"
                + hostname + "'");
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

        if (superPrivilege) {
            System.out.println("test '" + testName + "' skipped because user '" + username + "' has SUPER privileges");
        }

        return superPrivilege;
    }

    protected Protocol getProtocolFromConnection(Connection conn) throws Throwable {

        Method getProtocol = MariaDbConnection.class.getDeclaredMethod("getProtocol", new Class[0]);
        getProtocol.setAccessible(true);
        return (Protocol) getProtocol.invoke(conn);
    }

    /**
     * Retreive server Id.
     * @param connection connection
     * @return server index
     * @throws Throwable  exception
     */
    public int getServerId(Connection connection) throws Throwable {
        Protocol protocol = getProtocolFromConnection(connection);
        HostAddress hostAddress = protocol.getHostAddress();
        List<HostAddress> hostAddressList = protocol.getUrlParser().getHostAddresses();
        return hostAddressList.indexOf(hostAddress) + 1;
    }

    public boolean inTransaction(Connection connection) throws Throwable {
        Protocol protocol = getProtocolFromConnection(connection);
        return protocol.inTransaction();
    }

    boolean isMariaDbServer(Connection connection) throws SQLException {
        DatabaseMetaData md = connection.getMetaData();
        return md.getDatabaseProductVersion().indexOf("MariaDB") != -1;
    }

    PrepareResult getPrepareResult(MariaDbServerPreparedStatement preparedStatement) throws IllegalAccessException, NoSuchFieldException {
        Field prepareResultField = MariaDbServerPreparedStatement.class.getDeclaredField("prepareResult"); //NoSuchFieldException
        prepareResultField.setAccessible(true);
        return (PrepareResult) prepareResultField.get(preparedStatement); //IllegalAccessException
    }
}