package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.common.HaMode;
import org.mariadb.jdbc.internal.mysql.Protocol;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Test for sequential connection
 * exemple mvn test  -DdefaultGaleraUrl=jdbc:mysql:sequential//localhost:3306,localhost:3307/test?user=root.
 */
public class SequentialFailoverTest extends BaseMultiHostTest {
    protected Connection connection;

    /**
     * Initialisation of failover parameters.
     * @throws SQLException exception
     */
    @Before
    public void init() throws SQLException {
        currentType = HaMode.SEQUENTIAL;
        initialUrl = initialSequentialUrl;
        proxyUrl = proxySequentialUrl;
        Assume.assumeTrue(initialSequentialUrl != null);
        connection = null;
    }

    /**
     * Closing proxies.
     * @throws SQLException exception
     */
    @After
    public void after() throws SQLException {
        assureProxy();
        assureBlackList(connection);
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void connectionOrder() throws Throwable {
        Assume.assumeTrue(!initialGaleraUrl.contains("failover"));
        UrlParser urlParser = UrlParser.parse(initialGaleraUrl);
        for (int i = 0; i < urlParser.getHostAddresses().size(); i++) {
            connection = getNewConnection(true);
            int serverNb = getServerId(connection);
            Assert.assertTrue(serverNb == i + 1);
            connection.close();
            stopProxy(serverNb);
        }
    }

    @Test
    public void checkStaticBlacklist() throws Throwable {
        try {
            connection = getNewConnection("&loadBalanceBlacklistTimeout=500", true);
            Statement st = connection.createStatement();

            int firstServerId = getServerId(connection);
            stopProxy(firstServerId);

            try {
                st.execute("SELECT 1");
                Assert.fail();
            } catch (SQLException e) {
                //normal exception that permit to blacklist the failing connection.
            }

            //check blacklist size
            try {
                Protocol protocol = getProtocolFromConnection(connection);
                log.debug("backlist size : " + protocol.getProxy().getListener().getBlacklist().size());
                Assert.assertTrue(protocol.getProxy().getListener().getBlacklist().size() == 1);

                //replace proxified HostAddress by normal one
                UrlParser urlParser = UrlParser.parse(initialUrl);
                protocol.getProxy().getListener().getBlacklist().put(urlParser.getHostAddresses().get(firstServerId - 1),
                        System.currentTimeMillis());
            } catch (Throwable e) {
                e.printStackTrace();
                Assert.fail();
            }

            //add first Host to blacklist
            Protocol protocol = getProtocolFromConnection(connection);
            protocol.getProxy().getListener().getBlacklist().size();

            ExecutorService exec = Executors.newFixedThreadPool(2);

            //check blacklist shared
            exec.execute(new CheckBlacklist(firstServerId, protocol.getProxy().getListener().getBlacklist()));
            exec.execute(new CheckBlacklist(firstServerId, protocol.getProxy().getListener().getBlacklist()));
            //wait for thread endings

            exec.shutdown();
            try {
                exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                //eat exception
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testMultiHostWriteOnMaster() throws Throwable {
        Assume.assumeTrue(initialGaleraUrl != null);
        Connection connection = null;
        log.debug("testMultiHostWriteOnMaster begin");
        try {
            connection = getNewConnection();
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinode");
            stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
            log.debug("testMultiHostWriteOnMaster OK");
        } finally {
            assureProxy();
            assureBlackList(connection);
            log.debug("testMultiHostWriteOnMaster done");
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void pingReconnectAfterRestart() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=1&failOnReadOnly=false"
                + "&queriesBeforeRetryMaster=50000", true);
        Statement st = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);
        long stoppedTime = System.currentTimeMillis();

        try {
            st.execute("SELECT 1");
        } catch (SQLException e) {
            //eat exception
        }
        restartProxy(masterServerId);
        long restartTime = System.currentTimeMillis();
        boolean loop = true;
        while (loop) {
            if (!connection.isClosed()) {
                log.debug("reconnection with failover loop after : " + (System.currentTimeMillis() - stoppedTime)
                        + "ms");
                loop = false;
            }
            if (System.currentTimeMillis() - restartTime > 15 * 1000) {
                Assert.fail();
            }
            Thread.sleep(250);
        }
    }

    @Test
    public void socketTimeoutTest() throws SQLException {

        // set a short connection timeout
        connection = getNewConnection("&socketTimeout=15000&retriesAllDown=1", false);

        PreparedStatement ps = connection.prepareStatement("SELECT 1");
        ResultSet rs = ps.executeQuery();
        rs.next();

        // wait for the connection to time out
        ps = connection.prepareStatement("SELECT sleep(16)");

        // a timeout should occur here
        try {
            ps.executeQuery();
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("timed out"));
        }
        try {
            ps = connection.prepareStatement("SELECT 2");
            ps.execute();
        } catch (Exception e) {
            Assert.fail();
        }

        // the connection should not be closed
        assertTrue(!connection.isClosed());
    }

    protected class CheckBlacklist implements Runnable {
        int firstServerId;
        Map<HostAddress, Long> blacklist;

        public CheckBlacklist(int firstServerId, Map<HostAddress, Long> blacklist) {
            this.firstServerId = firstServerId;
            this.blacklist = blacklist;
        }

        public void run() {
            Connection connection2 = null;
            try {
                connection2 = getNewConnection();
                int otherServerId = getServerId(connection2);
                log.debug("connected to server " + otherServerId);
                Assert.assertTrue(otherServerId != firstServerId);
                Protocol protocol = getProtocolFromConnection(connection2);
                Assert.assertTrue(blacklist.keySet().toArray()[0].equals(protocol.getProxy().getListener()
                        .getBlacklist().keySet().toArray()[0]));

            } catch (Throwable e) {
                e.printStackTrace();
                Assert.fail();
            } finally {
                if (connection2 != null) {
                    try {
                        connection2.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    class MutableInt {
        int value = 1; // note that we start at 1 since we're counting

        public void increment() {
            ++value;
        }

        public int get() {
            return value;
        }
    }

}
