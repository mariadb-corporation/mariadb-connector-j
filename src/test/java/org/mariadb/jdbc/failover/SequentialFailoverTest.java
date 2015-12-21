package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.threadly.test.concurrent.TestableScheduler;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for sequential connection
 * exemple mvn test  -DdefaultGaleraUrl=jdbc:mysql:sequential//localhost:3306,localhost:3307/test?user=root.
 */
public class SequentialFailoverTest extends BaseMultiHostTest {

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() throws SQLException {
        proxyUrl = proxySequentialUrl;
        Assume.assumeTrue(initialGaleraUrl != null);
    }

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @Before
    public void init() throws SQLException {
        defaultUrl = initialGaleraUrl;
        currentType = HaMode.SEQUENTIAL;
    }

    @Test
    public void connectionOrder() throws Throwable {

        Assume.assumeTrue(!initialGaleraUrl.contains("failover"));
        UrlParser urlParser = UrlParser.parse(initialGaleraUrl);
        for (int i = 0; i < urlParser.getHostAddresses().size(); i++) {
            Connection connection = getNewConnection(true);
            int serverNb = getServerId(connection);
            Assert.assertTrue(serverNb == i + 1);
            connection.close();
            stopProxy(serverNb);
        }
    }

    @Test
    public void checkStaticBlacklist() throws Throwable {
        Connection connection = null;
        try {
            assureProxy();
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
                Assert.assertTrue(protocol.getProxy().getListener().getBlacklist().size() == 1);

                //replace proxified HostAddress by normal one
                UrlParser urlParser = UrlParser.parse(defaultUrl);
                protocol.getProxy().getListener().getBlacklist().put(urlParser.getHostAddresses().get(firstServerId - 1),
                        System.nanoTime());
            } catch (Throwable e) {
                e.printStackTrace();
                Assert.fail();
            }

            //add first Host to blacklist
            Protocol protocol = getProtocolFromConnection(connection);
            TestableScheduler scheduler = new TestableScheduler();

            //check blacklist shared
            scheduler.execute(new CheckBlacklist(firstServerId, protocol.getProxy().getListener().getBlacklist()));
            scheduler.execute(new CheckBlacklist(firstServerId, protocol.getProxy().getListener().getBlacklist()));
            
            // deterministically execute CheckBlacklists
            scheduler.tick();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void testMultiHostWriteOnMaster() throws Throwable {
        Assume.assumeTrue(initialGaleraUrl != null);
        Connection connection = null;
        try {
            connection = getNewConnection();
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinode");
            stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void pingReconnectAfterRestart() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=1"
                    + "&queriesBeforeRetryMaster=50000", true);
            Statement st = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);

            try {
                st.execute("SELECT 1");
            } catch (SQLException e) {
                //eat exception
            }
            restartProxy(masterServerId);
            long restartTime = System.nanoTime();
            boolean loop = true;
            while (loop) {
                if (!connection.isClosed()) {
                    loop = false;
                }
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - restartTime);
                if (duration > 15 * 1000) {
                    Assert.fail();
                }
                Thread.sleep(250);
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
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
