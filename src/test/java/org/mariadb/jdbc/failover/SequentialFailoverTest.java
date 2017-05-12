package org.mariadb.jdbc.failover;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.threadly.test.concurrent.TestableScheduler;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for sequential connection
 * exemple mvn test  -DdefaultGaleraUrl=jdbc:mariadb:sequential//localhost:3306,localhost:3307/test?user=root.
 */
public class SequentialFailoverTest extends BaseMultiHostTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() throws SQLException {
        proxyUrl = proxySequentialUrl;
        Assume.assumeTrue(initialGaleraUrl != null);
    }

    /**
     * Initialisation.
     *
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
            int serverNb;
            try (Connection connection = getNewConnection(true)) {
                serverNb = getServerId(connection);
                assertTrue(serverNb == i + 1);
            }
            stopProxy(serverNb);
        }
    }

    @Test
    public void checkStaticBlacklist() throws Throwable {
        assureProxy();
        try (Connection connection = getNewConnection("&loadBalanceBlacklistTimeout=500", true)) {
            Statement st = connection.createStatement();

            int firstServerId = getServerId(connection);
            stopProxy(firstServerId);

            try {
                st.execute("SELECT 1");
                fail();
            } catch (SQLException e) {
                //normal exception that permit to blacklist the failing connection.
            }

            //check blacklist size
            try {
                Protocol protocol = getProtocolFromConnection(connection);
                assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 1);

                //replace proxified HostAddress by normal one
                UrlParser urlParser = UrlParser.parse(defaultUrl);
                protocol.getProxy().getListener().addToBlacklist(urlParser.getHostAddresses().get(firstServerId - 1));
            } catch (Throwable e) {
                e.printStackTrace();
                fail();
            }

            //add first Host to blacklist
            Protocol protocol = getProtocolFromConnection(connection);
            TestableScheduler scheduler = new TestableScheduler();

            //check blacklist shared
            scheduler.execute(new CheckBlacklist(firstServerId, protocol.getProxy().getListener().getBlacklistKeys()));
            scheduler.execute(new CheckBlacklist(firstServerId, protocol.getProxy().getListener().getBlacklistKeys()));

            // deterministically execute CheckBlacklists
            scheduler.tick();
        }
    }

    @Test
    public void testMultiHostWriteOnMaster() throws Throwable {
        Assume.assumeTrue(initialGaleraUrl != null);
        try (Connection connection = getNewConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinode");
            stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
        } catch (SQLException sqle) {
            fail("must have worked");
        }
    }

    @Test
    public void pingReconnectAfterRestart() throws Throwable {
        try (Connection connection = getNewConnection("&retriesAllDown=6", true)) {
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
                    fail();
                }
                Thread.sleep(250);
            }
        }
    }

    protected class CheckBlacklist implements Runnable {
        private int firstServerId;
        private Set<HostAddress> blacklistKeys;

        public CheckBlacklist(int firstServerId, Set<HostAddress> blacklistKeys) {
            this.firstServerId = firstServerId;
            this.blacklistKeys = blacklistKeys;
        }

        public void run() {
            try (Connection connection2 = getNewConnection()) {
                int otherServerId = getServerId(connection2);
                assertTrue(otherServerId != firstServerId);
                Protocol protocol = getProtocolFromConnection(connection2);
                assertTrue(blacklistKeys.toArray()[0].equals(protocol.getProxy().getListener()
                        .getBlacklistKeys().toArray()[0]));

            } catch (Throwable e) {
                e.printStackTrace();
                fail();
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
