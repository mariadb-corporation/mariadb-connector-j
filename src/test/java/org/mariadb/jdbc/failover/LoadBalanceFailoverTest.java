package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Exemple mvn test  -DdefaultLoadbalanceUrl=jdbc:mysql:loadbalance//localhost:3306,localhost:3307/test?user=root.
 */
public class LoadBalanceFailoverTest extends BaseMultiHostTest {
    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() throws SQLException {
        proxyUrl = proxyLoadbalanceUrl;
        Assume.assumeTrue(initialLoadbalanceUrl != null);
    }

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @Before
    public void init() throws SQLException {
        defaultUrl = initialLoadbalanceUrl;
        currentType = HaMode.LOADBALANCE;
    }

    @Test(expected = SQLException.class)
    public void failover() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&autoReconnect=true&retriesAllDown=6", true);
            int master1ServerId = getServerId(connection);
            stopProxy(master1ServerId);
            connection.createStatement().execute("SELECT 1");
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }


    @Test
    public void randomConnection() throws Throwable {
        Assume.assumeTrue(initialLoadbalanceUrl.contains("loadbalance"));
        Map<String, MutableInt> connectionMap = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            Connection connection = getNewConnection(false);
            int serverId = getServerId(connection);
            MutableInt count = connectionMap.get(String.valueOf(serverId));
            if (count == null) {
                connectionMap.put(String.valueOf(serverId), new MutableInt());
            } else {
                count.increment();
            }
            connection.close();
        }

        Assert.assertTrue(connectionMap.size() >= 2);
        for (String key : connectionMap.keySet()) {
            Integer connectionCount = connectionMap.get(key).get();
            Assert.assertTrue(connectionCount > 1);
        }
    }


    @Test
    public void testReadonly() throws SQLException {
        Connection connection = null;
        try {
            connection = getNewConnection(false);
            connection.setReadOnly(true);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinode");
            stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
        } finally {
            if (connection != null) {
                connection.close();
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
