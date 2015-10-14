package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.internal.common.UrlHAMode;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * exemple mvn test  -DdefaultLoadbalanceUrl=jdbc:mysql:loadbalance//localhost:3306,localhost:3307/test?user=root
 */
public class LoadBalanceFailoverTest extends BaseMultiHostTest {
    private Connection connection;

    @Before
    public void init() throws SQLException {
        currentType = UrlHAMode.LOADBALANCE;
        initialUrl = initialLoadbalanceUrl;
        proxyUrl = proxyLoadbalanceUrl;
        Assume.assumeTrue(initialLoadbalanceUrl != null);
        connection = null;
    }

    @After
    public void after() throws SQLException {
        assureProxy();
        assureBlackList(connection);
        if (connection != null) connection.close();
    }


    @Test(expected = SQLException.class)
    public void failover() throws Throwable {
        connection = getNewConnection("&autoReconnect=true&retriesAllDown=1", true);
        int master1ServerId = getServerId(connection);
        stopProxy(master1ServerId);
        connection.createStatement().execute("SELECT 1");
    }


    @Test
    public void randomConnection() throws Throwable {
        Assume.assumeTrue(initialLoadbalanceUrl.contains("loadbalance"));
        Map<String, MutableInt> connectionMap = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            connection = getNewConnection(false);
            int serverId = getServerId(connection);
            log.debug("master server found " + serverId);
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
            log.debug(" ++++ Server " + key + " : " + connectionCount + " connections ");
            Assert.assertTrue(connectionCount > 1);
        }
        log.debug("randomConnection OK");
    }


    @Test
    public void testReadonly() throws SQLException {
        connection = getNewConnection(false);
        connection.setReadOnly(true);
        Statement stmt = connection.createStatement();
        stmt.execute("drop table  if exists multinode");
        stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
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
