package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.common.UrlHAMode;
import org.mariadb.jdbc.internal.mysql.Protocol;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * test for galera
 * The node must be configure with specific names :
 * node 1 : wsrep_node_name = "galera1"
 * ...
 * node x : wsrep_node_name = "galerax"
 * exemple mvn test  -DdbUrl=jdbc:mysql://localhost:3306,localhost:3307/test?user=root
 */
public class GaleraFailoverTest extends SequentialFailoverTest {

    @Before
    @Override
    public void init() throws SQLException {
        currentType = UrlHAMode.FAILOVER;
        initialUrl = initialGaleraUrl;
        proxyUrl = proxyGaleraUrl;
        Assume.assumeTrue(initialGaleraUrl != null);
        connection = null;
    }

    @Test
    @Override
    public void connectionOrder() throws Throwable {
        Assume.assumeTrue(initialGaleraUrl.contains("failover"));
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

}
