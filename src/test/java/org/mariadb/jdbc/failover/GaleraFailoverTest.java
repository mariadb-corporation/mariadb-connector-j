package org.mariadb.jdbc.failover;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mariadb.jdbc.internal.common.HaMode;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * test for galera
 * The node must be configure with specific names :
 * node 1 : wsrep_node_name = "galera1"
 * ...
 * node x : wsrep_node_name = "galerax"
 * exemple mvn test  -DdbUrl=jdbc:mysql://localhost:3306,localhost:3307/test?user=root
 */
public class GaleraFailoverTest extends SequentialFailoverTest {
    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @Before
    @Override
    public void init() throws SQLException {
        currentType = HaMode.FAILOVER;
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
