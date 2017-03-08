package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * test for galera
 * The node must be configure with specific names :
 * node 1 : wsrep_node_name = "galera1"
 * ...
 * node x : wsrep_node_name = "galerax"
 * exemple mvn test  -DdbUrl=jdbc:mariadb://localhost:3306,localhost:3307/test?user=root
 */
public class GaleraFailoverTest extends SequentialFailoverTest {
    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() throws SQLException {
        proxyUrl = proxyGaleraUrl;
        Assume.assumeTrue(initialGaleraUrl != null);
    }

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @Before
    public void init() throws SQLException {
        defaultUrl = initialGaleraUrl;
        currentType = HaMode.FAILOVER;
    }


    @Test
    @Override
    public void connectionOrder() throws Throwable {
        Assume.assumeTrue(initialGaleraUrl.contains("failover"));
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

}
