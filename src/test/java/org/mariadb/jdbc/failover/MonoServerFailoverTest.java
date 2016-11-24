package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;

public class MonoServerFailoverTest extends BaseMonoServer {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() throws SQLException {
        Assume.assumeTrue(initialUrl != null);
    }

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @Before
    public void init() throws SQLException {
        Assume.assumeTrue(initialUrl != null);
        defaultUrl = initialUrl;
        currentType = HaMode.NONE;
    }

    @Test
    public void checkClosedConnectionAfterFailover() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6", true);

            Statement st = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);
            try {
                st.execute("SELECT 1");
                Assert.fail();
            } catch (SQLException e) {
                //normal exception
            }
            Assert.assertTrue(st.isClosed());
            restartProxy(masterServerId);
            try {
                st = connection.createStatement();
                st.execute("SELECT 1");
            } catch (SQLException e) {
                Assert.fail();
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

    }

    @Test
    public void checkErrorAfterDeconnection() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6", true);

            Statement st = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);
            try {
                st.execute("SELECT 1");
                Assert.fail();
            } catch (SQLException e) {
                //normal exception
            }

            restartProxy(masterServerId);
            try {
                st.execute("SELECT 1");
                Assert.fail();
            } catch (SQLException e) {
                //statement must be closed -> error
            }
            Assert.assertTrue(connection.isClosed());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }


    @Test
    public void checkAutoReconnectDeconnection() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6", true);

            Statement st = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);
            try {
                st.execute("SELECT 1");
                Assert.fail();
            } catch (SQLException e) {
                //normal exception
            }

            restartProxy(masterServerId);
            try {
                //with autoreconnect -> not closed
                st = connection.createStatement();
                st.execute("SELECT 1");
            } catch (SQLException e) {
                Assert.fail();
            }
            Assert.assertFalse(connection.isClosed());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

    }

    /**
     * CONJ-120 Fix Connection.isValid method
     *
     * @throws Exception exception
     */
    @Test
    public void isValidConnectionThatIsKilledExternally() throws Throwable {
        Connection killerConnection = null;
        Connection connection = null;
        try {
            connection = getNewConnection();
            connection.setCatalog("mysql");
            Protocol protocol = getProtocolFromConnection(connection);
            killerConnection = getNewConnection();
            Statement killerStatement = killerConnection.createStatement();
            long threadId = protocol.getServerThreadId();
            killerStatement.execute("KILL CONNECTION " + threadId);
            boolean isValid = connection.isValid(0);
            assertFalse(isValid);
        } finally {
            if (killerConnection != null) {
                killerConnection.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void checkPrepareStatement() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6", true);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists failt1");
            stmt.execute("create table failt1 (id int not null primary key auto_increment, tt int)");


            PreparedStatement preparedStatement = connection.prepareStatement("insert into failt1(id, tt) values (?,?)");

            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);

            preparedStatement.setInt(1, 1);
            preparedStatement.setInt(2, 1);
            preparedStatement.addBatch();
            try {
                preparedStatement.executeBatch();
                Assert.fail();
            } catch (SQLException e) {
                //normal exception
            }
            restartProxy(masterServerId);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

}
