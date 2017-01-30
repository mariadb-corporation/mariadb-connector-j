package org.mariadb.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class ReconnectionStateMaxAllowedStatement extends BaseTest {

    @Test
    public void isolationLevelResets() throws SQLException {
        try (Connection connection = setConnection()) {
            long max = maxPacket(connection);
            if (max > Integer.MAX_VALUE - 10) {
                fail("max_allowed_packet too high for this test");
            }
            connection.prepareStatement("create table if not exists foo (x longblob)").execute();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

            assertEquals("READ-UNCOMMITTED", level(connection));
            try (PreparedStatement st = connection.prepareStatement("insert into foo (?)")) {
                st.setBytes(1, data((int) (max + 10)));
                st.execute();
                fail();
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("max_allowed_packet"));
                // we still have a working connection
                assertTrue(connection.isValid(0));
                // our isolation level must have stay the same
                assertEquals("READ-UNCOMMITTED", level(connection));
            }
        }
    }

    private String level(Connection connection) throws SQLException {
        try (ResultSet rs = connection.prepareStatement("select @@tx_isolation").executeQuery()) {
            rs.next();
            return rs.getString(1);
        }
    }

    private long maxPacket(Connection connection) throws SQLException {
        try (ResultSet rs = connection.prepareStatement("select @@max_allowed_packet").executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private byte[] data(int size) {
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) 'a');
        return data;
    }
}
