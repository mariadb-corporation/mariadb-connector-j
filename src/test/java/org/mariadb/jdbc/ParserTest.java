package org.mariadb.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.internal.common.Options;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.*;

public class ParserTest extends BaseTest {

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("table1", "id1 int auto_increment primary key");
        createTable("table2", "id2 int auto_increment primary key");
    }


    @Test
    public void addProperties() throws Exception {
        Connection connection = null;
        try {
            connection = setConnection();
            Field field = MySQLConnection.class.getDeclaredField("options");
            field.setAccessible(true);
            Options options = (Options) field.get(connection);
            assertFalse(options.useSSL);
            connection.setClientInfo("useSSL", "true");

            options = (Options) field.get(connection);
            assertTrue(options.useSSL);

            Properties prop = new Properties();
            prop.put("autoReconnect", "true");
            prop.put("useSSL", "false");
            connection.setClientInfo(prop);
            assertFalse(options.useSSL);
            assertTrue(options.autoReconnect);
        } finally {
            connection.close();
        }
    }

    @Test
    public void libreOfficeBase() {
        String sql;
        try {
            Statement statement = sharedConnection.createStatement();
            sql = "INSERT INTO table1 VALUES (1),(2),(3),(4),(5),(6)";
            statement.execute(sql);
            sql = "INSERT INTO table2 VALUES (1),(2),(3),(4),(5),(6)";
            statement.execute(sql);
            // uppercase OJ
            sql = "SELECT table1.id1, table2.id2 FROM { OJ table1 LEFT OUTER JOIN table2 ON table1.id1 = table2.id2 }";
            ResultSet rs = statement.executeQuery(sql);
            for (int count = 1; count <= 6; count++) {
                assertTrue(rs.next());
                assertEquals(count, rs.getInt(1));
                assertEquals(count, rs.getInt(2));
            }
            // mixed oJ
            sql = "SELECT table1.id1, table2.id2 FROM { oJ table1 LEFT OUTER JOIN table2 ON table1.id1 = table2.id2 }";
            rs = statement.executeQuery(sql);
            for (int count = 1; count <= 6; count++) {
                assertTrue(rs.next());
                assertEquals(count, rs.getInt(1));
                assertEquals(count, rs.getInt(2));
            }
        } catch (SQLException e) {
            assertTrue(false);
        }
    }
}
