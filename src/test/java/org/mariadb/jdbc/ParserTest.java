package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.internal.util.DefaultOptions;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class ParserTest extends BaseTest {
    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("table1", "id1 int auto_increment primary key");
        createTable("table2", "id2 int auto_increment primary key");
    }

    @Test
    public void poolVerification() throws Exception {
        ArrayList<HostAddress> hostAddresses = new ArrayList<>();
        hostAddresses.add(new HostAddress(hostname, port));
        UrlParser urlParser = new UrlParser(database, hostAddresses, DefaultOptions.defaultValues(HaMode.NONE), HaMode.NONE);
        urlParser.setUsername("USER");
        urlParser.setPassword("PWD");
        urlParser.parseUrl("jdbc:mariadb://localhost:3306/db");
        assertEquals("USER", urlParser.getUsername());
        assertEquals("PWD", urlParser.getPassword());

        MariaDbDataSource datasource = new MariaDbDataSource();
        datasource.setUser("USER");
        datasource.setPassword("PWD");
        datasource.setUrl("jdbc:mariadb://localhost:3306/db");
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
            fail();
        }
    }
}
