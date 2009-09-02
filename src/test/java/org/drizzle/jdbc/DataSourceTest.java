package org.drizzle.jdbc;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Connection;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Sep 1, 2009
 * Time: 7:03:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class DataSourceTest {
    @Test
    public void testDrizzleDataSource() throws SQLException {
        DataSource ds = new DrizzleDataSource("localhost",4427,"test_units_jdbc");
        Connection connection = ds.getConnection();
        assertEquals(connection.isValid(0),true);
    }
    @Test
    public void testDrizzleDataSource2() throws SQLException {
        DataSource ds = new DrizzleDataSource("localhost",4427,"test_units_jdbc");
        Connection connection = ds.getConnection("","");
        assertEquals(connection.isValid(0),true);
    }
}
