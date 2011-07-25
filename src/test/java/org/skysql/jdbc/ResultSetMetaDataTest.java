package org.skysql.jdbc;

import static junit.framework.Assert.assertEquals;

import java.sql.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.junit.Test;

/**
 * User: marcuse
 * Date: Feb 26, 2009
 * Time: 10:12:52 PM
 */
public class ResultSetMetaDataTest {
    static { Logger.getLogger("").setLevel(Level.OFF); }
    @Test
    public void metaDataTest() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:drizzle://root@"+DriverTest.host+":3307/test_units_jdbc");
        Statement stmt = connection.createStatement();
        stmt.execute("drop table if exists test_rsmd");  
        stmt.execute("create table test_rsmd (id_col int not null primary key auto_increment, " +
                                            "nullable_col varchar(20)," +
                                            "unikey_col int unique)");
        stmt.execute("insert into test_rsmd (id_col,nullable_col,unikey_col) values (null, 'hej', 9)");
        ResultSet rs = stmt.executeQuery("select id_col, nullable_col, unikey_col as something from test_rsmd");
        assertEquals(true,rs.next());
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals(true,rsmd.isAutoIncrement(1));
        assertEquals(3,rsmd.getColumnCount());
        assertEquals(ResultSetMetaData.columnNullable,rsmd.isNullable(2));
        assertEquals(ResultSetMetaData.columnNoNulls,rsmd.isNullable(1));
        assertEquals(String.class.getName(), rsmd.getColumnClassName(2));
        assertEquals(Long.class.getName(), rsmd.getColumnClassName(1));
        assertEquals(Long.class.getName(), rsmd.getColumnClassName(3));
        assertEquals("id_col",rsmd.getColumnLabel(1));
        assertEquals("nullable_col",rsmd.getColumnLabel(2));
        assertEquals("something",rsmd.getColumnLabel(3));
        assertEquals("unikey_col",rsmd.getColumnName(3));
    }
}
