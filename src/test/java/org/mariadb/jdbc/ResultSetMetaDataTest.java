package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;

public class ResultSetMetaDataTest extends BaseTest {

    @Test
    public void metaDataTest() throws SQLException {
        requireMinimumVersion(5, 0);
        Statement stmt = connection.createStatement();
        stmt.execute("drop table if exists test_rsmd");
        stmt.execute("create table test_rsmd (id_col int not null primary key auto_increment, "
                + "nullable_col varchar(20),unikey_col int unique, char_col char(10), us  smallint unsigned)");
        stmt.execute("insert into test_rsmd (id_col,nullable_col,unikey_col) values (null, 'hej', 9)");
        ResultSet rs = stmt
                .executeQuery("select id_col, nullable_col, unikey_col as something, char_col,us from test_rsmd");
        assertEquals(true, rs.next());
        ResultSetMetaData rsmd = rs.getMetaData();

        assertEquals(true, rsmd.isAutoIncrement(1));
        assertEquals(5, rsmd.getColumnCount());
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(2));
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));
        assertEquals(String.class.getName(), rsmd.getColumnClassName(2));
        assertEquals(Integer.class.getName(), rsmd.getColumnClassName(1));
        assertEquals(Integer.class.getName(), rsmd.getColumnClassName(3));
        assertEquals("id_col", rsmd.getColumnLabel(1));
        assertEquals("nullable_col", rsmd.getColumnLabel(2));
        assertEquals("something", rsmd.getColumnLabel(3));
        assertEquals("unikey_col", rsmd.getColumnName(3));
        assertEquals(Types.CHAR, rsmd.getColumnType(4));
        assertEquals(Types.SMALLINT, rsmd.getColumnType(5));

        DatabaseMetaData md = connection.getMetaData();
        ResultSet cols = md.getColumns(null, null, "test\\_rsmd", null);
        cols.next();
        assertEquals("id_col", cols.getString("COLUMN_NAME"));
        assertEquals(Types.INTEGER, cols.getInt("DATA_TYPE"));
        cols.next(); /* nullable_col */
        cols.next(); /* unikey_col */
        cols.next(); /* char_col */
        assertEquals("char_col", cols.getString("COLUMN_NAME"));
        assertEquals(Types.CHAR, cols.getInt("DATA_TYPE"));
        cols.next(); /* us */// CONJ-96: SMALLINT UNSIGNED gives Types.SMALLINT
        assertEquals("us", cols.getString("COLUMN_NAME"));
        assertEquals(Types.SMALLINT, cols.getInt("DATA_TYPE"));
    }

    @Test
    public void conj17() throws Exception {
        requireMinimumVersion(5, 0);
        ResultSet rs = connection.createStatement().executeQuery(
                "select count(*),1 from information_schema.tables");
        rs.next();
        assertEquals(rs.getMetaData().getColumnName(1), "count(*)");
        assertEquals(rs.getMetaData().getColumnName(2), "1");
    }

    @Test
    public void conj84() throws Exception {
        requireMinimumVersion(5, 0);
        Statement stmt = connection.createStatement();

        stmt.execute("DROP TABLE IF EXISTS t1");
        stmt.execute("DROP TABLE IF EXISTS t2");
        stmt.execute("CREATE TABLE t1 (id int, name varchar(20))");
        stmt.execute("CREATE TABLE t2 (id int, name varchar(20))");
        stmt.execute("INSERT INTO t1 VALUES (1, 'foo')");
        stmt.execute("INSERT INTO t2 VALUES (2, 'bar')");
        ResultSet rs = connection.createStatement().executeQuery(
                "select t1.*, t2.* FROM t1 join t2");
        rs.next();
        assertEquals(rs.findColumn("id"), 1);
        assertEquals(rs.findColumn("name"), 2);
        assertEquals(rs.findColumn("t1.id"), 1);
        assertEquals(rs.findColumn("t1.name"), 2);
        assertEquals(rs.findColumn("t2.id"), 3);
        assertEquals(rs.findColumn("t2.name"), 4);
    }

    /*
     * CONJ-149: ResultSetMetaData.getTableName returns table alias instead of real table name
     *
     * @throws SQLException
     */
    @Test
    public void tableNameTest() throws Exception {

        Statement stmt = connection.createStatement();

        stmt.execute("DROP TABLE IF EXISTS t1");
        stmt.execute("CREATE TABLE t1 (id int, name varchar(20))");

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT id AS id_alias FROM t1 AS t1_alias");
        ResultSetMetaData rsmd = rs.getMetaData();

        // this should return the original name of the table, not the alias
        logInfo(rsmd.getTableName(1));
        Assert.assertEquals(rsmd.getTableName(1), "t1");

        Assert.assertEquals(rsmd.getColumnLabel(1), "id_alias");
        Assert.assertEquals(rsmd.getColumnName(1), "id");

        // add useOldAliasMetadataBehavior to get the alias instead of the real
        // table name
        setConnection("&useOldAliasMetadataBehavior=true");

        rs = connection.createStatement().executeQuery(
                "SELECT id AS id_alias FROM t1 AS t1_alias");
        rsmd = rs.getMetaData();

        // this should return the alias name of the table, i.e. old behavior
        logInfo(rsmd.getTableName(1));
        Assert.assertEquals(rsmd.getTableName(1), "t1_alias");
        Assert.assertEquals(rsmd.getColumnLabel(1), "id_alias");
        Assert.assertEquals(rsmd.getColumnName(1), "id_alias");

    }

}
