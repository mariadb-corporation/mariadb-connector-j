package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.Test;

public class MySQLCompatibilityTest extends BaseTest {

    /**
     * CONJ-82: data type LONGVARCHAR not supported in setObject()
     * @throws SQLException
     */
    @Test
    public void datatypesTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("DROP TABLE IF EXISTS datatypesTest");
        stmt.execute("CREATE TABLE datatypesTest (type_longvarchar TEXT NULL)");
        PreparedStatement preparedStmt = connection.prepareStatement("INSERT INTO `datatypesTest` (`type_longvarchar`) VALUES ( ? )");
        preparedStmt.setObject(1, "longvarcharTest" , Types.LONGVARCHAR);
        preparedStmt.executeUpdate();
        preparedStmt.close();
        ResultSet rs = stmt.executeQuery("SELECT * FROM datatypesTest");
        stmt.close();
        rs.next();
        assertEquals("longvarcharTest", rs.getString(1));
    }

    /**
     * The Mysql connector returns "0" or "1" for BIT(1) with ResultSet.getString().
     * CONJ-102: mariadb-java-client returned "false" or "true".
     * @throws SQLException
     */
    @Test
    public void testBitConj102() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop table  if exists mysqlcompatibilitytest");
        stmt.execute("create table mysqlcompatibilitytest (id int not null primary key auto_increment, test bit(1))");
        stmt.execute("insert into mysqlcompatibilitytest values(null, b'0')");
        stmt.execute("insert into mysqlcompatibilitytest values(null, b'1')");
        ResultSet rs = stmt.executeQuery("select * from mysqlcompatibilitytest");
        assertTrue(rs.next());
        assertTrue("0".equalsIgnoreCase(rs.getString(2)));
        assertTrue(rs.next());
        assertTrue("1".equalsIgnoreCase(rs.getString(2)));
    }

}
