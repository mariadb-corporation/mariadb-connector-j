package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

import org.junit.Test;

public class MySQLCompatibilityTest extends BaseTest {

	@Test
	public void datatypesTest() throws Exception {
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
	
}
