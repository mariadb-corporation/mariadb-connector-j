package org.mariadb.jdbc;


import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class CheckDataTest extends BaseTest {

    @Test
    public void testStatementExecuteAutoincrement() throws SQLException {
        createTable("CheckDataTest1", "id int not null primary key auto_increment, test varchar(10)");
        Statement stmt = sharedConnection.createStatement();
        int insert = stmt.executeUpdate("INSERT INTO CheckDataTest1 (test) VALUES ('test1')", Statement.RETURN_GENERATED_KEYS);
        Assert.assertEquals(1, insert);

        ResultSet rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        rs = stmt.executeQuery("SELECT * FROM CheckDataTest1");
        assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals("test1", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testStatementBatch() throws SQLException {
        createTable("CheckDataTest2", "id int not null primary key auto_increment, test varchar(10)");
        Statement stmt = sharedConnection.createStatement();
        stmt.addBatch("INSERT INTO CheckDataTest2 (id, test) VALUES (5, 'test1')");
        stmt.addBatch("INSERT INTO CheckDataTest2 (test) VALUES ('test2')");
        stmt.addBatch("UPDATE CheckDataTest2 set test = CONCAT(test, 'tt')");
        stmt.addBatch("INSERT INTO CheckDataTest2 (id, test) VALUES (9, 'test3')");
        int[] res = stmt.executeBatch();

        Assert.assertEquals(4, res.length);
        Assert.assertEquals(1, res[0]);
        Assert.assertEquals(1, res[1]);
        Assert.assertEquals(2, res[2]);
        Assert.assertEquals(1, res[3]);

        ResultSet rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        Assert.assertEquals(5, rs.getInt(1));
        assertTrue(rs.next());
        Assert.assertEquals(6, rs.getInt(1));
        assertTrue(rs.next());
        Assert.assertEquals(9, rs.getInt(1));
        assertFalse(rs.next());

        rs = stmt.executeQuery("SELECT * FROM CheckDataTest2");
        assertTrue(rs.next());
        Assert.assertEquals(5, rs.getInt(1));
        Assert.assertEquals("test1tt", rs.getString(2));
        assertTrue(rs.next());
        Assert.assertEquals(6, rs.getInt(1));
        Assert.assertEquals("test2tt", rs.getString(2));
        assertTrue(rs.next());
        Assert.assertEquals(9, rs.getInt(1));
        Assert.assertEquals("test3", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testPrepareStatementExecuteAutoincrement() throws SQLException {
        createTable("CheckDataTest3", "id int not null primary key auto_increment, test varchar(10)");
        PreparedStatement stmt = sharedConnection.prepareStatement("INSERT INTO CheckDataTest3 (test) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS);
        stmt.setString(1, "test1");
        int insert = stmt.executeUpdate();
        Assert.assertEquals(1, insert);

        ResultSet rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        //without addBatch -> no execution
        int[] noBatch = stmt.executeBatch();
        Assert.assertEquals(0, noBatch.length);

        //with addBatch
        stmt.addBatch();
        int[] nbBatch = stmt.executeBatch();
        Assert.assertEquals(1, nbBatch.length);
        if (sharedIsRewrite()) {
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, nbBatch[0]);
        } else {
            Assert.assertEquals(1, nbBatch[0]);
        }

        rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        rs = stmt.executeQuery("SELECT * FROM CheckDataTest3");
        assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals("test1", rs.getString(2));
        assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
        Assert.assertEquals("test1", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testPrepareStatementBatch() throws SQLException {
        createTable("CheckDataTest4", "id int not null primary key auto_increment, test varchar(10)");
        PreparedStatement stmt = sharedConnection.prepareStatement("INSERT INTO CheckDataTest4 (test) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS);
        stmt.setString(1, "test1");
        stmt.addBatch();
        stmt.setString(1, "test2");
        stmt.addBatch();
        stmt.addBatch();

        int[] res = stmt.executeBatch();

        Assert.assertEquals(3, res.length);
        if (sharedIsRewrite()) {
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, res[0]);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, res[1]);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, res[2]);
        } else {
            Assert.assertEquals(1, res[0]);
            Assert.assertEquals(1, res[1]);
            Assert.assertEquals(1, res[2]);
        }

        ResultSet rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        Assert.assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        rs = stmt.executeQuery("SELECT * FROM CheckDataTest4");
        assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals("test1", rs.getString(2));
        assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
        Assert.assertEquals("test2", rs.getString(2));
        assertTrue(rs.next());
        Assert.assertEquals(3, rs.getInt(1));
        Assert.assertEquals("test2", rs.getString(2));
        assertFalse(rs.next());
    }
}
