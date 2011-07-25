package org.skysql.jdbc;

import org.skysql.jdbc.internal.common.Utils;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class MultiTest {
    private Connection connection;

    public MultiTest() throws SQLException {
        connection = DriverManager.getConnection("jdbc:mysql:thin://"+DriverTest.host+":3306/test_units_jdbc?allowMultiQueries=true");
    }

    @Test
    public void basicTest() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from t1;select * from t2;");
        int count = 0;
        while(rs.next()) {
            count++;
        }
        assertTrue(count > 0);
        assertTrue(statement.getMoreResults());
        rs = statement.getResultSet();
        count=0;
        while(rs.next()) {
            count++;
        }
        assertTrue(count>0);
        assertFalse(statement.getMoreResults());



    }

    @Test
    public void updateTest() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("update t1 set test='a "+System.currentTimeMillis()+"' where id = 2;select * from t2;");
        assertTrue(statement.getUpdateCount() == 1);
        assertTrue(statement.getMoreResults());
        ResultSet rs = statement.getResultSet();
        int count = 0;
        while(rs.next()) {
            count++;
        }
        assertTrue(count>0);
        assertFalse(statement.getMoreResults());
    }
    @Test
    public void updateTest2() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("select * from t2;update t1 set test='a "+System.currentTimeMillis()+"' where id = 2;");
        ResultSet rs = statement.getResultSet();
        int count = 0;
        while(rs.next()) { count++;}
        assertTrue(count>0);
        statement.getMoreResults();
        
        assertTrue(statement.getUpdateCount() == 1);
    }

    @Test
    public void selectTest() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("select * from t2;select * from t1;");
        ResultSet rs = statement.getResultSet();
        int count = 0;
        while(rs.next()) { count++;}
        assertTrue(count>0);
        rs = statement.executeQuery("select * from t1");
        count = 0;
        while(rs.next()) { count++;}
        assertTrue(count>0);
    }


}
