package org.skysql.jdbc;


import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Ignore
public class BaseTest {
    Connection connection;
    @Before
    public  void before() throws SQLException{
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root");
    }
    @After
    public  void after() throws SQLException {
        try {
       connection.close();
        }
        catch(Exception e) {

        }
    }
}
