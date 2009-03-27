package org.drizzle.jdbc;

import org.junit.Test;
import org.apache.log4j.BasicConfigurator;

import java.sql.*;

import static junit.framework.Assert.assertEquals;

/**
 * User: marcuse
 * Date: Feb 10, 2009
 * Time: 9:53:59 PM
 */
public class LoadTest {
    static { BasicConfigurator.configure(); }
   
    @Test
    public void loadTest() throws SQLException {
        try {
            Class.forName("org.drizzle.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("Could not load driver");
        }
        Connection connection = DriverManager.getConnection("jdbc:drizzle://"+DriverTest.host+":4427/test_units_jdbc");
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists loadsofdata");
        stmt.executeUpdate("create table loadsofdata (id int not null primary key auto_increment, data varchar(100))");
        stmt.close();
        for(int i=0;i<100;i++) {
            stmt=connection.createStatement();
            stmt.executeUpdate("insert into loadsofdata (data) values ('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"+i+"')");
            stmt.close();
        }

        for(int i=0;i<100;i++){
            stmt=connection.createStatement();
            stmt.executeQuery("select * from loadsofdata");
            stmt.close();
        }
    }
}
