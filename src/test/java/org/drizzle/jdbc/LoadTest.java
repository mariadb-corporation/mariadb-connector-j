package org.drizzle.jdbc;

import org.junit.Test;
//import org.apache.log4j.BasicConfigurator;

import java.sql.*;

import static junit.framework.Assert.assertEquals;

/**
 * User: marcuse
 * Date: Feb 10, 2009
 * Time: 9:53:59 PM
 */
public class LoadTest {
  //  static { BasicConfigurator.resetConfiguration();}
    @Test
    public void tm() throws SQLException {
      try {
                 Class.forName("org.drizzle.jdbc.Driver");
             } catch (ClassNotFoundException e) {
                 throw new SQLException("Could not load driver");
             }
       
          this.loadTest();
         // this.loadMysqlTest();
    }
    public void loadTest() throws SQLException {

        Connection connection = DriverManager.getConnection("jdbc:drizzle://localhost:3306/test_units_jdbc");
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists loadsofdata");
        stmt.executeUpdate("create table loadsofdata (id int not null primary key auto_increment, data varchar(100))");
        stmt.close();
        long startTime=System.currentTimeMillis();
        for(int i=0;i<1000;i++) {
            stmt=connection.createStatement();
            stmt.executeUpdate("insert into loadsofdata (data) values ('x"+i+"')");
            stmt.close();
        }
        System.out.println("--------------- "+(System.currentTimeMillis()-startTime)+" milli seconds");
        startTime=System.currentTimeMillis();
        for(int i=0;i<1000;i++){
            stmt=connection.createStatement();

            ResultSet rs = stmt.executeQuery("select * from loadsofdata ");
            while(rs.next()) {
                rs.getInt("id");
                rs.getString("data");
                rs.getInt(1);
                rs.getString(2);
            }
            rs.close();
            stmt.close();
        }
        System.out.println("--------------- "+(System.currentTimeMillis()-startTime)+" milli seconds");
    }
    public void loadMysqlTest() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("Could not load driver");
        }
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost/test_units_jdbc","root","");

        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists loadsofdata");
        stmt.executeUpdate("create table loadsofdata (id int not null primary key auto_increment, data varchar(100)) engine=innodb");
        stmt.close();
        long startTime = System.currentTimeMillis();
        for(int i=0;i<1000;i++) {
            stmt=connection.createStatement();
            stmt.executeUpdate("insert into loadsofdata (data) values ('x"+i+"')");
            stmt.close();
        }
        System.out.println("--------------- "+(System.currentTimeMillis()-startTime)+" milli seconds");
        startTime=System.currentTimeMillis();
        for(int i=0;i<1000;i++) {
            stmt=connection.createStatement();

            ResultSet rs = stmt.executeQuery("select * from loadsofdata");
            while(rs.next()) {
                rs.getInt("id");
                rs.getString("data");
                rs.getInt(1);
                rs.getString(2);
            }
            rs.close();
            stmt.close();
        }
        System.out.println("--------------- "+(System.currentTimeMillis()-startTime)+" milli seconds");
    }
}
