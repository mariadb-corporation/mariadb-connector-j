package org.drizzle.jdbc;

import org.junit.Test;
//import org.apache.log4j.BasicConfigurator;

import java.sql.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import static junit.framework.Assert.assertEquals;

/**
 * User: marcuse
 * Date: Feb 10, 2009
 * Time: 9:53:59 PM
 */
public class LoadTest {
static { Logger.getLogger("").setLevel(Level.OFF); }
    @Test
    public void tm() throws SQLException {
      try {
                 Class.forName("org.drizzle.jdbc.Driver");
             } catch (ClassNotFoundException e) {
                 throw new SQLException("Could not load driver");
             }
      Connection drizConnection = DriverManager.getConnection("jdbc:drizzle://localhost:4427/test_units_jdbc");
      //Connection mysqlConnection = DriverManager.getConnection("jdbc:mysql:thin://localhost/test_units_jdbc","","");

      long sum = 0;
      int i;
      for(i =0;i<1;i++)
          sum+=this.loadTest(drizConnection);
/*      System.out.println("AVG: "+(sum/i));
      sum=0;
      for(i =0;i<10;i++)
          sum+=this.loadTest(mysqlConnection);
      System.out.println("MyAVG: "+(sum/i));*/
    }
    public long loadTest(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists loadsofdata");
        stmt.executeUpdate("create table loadsofdata (id int not null primary key auto_increment, data varchar(250)) engine=innodb");
        stmt.close();
        long startTime=System.currentTimeMillis();
        for(int i=0;i<1000;i++) {
            stmt=connection.createStatement();
            stmt.executeUpdate("insert into loadsofdata (data) values ('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"+i+"')");
            stmt.close();
        }
       // System.out.println(System.currentTimeMillis()-startTime));
       // startTime=System.currentTimeMillis();
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
        return System.currentTimeMillis()-startTime;
    }
}
