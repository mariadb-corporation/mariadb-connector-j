package org.drizzle.jdbc;

import com.mysql.jdbc.*;
import org.junit.Test;

import java.sql.*;
import java.sql.Connection;
import java.sql.Statement;


public class LoadTest {
    @Test    
    public void tm() throws SQLException {
        Connection drizConnection = DriverManager.getConnection("jdbc:mysql:thin://10.100.100.50:4427/test_units_jdbc");
        Connection mysqlConnection = DriverManager.getConnection("jdbc:mysql://10.100.100.50:4427/test_units_jdbc");
              
        long sum = 0;
        int i;
        for(i  =0;i<10;i++)
          sum+=this.loadTest(mysqlConnection);
        System.out.println(sum/i);
        sum = 0;
        for(i = 0;i<10;i++)
          sum+=this.loadTest(drizConnection);
        System.out.println(sum/i);
    }

    public long loadTest(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists loadsofdata");
        stmt.executeUpdate("create table loadsofdata (id int not null primary key auto_increment, data varchar(250)) engine=innodb");
        stmt.close();
        long startTime=System.currentTimeMillis();
        for(int i=0;i<100;i++) {
            stmt=connection.createStatement();
            stmt.executeUpdate("insert into loadsofdata (data) values ('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"+i+"')");
            stmt.close();
        }
        for(int i=0;i<100;i++){
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

