package org.drizzle.jdbc;

import org.junit.Test;

import java.sql.*;
import java.sql.Connection;
import java.sql.Statement;


public class LoadTest {
    @Test    
    public void tm() throws SQLException {
        Connection drizConnection = DriverManager.getConnection("jdbc:mysql:thin://10.100.100.50:3306/test_units_jdbc");
        Connection mysqlConnection = DriverManager.getConnection("jdbc:mysql://10.100.100.50:3306/test_units_jdbc");
              
        long sum = 0;
        int i;
        for(i=0;i<10;i++)
          sum+=this.loadTest(drizConnection);
        System.out.println(sum/i);
        sum = 0;
        for(i = 0;i<10;i++)
          sum+=this.loadTest(mysqlConnection);
        System.out.println(sum/i);
    }

    public long loadTest(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists loadsofdata");
        stmt.executeUpdate("create table loadsofdata (id int not null primary key auto_increment, data varchar(250)) engine=myisam");
        stmt.close();
        long startTime=System.currentTimeMillis();
        for(int i=0;i<1000;i++) {
            stmt=connection.createStatement();
            stmt.executeUpdate("insert into loadsofdata (data) values ('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"+i+"')");
            stmt.close();
        }
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
    @Test
    public void prepareTest() throws SQLException {
        Connection drizConnection = DriverManager.getConnection("jdbc:mysql:thin://10.100.100.50:3306/test_units_jdbc");
        Connection mysqlConnection = DriverManager.getConnection("jdbc:mysql://10.100.100.50:3306/test_units_jdbc");
        Statement stmt = drizConnection.createStatement();
        stmt.executeUpdate("drop table if exists loadsofdata2");
        stmt.executeUpdate("create table loadsofdata2 (id int not null primary key auto_increment, data blob) engine=innodb");
        byte [] theBytes = new byte[500];
        for(int i=0;i<500; i++) {
            theBytes[i]= (byte) i;
        }
        long startTime=System.currentTimeMillis();

        for(int i=1; i<10000; i++) {
            PreparedStatement ps = drizConnection.prepareStatement("insert into loadsofdata2 (id,data) values (?,?)");
            ps.setInt(1,i);
            ps.setBytes(2,theBytes);
            ps.execute();
            ps.close();
        }
        System.out.println(System.currentTimeMillis() - startTime);

        stmt.executeUpdate("drop table if exists loadsofdata2");
        stmt.executeUpdate("create table loadsofdata2 (id int not null primary key auto_increment, data blob) engine=innodb");

        startTime=System.currentTimeMillis();
        for(int i=1; i<10000; i++) {
            PreparedStatement ps = mysqlConnection.prepareStatement("insert into loadsofdata2 (id,data) values (?,?)");
            ps.setInt(1,i);
            ps.setBytes(2,theBytes);
            ps.execute();
            ps.close();
        }
        System.out.println(System.currentTimeMillis() - startTime);
        startTime=System.currentTimeMillis();

        for(int i=1; i<10000; i++) {
            PreparedStatement ps = drizConnection.prepareStatement("select * from loadsofdata2 where id = ?");
            ps.setInt(1,i);
            ResultSet rs = ps.executeQuery();
            rs.next();
            rs.getBytes(2);
            ps.close();
        }
        System.out.println(System.currentTimeMillis() - startTime);

        startTime=System.currentTimeMillis();

        for(int i=1; i<10000; i++) {
            PreparedStatement ps = mysqlConnection.prepareStatement("select * from loadsofdata2 where id = ?");
            ps.setInt(1,i);
            ResultSet rs = ps.executeQuery();
            rs.next();
            rs.getBytes(2);
            ps.close();

        }

        System.out.println(System.currentTimeMillis() - startTime);

    }

    @Test
    public void prepareManyParamsTest() throws SQLException {
        Connection drizConnection = DriverManager.getConnection("jdbc:mysql:thin://10.100.100.50:3306/test_units_jdbc");
        Connection mysqlConnection = DriverManager.getConnection("jdbc:mysql://10.100.100.50:3306/test_units_jdbc");
        Statement stmt = drizConnection.createStatement();
        stmt.executeUpdate("drop table if exists loadsofdata3");
        StringBuilder sb = new StringBuilder("d0 int");
        for(int i=1; i<500; i++) {

            sb.append(", d").append(i).append(" int");
        }
        stmt.executeUpdate("create table loadsofdata3 (id int not null primary key auto_increment,"+sb.toString()+") engine=innodb");
        StringBuilder qb = new StringBuilder("?");
        for(int i=1; i<500; i++) {
            qb.append(", ?");
        }

        String query = "insert into loadsofdata3 values(?, "+qb.toString()+")";
        long startTime=System.currentTimeMillis();
        for(int i=0; i < 10000; i++) {
            PreparedStatement ps = drizConnection.prepareStatement(query);
            ps.setInt(1, i+1);
            for(int k=2; k<502; k++) {
                ps.setInt(k, i);
            }
            ps.execute();
        }
        System.out.println(System.currentTimeMillis() - startTime);
        stmt.executeUpdate("drop table if exists loadsofdata3");
        stmt.executeUpdate("create table loadsofdata3 (id int not null primary key auto_increment,"+sb.toString()+") engine=innodb");
        
        startTime=System.currentTimeMillis();
        for(int i=0; i < 10000; i++) {
            PreparedStatement ps = mysqlConnection.prepareStatement(query);
            ps.setInt(1, i+1);
            for(int k=2; k<502; k++) {
                ps.setInt(k, i);
            }
            ps.execute();

        }
        System.out.println(System.currentTimeMillis() - startTime);

    }
}

