package org.mariadb.jdbc;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;


@Ignore
public class LoadTest extends BaseTest {

    @Test
    public void tm() throws SQLException {
        Connection drizConnection = openNewConnection(connU);
        setConnection();

        long sum = 0;
        int i;
        for (i = 0; i < 10; i++) {
            sum += this.loadTest(drizConnection);
            log.trace(String.valueOf(i));
        }
        log.trace(String.valueOf(sum / i));
        sum = 0;
        for (i = 0; i < 10; i++) {
            sum += this.loadTest(connection);
            log.trace(String.valueOf(i));
        }
        log.trace(String.valueOf(sum / i));
    }

    public long loadTest(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();
        createTestTable("loadsofdata","id int not null primary key auto_increment, data varchar(250)","engine=innodb");
        stmt.close();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            stmt = connection.createStatement();
            stmt.executeUpdate("insert into loadsofdata (data) values ('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" + i + "')");
            stmt.close();
        }
        for (int i = 0; i < 100; i++) {
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select * from loadsofdata ");
            while (rs.next()) {
                rs.getInt("id");
                rs.getString("data");
                rs.getInt(1);
                rs.getString(2);
            }
            rs.close();
            stmt.close();
        }
        return System.currentTimeMillis() - startTime;
    }

    @Test
    public void prepareTest() throws SQLException {
        Connection drizConnection = openNewConnection(connU);
        setConnection();
        Statement stmt = drizConnection.createStatement();
        createTestTable("loadsofdata2","id int not null primary key auto_increment, data blob","engine=innodb");
        byte[] theBytes = new byte[500];
        for (int i = 0; i < 500; i++) {
            theBytes[i] = (byte) i;
        }
        long startTime = System.currentTimeMillis();

        for (int i = 1; i < 10000; i++) {
            PreparedStatement ps = drizConnection.prepareStatement("insert into loadsofdata2 (id,data) values (?,?)");
            ps.setInt(1, i);
            ps.setBytes(2, theBytes);
            ps.execute();
            ps.close();
        }
        log.debug(String.valueOf(System.currentTimeMillis() - startTime));

        createTestTable("loadsofdata2", "id int not null primary key auto_increment, data blob", "engine=innodb");

        startTime = System.currentTimeMillis();
        for (int i = 1; i < 10000; i++) {
            PreparedStatement ps = connection.prepareStatement("insert into loadsofdata2 (id,data) values (?,?)");
            ps.setInt(1, i);
            ps.setBytes(2, theBytes);
            ps.execute();
            ps.close();
        }
        log.debug(String.valueOf(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();

        for (int i = 1; i < 10000; i++) {
            PreparedStatement ps = drizConnection.prepareStatement("select * from loadsofdata2 where id = ?");
            ps.setInt(1, i);
            ResultSet rs = ps.executeQuery();
            rs.next();
            rs.getBytes(2);
            ps.close();
        }
        log.debug(String.valueOf(System.currentTimeMillis() - startTime));

        startTime = System.currentTimeMillis();

        for (int i = 1; i < 10000; i++) {
            PreparedStatement ps = connection.prepareStatement("select * from loadsofdata2 where id = ?");
            ps.setInt(1, i);
            ResultSet rs = ps.executeQuery();
            rs.next();
            rs.getBytes(2);
            ps.close();

        }

        log.debug(String.valueOf(System.currentTimeMillis() - startTime));

    }

    @Test
    public void prepareManyParamsTest() throws SQLException {
        Connection drizConnection = openNewConnection(connU);
        setConnection();
        Statement stmt = drizConnection.createStatement();

        StringBuilder sb = new StringBuilder("d0 int");
        for (int i = 1; i < 500; i++) {

            sb.append(", d").append(i).append(" int");
        }
        createTestTable("loadsofdata3","id int not null primary key auto_increment," + sb.toString(), "engine=innodb");
        StringBuilder qb = new StringBuilder("?");
        for (int i = 1; i < 500; i++) {
            qb.append(", ?");
        }

        String query = "insert into loadsofdata3 values(?, " + qb.toString() + ")";
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            PreparedStatement ps = drizConnection.prepareStatement(query);
            ps.setInt(1, i + 1);
            for (int k = 2; k < 502; k++) {
                ps.setInt(k, i);
            }
            ps.execute();
        }
        log.debug(String.valueOf(System.currentTimeMillis() - startTime));
        createTestTable("loadsofdata3","id int not null primary key auto_increment," + sb.toString(), "engine=innodb");

        startTime = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            PreparedStatement ps = connection.prepareStatement(query);
            ps.setInt(1, i + 1);
            for (int k = 2; k < 502; k++) {
                ps.setInt(k, i);
            }
            ps.execute();

        }
        log.debug(String.valueOf(System.currentTimeMillis() - startTime));

    }

    @Test
    public void benchPrepare() throws SQLException {
        Connection drizConnection = openNewConnection(connU);
        long startTime = System.nanoTime();
        long x = 1000000;
        for (int i = 0; i < x; i++) {
            drizConnection.prepareStatement("SELECT * FROM EH WHERE EH=? and UU=? and GG=?");
        }
        log.debug(String.valueOf((System.nanoTime() - startTime) / x));
    }


}

