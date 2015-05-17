package org.mariadb.jdbc;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.fail;

/**
 * Created by diego_000 on 17/05/2015.
 */
public class AuroraFailoverTest {

    static { Logger.getLogger("").setLevel(Level.FINEST); }
    private final static Logger log = Logger.getLogger(FailoverTest.class.getName());

    //the active connection
    protected Connection connection;
    //default multi-host URL
    protected static final String defaultUrl = "jdbc:mysql://host1,host2,host3:3306/test?user=root";
    //hosts
    protected String[] hosts;

    @Before
    public void beforeClassFailover()  throws SQLException {
        //get the multi-host connection string
        String url = System.getProperty("dbUrl", defaultUrl);
        //parse the url
        JDBCUrl jdbcUrl = JDBCUrl.parse(url);
        connection = DriverManager.getConnection(url);
    }

    @Test
    public void simulateChangeToReadonlyHost()  throws SQLException{
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("show global variables like 'innodb_read_only'");
        rs.next();
        log.info("Connect to master : READ ONLY : " + rs.getString(2));
        Assert.assertTrue("OFF".equals(rs.getString(2)));

        //switching to read-onlyConnection
        connection.setReadOnly(true);

        //verification that secondary take place
        rs = stmt.executeQuery("show global variables like 'innodb_read_only'");
        rs.next();
        log.info("verifying switch to replica : READ ONLY : " + rs.getString(2));
        Assert.assertFalse("OFF".equals(rs.getString(2)));

        //simulate master crash
        try {
            stmt.execute("ALTER SYSTEM CRASH");
        } catch ( Exception e) { }

        rs = stmt.executeQuery("show global variables like 'innodb_read_only'");
        rs.next();
        log.info("verifying switch temporary to master : " + rs.getString(2));
        Assert.assertTrue("OFF".equals(rs.getString(2)));

        try {
            Thread.sleep((long) 90 * 1000);
        } catch (InterruptedException IE) { }

        //after 90 second, the ALTER SYSTEM CRASH has ended, so the master must have been relaunched.
        //switching to master connection
        connection.setReadOnly(false);

        //verification that secondary take place
        rs = stmt.executeQuery("show global variables like 'innodb_read_only'");
        rs.next();
        log.info("verifying switch to replica : READ ONLY : " + rs.getString(2));
        Assert.assertTrue("OFF".equals(rs.getString(2)));
    }

    @After
    public void after() throws SQLException {
        try {
            connection.close();
        } catch(Exception e) {
            logInfo(e.toString());
        }
    }

    // common function for logging information
    static void logInfo(String message) {
        log.info(message);
    }
}
