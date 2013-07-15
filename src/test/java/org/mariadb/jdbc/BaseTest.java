package org.mariadb.jdbc;


import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.sql.*;

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

    boolean checkMaxAllowedPacket(String testName) throws SQLException
    {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("select @@max_allowed_packet");
        rs.next();
        int max_allowed_packet = rs.getInt(1);
        if(max_allowed_packet < 0xffffff)
        {
          System.out.println("test '" + testName + "' skipped  due to server variable max_allowed_packet < 16M");
          return false;
        }
        return true;
    }

    boolean haveSSL(){
            try {
                ResultSet rs = connection.createStatement().executeQuery("select @@have_ssl");
                rs.next();
                String value = rs.getString(1);
                return value.equals("YES");
            } catch (Exception e)  {
                return false; /* maybe 4.x ? */
            }
        }

    void requireMinimumVersion(int major, int minor) throws SQLException {
        DatabaseMetaData md = connection.getMetaData();
        int dbMajor = md.getDatabaseMajorVersion();
        int dbMinor = md.getDatabaseMinorVersion();
        org.junit.Assume.assumeTrue(dbMajor > major ||
                (dbMajor == major && dbMinor >= minor));

    }
}
