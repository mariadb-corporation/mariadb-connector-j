package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

/**
 * Created by diego on 10/07/2017.
 */
public class Testt extends BaseTest {

    @Test
    public void testt() throws Exception {
        createTable("BOUH", "t1 DATETIME(6), t2 DATETIME(6), t3 DATETIME(6)");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("INSERT INTO BOUH(t1,t2,t3) values ('0000-00-00 00:00:00', null, '2017-03-09 12:13:14.156789')");

        try (Connection connection = DriverManager.getConnection("jdbc:mariadb://localhost:3306/testj?user=root&profileSql=true&useServerPrepStmts=false")) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * from BOUH")) {
                ResultSet rs = preparedStatement.executeQuery();
                rs.next();
                //        Assert.assertEquals(null, rs.getInt(1));
                //        Assert.assertEquals(null, rs.getInt(2));
                Assert.assertEquals(null, rs.getDate(1));
                Assert.assertEquals(null, rs.getDate(2));
                Assert.assertEquals(null, rs.getDate(2));
                Assert.assertEquals("0000-00-00 00:00:00.000000", rs.getString(1));
                Assert.assertEquals(null, rs.getString(2));
                Assert.assertEquals("2017-03-09 12:13:14.156789", rs.getString(3));
                Assert.assertEquals(null, rs.getObject(1));
                Assert.assertEquals(null, rs.getObject(2));
                Assert.assertEquals(null, rs.getTimestamp(1));
                Assert.assertEquals(null, rs.getTimestamp(2));


            }
        }
    }
}
