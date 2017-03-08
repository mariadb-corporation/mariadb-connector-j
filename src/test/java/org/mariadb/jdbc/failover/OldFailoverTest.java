package org.mariadb.jdbc.failover;

import org.junit.Assert;
import org.junit.Test;
import org.mariadb.jdbc.BaseTest;

import java.sql.Connection;
import java.sql.Statement;

public class OldFailoverTest extends BaseTest {

    /**
     * Check old connection way before multihost was handle.
     *
     * @throws Exception exception
     */
    @Test
    public void isOldConfigurationValid() throws Exception {
        String falseUrl = "jdbc:mariadb://localhost:1111," + ((hostname == null) ? "localhost" : hostname) + ":"
                + port + "/" + database + "?user=" + username
                + (password != null && !"".equals(password) ? "&password=" + password : "")
                + (parameters != null ? "&" + parameters : "");

        try {
            //the first host doesn't exist, so with the random host selection, verifying that we connect to the good
            //host
            for (int i = 0; i < 10; i++) {
                Connection tmpConnection = openNewConnection(falseUrl);
                Statement tmpStatement = tmpConnection.createStatement();
                tmpStatement.execute("SELECT 1");
            }
        } catch (Exception e) {
            Assert.fail();
        }
    }


    @Test
    public void errorUrl() throws Exception {
        String falseUrl = "jdbc:mariadb://localhost:1111/test";

        try {
            openNewConnection(falseUrl);
            Assert.fail();
        } catch (Exception e) {
            //normal exception
        }
    }


}
