package org.mariadb.jdbc;

import org.junit.Test;

import java.nio.charset.Charset;
import java.sql.*;

import static org.junit.Assert.*;

public class PasswordEncodingTest extends BaseTest {
    private static String exoticPwd = "abéï你好";

    @Test
    public void testPwdCharset() throws Exception {
        cancelForVersion(5, 6, 36); //has password charset issue

        String[] charsets = new String[] {"UTF-8",
                "windows-1252",
                "Big5"};
        String[] charsetsMysql = new String[] {"utf8",
                "latin1",
                "big5"};
        try {
            for (int i = 0; i < charsets.length; i++) {
                createUser(charsets[i], charsetsMysql[i]);
            }

            for (String currentCharsetName : charsets) {
                try (Connection connection = DriverManager.getConnection("jdbc:mariadb://" + ((hostname != null) ? hostname : "localhost")
                        + ":" + port + "/" + database + "?user=test" + currentCharsetName + "&password=" + exoticPwd)) {
                    //windows-1252 and windows-1250 will work have the same conversion for this password
                    if (!currentCharsetName.equals(Charset.defaultCharset().name())
                            && (!"windows-1252".equals(currentCharsetName) || !Charset.defaultCharset().name().startsWith("windows-125"))) {
                        fail("must have failed for currentCharsetName=" + currentCharsetName + " using java default charset "
                                + Charset.defaultCharset().name());
                    }
                } catch (SQLInvalidAuthorizationSpecException sqle) {
                    if (currentCharsetName.equals(Charset.defaultCharset().name())) {
                        fail("must have not have failed for charsetName=" + currentCharsetName + " which is java default");
                    }
                }
            }

            for (String charsetName : charsets) checkConnection(charsetName, charsets);
        } finally {
            Statement stmt = sharedConnection.createStatement();
            for (String charsetName : charsets) {
                try {
                    stmt.execute("DROP USER 'test" + charsetName + "'@'%'");
                } catch (SQLException e) {
                    //nothing
                }
            }
        }


    }

    private void createUser(String charsetName, String serverCharset) throws Exception {
        try (Connection connection = setConnection()) {

            MariaDbStatement stmt = connection.createStatement().unwrap(MariaDbStatement.class);
            stmt.execute("set @@character_set_client='" + serverCharset + "'");
            stmt.execute("CREATE USER 'test" + charsetName + "'@'%'");

            //non jdbc method that send query according to charset
            stmt.testExecute("GRANT ALL on *.* to 'test" + charsetName + "' identified by '" + exoticPwd + "'", Charset.forName(charsetName));
        }
    }

    private void checkConnection(String charsetName, String[] charsets) throws Exception {

        for (String currentCharsetName : charsets) {
            try (Connection connection = DriverManager.getConnection("jdbc:mariadb://" + ((hostname != null) ? hostname : "localhost")
                    + ":" + port + "/" + database + "?user=test" + charsetName + "&password="
                    + exoticPwd + "&passwordCharacterEncoding=" + currentCharsetName)) {
                if (!currentCharsetName.equals(charsetName)) {
                    fail("must have failed for charsetName=" + charsetName + " using passwordCharacterEncoding=" + currentCharsetName);
                }

            } catch (SQLInvalidAuthorizationSpecException sqle) {
                if (currentCharsetName.equals(charsetName)) {
                    fail("must not have failed for charsetName=" + charsetName + " using passwordCharacterEncoding=" + currentCharsetName);
                }
            }
        }
    }

}
