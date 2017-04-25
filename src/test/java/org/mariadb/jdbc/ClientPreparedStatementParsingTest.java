package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class ClientPreparedStatementParsingTest extends BaseTest {

    private void checkParsing(String sql, int paramNumber, boolean rewritable, boolean allowMultiqueries, String[] partsRewrite,
                              String[] partsMulti) throws Exception {

        MariaDbPreparedStatementClient statement = new MariaDbPreparedStatementClient((MariaDbConnection) sharedConnection, sql,
                ResultSet.FETCH_FORWARD);
        assertEquals(paramNumber, statement.getParameterCount());

        if (sharedIsRewrite()) {
            for (int i = 0; i < partsRewrite.length; i++) {
                Assert.assertEquals(partsRewrite[i], new String(statement.getPrepareResult().getQueryParts().get(i)));
            }
            assertEquals(rewritable, statement.getPrepareResult().isQueryMultiValuesRewritable());
        } else {
            for (int i = 0; i < partsMulti.length; i++) {
                Assert.assertEquals(partsMulti[i], new String(statement.getPrepareResult().getQueryParts().get(i)));
            }
            assertEquals(allowMultiqueries, statement.getPrepareResult().isQueryMultipleRewritable());

        }
    }

    @Test
    public void testRewritableWithConstantParameter() throws Exception {
        checkParsing("INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE col2=col2+10",
                2, true, true,
                new String[]{
                        "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES",
                        " (9, ",
                        ", 5, ",
                        ", 8)",
                        " ON DUPLICATE KEY UPDATE col2=col2+10"},
                new String[]{
                        "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ",
                        ", 5, ",
                        ", 8) ON DUPLICATE KEY UPDATE col2=col2+10"});
    }

    @Test
    public void testComment() throws Exception {
        checkParsing("/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
                        + " INSERT into "
                        + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
                        + " tt VALUES "
                        + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
                        + " (?) "
                        + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */",
                1, true, true,
                new String[]{
                        "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
                                + " INSERT into "
                                + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
                                + " tt VALUES",
                        " /* insert Select INSERT INTO tt VALUES (?,?,?,?)  */ (",
                        ")",
                        " /* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"},
                new String[]{"/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
                        + " INSERT into "
                        + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
                        + " tt VALUES "
                        + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
                        + " (",
                        ") "
                                + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"});
    }

    @Test
    public void testRewritableWithConstantParameterAndParamAfterValue() throws Exception {
        checkParsing("INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE col2=?",
                3, false, true,
                new String[]{
                        "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES",
                        " (9, ",
                        ", 5, ",
                        ", 8) ON DUPLICATE KEY UPDATE col2=",
                        "",
                        ""},
                new String[]{"INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ",
                        ", 5, ",
                        ", 8) ON DUPLICATE KEY UPDATE col2=",
                        ""});
    }

    @Test
    public void testRewritableMultipleInserts() throws Exception {
        checkParsing("INSERT INTO TABLE(col1,col2) VALUES (?, ?), (?, ?)",
                4, false, true,
                new String[]{
                        "INSERT INTO TABLE(col1,col2) VALUES",
                        " (",
                        ", ",
                        "), (",
                        ", ",
                        ")",
                        ""},
                new String[]{"INSERT INTO TABLE(col1,col2) VALUES (",
                        ", ",
                        "), (",
                        ", ",
                        ")"});
    }


    @Test
    public void testCall() throws Exception {
        checkParsing("CALL dsdssd(?,?)",
                2, false, true,
                new String[]{
                        "CALL dsdssd(",
                        "",
                        ",",
                        ")",
                        ""},
                new String[]{"CALL dsdssd(",
                        ",",
                        ")"});
    }

    @Test
    public void testUpdate() throws Exception {
        checkParsing("UPDATE MultiTestt4 SET test = ? WHERE test = ?",
                2, false, true,
                new String[]{
                        "UPDATE MultiTestt4 SET test = ",
                        "",
                        " WHERE test = ",
                        "",
                        ""},
                new String[]{"UPDATE MultiTestt4 SET test = ",
                        " WHERE test = ",
                        ""});
    }

    @Test
    public void testInsertSelect() throws Exception {
        checkParsing("insert into test_insert_select ( field1) (select  TMP.field1 from (select CAST(? as binary) `field1` from dual) TMP)",
                1, false, true,
                new String[]{
                        "insert into test_insert_select ( field1) (select  TMP.field1 from (select CAST(",
                        "",
                        " as binary) `field1` from dual) TMP)",
                        ""},
                new String[]{"insert into test_insert_select ( field1) (select  TMP.field1 from (select CAST(",
                        " as binary) `field1` from dual) TMP)"});
    }

    @Test
    public void testWithoutParameter() throws Exception {
        checkParsing("SELECT testFunction()",
                0, false, true,
                new String[]{
                        "SELECT testFunction()",
                        "",
                        ""},
                new String[]{"SELECT testFunction()"});
    }

    @Test
    public void testWithoutParameterAndParenthesis() throws Exception {
        checkParsing("SELECT 1",
                0, false, true,
                new String[]{
                        "SELECT 1",
                        "",
                        ""},
                new String[]{"SELECT 1"});
    }

    @Test
    public void testWithoutParameterAndValues() throws Exception {
        checkParsing("INSERT INTO tt VALUES (1)",
                0, true, true,
                new String[]{
                        "INSERT INTO tt VALUES",
                        " (1)",
                        ""},
                new String[]{"INSERT INTO tt VALUES (1)"});
    }

    @Test
    public void testSemiColon() throws Exception {
        checkParsing("INSERT INTO tt (tt) VALUES (?); INSERT INTO tt (tt) VALUES ('multiple')",
                1, false, true,
                new String[]{
                        "INSERT INTO tt (tt) VALUES",
                        " (",
                        ")",
                        "; INSERT INTO tt (tt) VALUES ('multiple')"},
                new String[]{"INSERT INTO tt (tt) VALUES (",
                        "); INSERT INTO tt (tt) VALUES ('multiple')"});
    }

    @Test
    public void testSemicolonRewritableIfAtEnd() throws Exception {
        checkParsing("INSERT INTO table (column1) VALUES (?); ",
                1, true, false,
                new String[]{
                        "INSERT INTO table (column1) VALUES",
                        " (",
                        ")",
                        "; "},
                new String[]{"INSERT INTO table (column1) VALUES (",
                        "); "});
    }

    @Test
    public void testSemicolonNotRewritableIfNotAtEnd() throws Exception {
        checkParsing("INSERT INTO table (column1) VALUES (?); SELECT 1",
                1, false, true,
                new String[]{
                        "INSERT INTO table (column1) VALUES",
                        " (",
                        ")",
                        "; SELECT 1"},
                new String[]{"INSERT INTO table (column1) VALUES (",
                        "); SELECT 1"});
    }

    @Test
    public void testError() throws Exception {
        checkParsing("INSERT INTO tt (tt) VALUES (?); INSERT INTO tt (tt) VALUES ('multiple')",
                1, false, true,
                new String[]{
                        "INSERT INTO tt (tt) VALUES",
                        " (",
                        ")",
                        "; INSERT INTO tt (tt) VALUES ('multiple')"},
                new String[]{"INSERT INTO tt (tt) VALUES (",
                        "); INSERT INTO tt (tt) VALUES ('multiple')"});
    }


    @Test
    public void testLineComment() throws Exception {
        checkParsing("INSERT INTO tt (tt) VALUES (?) --fin",
                1, true, false,
                new String[]{
                        "INSERT INTO tt (tt) VALUES",
                        " (",
                        ")",
                        " --fin"},
                new String[]{"INSERT INTO tt (tt) VALUES (",
                        ") --fin"});
    }

    @Test
    public void testLineCommentFinished() throws Exception {
        checkParsing("INSERT INTO tt (tt) VALUES --fin\n (?)",
                1, true, true,
                new String[]{
                        "INSERT INTO tt (tt) VALUES",
                        " --fin\n (",
                        ")",
                        ""},
                new String[]{"INSERT INTO tt (tt) VALUES --fin\n (",
                        ")"});
    }

    @Test
    public void testSelect1() throws Exception {
        checkParsing("SELECT 1",
                0, false, true,
                new String[]{
                        "SELECT 1",
                        "",
                        ""},
                new String[]{"SELECT 1"});
    }

    @Test
    public void rewriteBatchedError() throws Exception {
        try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO errorTable (a, b) VALUES (?, ?, ?)");

            preparedStatement.setString(1, "1");
            preparedStatement.setString(2, "2");
            preparedStatement.setString(3, "3");
            preparedStatement.addBatch();
            try {
                preparedStatement.executeBatch();
                fail("must have thrown error since parameters are not good");
            } catch (SQLException e) {
                assertTrue(e.getCause().getCause().getMessage().contains("Query is: INSERT INTO errorTable (a, b) VALUES (?, ?, ?)"));
            }
        }
    }

    @Test
    public void testLastInsertId() throws Exception {
        checkParsing("INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?)",
                1, false, true,
                null,
                new String[]{"INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ")"});
    }

}
