package org.mariadb.jdbc;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ErrorMessageTest extends BaseTest {

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("testErrorMessage", "id int not null primary key auto_increment, test varchar(10), test2 int");
    }

    @Test
    public void testSmallRewriteErrorMessage() throws SQLException {
        try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
            executeBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test, test2) values ('whoua0', 0), ('whoua1', 1), "
                    + "('whoua2', 2), ('whoua3', 3), ('whoua4', 4), ('whoua5', 5), ('whoua6', 6), ('whoua7', 7), "
                    + "('whoua8', 8), ('whoua9', 9), ('more than 10 characters to provoc error', 10)"));
        }
    }

    @Test
    public void testSmallMultiErrorMessage() throws SQLException {
        Assume.assumeFalse(sharedIsRewrite());
        try (Connection connection = setConnection("&allowMultiQueries=true")) {
            executeBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test, test2) values ('whoua0', 0);"
                    + "INSERT INTO testErrorMessage(test, test2) values ('whoua1', 1);"
                    + "INSERT INTO testErrorMessage(test, test2) values ('whoua2', 2);"
                    + "INSERT INTO testErrorMessage(test, test2) values ('whoua3', 3);"
                    + "INSERT INTO testErrorMessage(test, test2) values ('whoua4', 4);"
                    + "INSERT INTO testErrorMessage(test, test2) values ('whoua5', 5);"
                    + "INSERT INTO testErrorMessage(test, test2) values ('whoua6', 6);"
                    + "INSERT INTO testErrorMessage(test, test2) values ('whoua7', 7);"
                    + "INSERT INTO testErrorMessage(test, test2) values ('whoua8', 8);"
                    + "INSERT INTO testErrorMessage(test, test2) values ('whoua9', 9);"
                    + "INSERT INTO testErrorMessage(test, test2) values ('more than 10 characters to provoc error', 10)"));
        }
    }

    @Test
    public void testSmallPrepareErrorMessage() throws SQLException {
        Assume.assumeTrue(sharedUsePrepare());
        try (Connection connection = setConnection("&useComMulti=false")) {
            executeBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test, test2) values (?, ?), "
                    + "parameters ['more than 10 characters to provoc error',10]"));
        }
    }

    @Test
    public void testSmallComMultiErrorMessage() throws SQLException {
        Assume.assumeTrue(sharedUsePrepare());
        try (Connection connection = setConnection("&useComMulti=true")) {
            executeBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            if (minVersion(10, 2)) {
                assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test, test2) values (?, ?), parameters "
                        + "['whoua0',0],['whoua1',1],['whoua2',2],['whoua3',3],['whoua4',4],['whoua5',5],['whoua6',6],"
                        + "['whoua7',7],['whoua8',8],['whoua9',9],['more than 10 characters to provoc error',10]"));
            } else {
                assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test, test2) values (?, ?), "
                        + "parameters ['more than 10 characters to provoc error',10]"));
            }
        }
    }

    @Test
    public void testBigRewriteErrorMessage() throws SQLException {
        try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
            executeBigBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("('whoua56', 56), ('whoua57', 57), ('whou..."));
        }
    }

    @Test
    public void testBigMultiErrorMessage() throws SQLException {
        Assume.assumeFalse(sharedIsRewrite());
        try (Connection connection = setConnection("&allowMultiQueries=true")) {
            executeBigBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(";INSERT INTO testErrorMessage(test, test2) values ('whoua15', 15);I..."));
        }
    }

    @Test
    public void testBigPrepareErrorMessage() throws SQLException {
        Assume.assumeTrue(sharedUsePrepare());
        try (Connection connection = setConnection("&useComMulti=false")) {
            executeBigBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test, test2) values (?, ?), parameters "
                    + "['more than 10 characters to provoc error',200]"));
        }
    }

    @Test
    public void testBigComMultiErrorMessage() throws SQLException {
        Assume.assumeTrue(sharedUsePrepare());
        try (Connection connection = setConnection("&useComMulti=true")) {
            executeBigBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            if (minVersion(10, 2)) {
                assertTrue(sqle.getMessage().contains(",['whoua60',60],['whoua61',61],['whoua62',62],['whoua63',63],['whoua64',64..."));
            } else {
                assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test, test2) values (?, ?), parameters "
                        + "['more than 10 characters to provoc error',200]"));
            }
        }
    }


    private void executeBatchWithException(Connection connection) throws SQLException {
        connection.createStatement().execute("TRUNCATE testErrorMessage");
        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO testErrorMessage(test, test2) values (?, ?)")) {
            for (int i = 0; i < 10; i++) {
                preparedStatement.setString(1, "whoua" + i);
                preparedStatement.setInt(2, i);
                preparedStatement.addBatch();
            }
            preparedStatement.setString(1, "more than 10 characters to provoc error");
            preparedStatement.setInt(2, 10);

            preparedStatement.addBatch();
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            //to be sure that packets are ok
            connection.createStatement().execute("SELECT 1");
            throw e;
        }
    }

    private void executeBigBatchWithException(Connection connection) throws SQLException {
        connection.createStatement().execute("TRUNCATE testErrorMessage");
        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO testErrorMessage(test, test2) values (?, ?)")) {
            for (int i = 0; i < 200; i++) {
                preparedStatement.setString(1, "whoua" + i);
                preparedStatement.setInt(2, i);
                preparedStatement.addBatch();
            }
            preparedStatement.setString(1, "more than 10 characters to provoc error");
            preparedStatement.setInt(2, 200);
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            //to be sure that packets are ok
            connection.createStatement().execute("SELECT 1");
            throw e;
        }
    }

}
