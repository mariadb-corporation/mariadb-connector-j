package org.mariadb.jdbc;

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
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("testErrorMessage", "id int not null primary key auto_increment, test varchar(10), test2 int");
    }

    @Test
    public void testSmallRewriteErrorMessage() throws SQLException {
        try (Connection connection = setBlankConnection("&rewriteBatchedStatements=true")) {
            executeBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getCause().getCause().getMessage().contains(
                    "INSERT INTO testErrorMessage(test, test2) values ('whoua0', 0), ('whoua1', 1), "
                    + "('whoua2', 2), ('whoua3', 3), ('whoua4', 4), ('whoua5', 5), ('whoua6', 6), ('whoua7', 7), "
                    + "('whoua8', 8), ('whoua9', 9), ('more than 10 characters to provoc error', 10)"));
        }
    }

    @Test
    public void testSmallMultiBatchErrorMessage() throws SQLException {
        try (Connection connection = setBlankConnection("&allowMultiQueries=true&useServerPrepStmts=false")) {
            executeBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue("message : " + sqle.getCause().getCause().getMessage(),
                    sqle.getCause().getCause().getMessage().contains(
                    "INSERT INTO testErrorMessage(test, test2) values "
                    + "('more than 10 characters to provoc error', 10)"));
        }
    }

    @Test
    public void testSmallPrepareErrorMessage() throws SQLException {
        try (Connection connection = setBlankConnection("&useBatchMultiSend=false")) {
            executeBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getCause().getCause().getMessage().contains(
                    "INSERT INTO testErrorMessage(test, test2) values (?, ?), "
                    + "parameters ['more than 10 characters to provoc error',10]"));
        }
    }

    @Test
    public void testSmallBulkErrorMessage() throws SQLException {
        Connection connection = setBlankConnection("&useBatchMultiSend=true");
        try {
            executeBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getCause().getCause().getMessage().contains(
                    "INSERT INTO testErrorMessage(test, test2) values (?, ?), parameters "
                    + "['whoua0',0],['whoua1',1],['whoua2',2],['whoua3',3],['whoua4',4],['whoua5',5],['whoua6',6],"
                    + "['whoua7',7],['whoua8',8],['whoua9',9],['more than 10 characters to provoc error',10]"));
        } finally {
            connection.close();
        }
    }

    @Test
    public void testBigRewriteErrorMessage() throws SQLException {
        try (Connection connection = setBlankConnection("&rewriteBatchedStatements=true")) {
            executeBigBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getCause().getCause().getMessage().contains("('whoua56', 56), ('whoua57', 57), ('whou..."));
        }
    }

    @Test
    public void testBigMultiErrorMessage() throws SQLException {
        try (Connection connection = setBlankConnection("&allowMultiQueries=true&useServerPrepStmts=false")) {
            executeBigBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue("message : " + sqle.getCause().getCause().getMessage(),
                    sqle.getCause().getCause().getMessage().contains(
                    "INSERT INTO testErrorMessage(test, test2) values "
                    + "('more than 10 characters to provoc error', 200)"));
        }
    }

    @Test
    public void testBigPrepareErrorMessage() throws SQLException {
        try (Connection connection = setBlankConnection("&useBatchMultiSend=false")) {
            executeBigBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue("message : " + sqle.getCause().getCause().getMessage(),
                    sqle.getCause().getCause().getMessage().contains(
                    "INSERT INTO testErrorMessage(test, test2) values (?, ?), parameters "
                            + "['more than 10 characters to provoc error',200]"));
        }
    }

    @Test
    public void testBigBulkErrorMessage() throws SQLException {
        Connection connection = setBlankConnection("&useBatchMultiSend=true");
        try {
            executeBigBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getCause().getCause().getMessage().contains(
                    ",['whoua60',60],['whoua61',61],['whoua62',62],['whoua63',63],['whoua64',64..."));
        } finally {
            connection.close();
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
