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
        createTable("testErrorMessage", "id int not null primary key auto_increment, test varchar(10)");
    }

    @Test
    public void testSmallRewriteErrorMessage() throws SQLException {
        try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
            executeBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test) values ('whoua0'), ('whoua1'), "
                    + "('whoua2'), ('whoua3'), ('whoua4'), ('whoua5'), ('whoua6'), ('whoua7'), ('whoua8'), ('whoua9'), "
                    + "('more than 10 characters to provoc error')"));
        }
    }

    @Test
    public void testSmallMultiErrorMessage() throws SQLException {
        Assume.assumeFalse(sharedIsRewrite());
        try (Connection connection = setConnection("&allowMultiQueries=true")) {
            executeBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test) values ('whoua0');"
                    + "INSERT INTO testErrorMessage(test) values ('whoua1');"
                    + "INSERT INTO testErrorMessage(test) values ('whoua2');"
                    + "INSERT INTO testErrorMessage(test) values ('whoua3');"
                    + "INSERT INTO testErrorMessage(test) values ('whoua4');"
                    + "INSERT INTO testErrorMessage(test) values ('whoua5');"
                    + "INSERT INTO testErrorMessage(test) values ('whoua6');"
                    + "INSERT INTO testErrorMessage(test) values ('whoua7');"
                    + "INSERT INTO testErrorMessage(test) values ('whoua8');"
                    + "INSERT INTO testErrorMessage(test) values ('whoua9');"
                    + "INSERT INTO testErrorMessage(test) values ('more than 10 characters to provoc error')"));
        }
    }

    @Test
    public void testSmallPrepareErrorMessage() throws SQLException {
        Assume.assumeTrue(sharedUsePrepare());
        try (Connection connection = setConnection("&useComMulti=false")) {
            executeBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test) values (?), parameters "
                    + "['more than 10 characters to provoc error']"));
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
                assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test) values (?), parameters "
                        + "['whoua0'],['whoua1'],['whoua2'],['whoua3'],['whoua4'],['whoua5'],['whoua6'],['whoua7'],['whoua8'],"
                        + "['whoua9'],['more than 10 characters to provoc error']"));
            } else {
                assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test) values (?), parameters "
                        + "['more than 10 characters to provoc error']"));
            }
        }
    }

    @Test
    public void testBigRewriteErrorMessage() throws SQLException {
        try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
            executeBigBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("('whoua72'), ('whoua73'), ('whoua74'), ('whoua75'), (..."));
        }
    }

    @Test
    public void testBigMultiErrorMessage() throws SQLException {
        Assume.assumeFalse(sharedIsRewrite());
        try (Connection connection = setConnection("&allowMultiQueries=true")) {
            executeBigBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(";INSERT INTO testErrorMessage(test) values ('whoua18');INSER..."));
        }
    }

    @Test
    public void testBigPrepareErrorMessage() throws SQLException {
        Assume.assumeTrue(sharedUsePrepare());
        try (Connection connection = setConnection("&useComMulti=false")) {
            executeBigBatchWithException(connection);
            fail("Must Have thrown error");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test) values (?), parameters "
                    + "['more than 10 characters to provoc error']"));
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
                assertTrue(sqle.getMessage().contains(",['whoua78'],['whoua79'],['whoua80'],[..."));
            } else {
                assertTrue(sqle.getMessage().contains("INSERT INTO testErrorMessage(test) values (?), parameters "
                        + "['more than 10 characters to provoc error']"));
            }
        }
    }


    private void executeBatchWithException(Connection connection) throws SQLException {
        connection.createStatement().execute("TRUNCATE testErrorMessage");
        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO testErrorMessage(test) values (?)")) {
            for (int i = 0; i < 10; i++) {
                preparedStatement.setString(1, "whoua" + i);
                preparedStatement.addBatch();
            }
            preparedStatement.setString(1, "more than 10 characters to provoc error");
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
        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO testErrorMessage(test) values (?)")) {
            for (int i = 0; i < 200; i++) {
                preparedStatement.setString(1, "whoua" + i);
                preparedStatement.addBatch();
            }
            preparedStatement.setString(1, "more than 10 characters to provoc error");
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            //to be sure that packets are ok
            connection.createStatement().execute("SELECT 1");
            throw e;
        }
    }

}
