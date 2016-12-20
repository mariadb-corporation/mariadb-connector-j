package org.mariadb.jdbc;


import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Properties;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class CallStatementTest extends BaseTest {
    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createProcedure("useParameterName", "(a int) begin select a; end");
        createProcedure("useWrongParameterName", "(a int) begin select a; end");
        createProcedure("multiResultSets", "() BEGIN  SELECT 1; SELECT 2; END");
        createProcedure("inoutParam", "(INOUT p1 INT) begin set p1 = p1 + 1; end\n");
        createProcedure("testGetProcedures", "(INOUT p1 INT) begin set p1 = p1 + 1; end\n");
        createProcedure("withStrangeParameter", "(IN a DECIMAL(10,2)) begin select a; end");
        createProcedure("TEST_SP1", "() BEGIN\n"
                + "SELECT @Something := 'Something';\n"
                + "SIGNAL SQLSTATE '70100'\n"
                + "SET MESSAGE_TEXT = 'Test error from SP'; \n"
                + "END");
    }

    @Before
    public void checkSp() throws SQLException {
        requireMinimumVersion(5, 0);
    }

    @Test
    public void stmtSimple() throws SQLException {
        createProcedure("stmtSimple", "(IN p1 INT, IN p2 INT) begin SELECT p1 + p2; end\n");
        ResultSet rs = sharedConnection.createStatement().executeQuery("{call stmtSimple(2,2)}");
        rs.next();
        int result = rs.getInt(1);
        assertEquals(result, 4);
    }

    @Test
    public void prepareStmtSimple() throws SQLException {
        createProcedure("prepareStmtSimple", "(IN p1 INT, IN p2 INT) begin SELECT p1 + p2; end\n");
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("{call prepareStmtSimple(?,?)}");
        preparedStatement.setInt(1, 2);
        preparedStatement.setInt(2, 2);
        ResultSet rs = preparedStatement.executeQuery();
        rs.next();
        int result = rs.getInt(1);
        assertEquals(result, 4);
    }

    @Test
    public void stmtSimpleFunction() throws SQLException {
        try {
            createFunction("stmtSimpleFunction", "(a float, b bigint, c int) RETURNS INT NO SQL\nBEGIN\nRETURN a;\nEND");
            sharedConnection.createStatement().execute("{call stmtSimpleFunction(2,2,2)}");
            fail("call mustn't work for function, use SELECT <function>");
        } catch (SQLSyntaxErrorException sqle) {
            assertTrue(sqle.getMessage().contains("PROCEDURE testj.stmtSimpleFunction does not exist"));
        }
    }

    @Test
    public void prepareStmtSimpleFunction() throws SQLException {
        try {
            createFunction("stmtSimpleFunction", "(a float, b bigint, c int) RETURNS INT NO SQL\nBEGIN\nRETURN a;\nEND");
            PreparedStatement preparedStatement = sharedConnection.prepareStatement("{call stmtSimpleFunction(?,?,?)}");
            preparedStatement.setInt(1, 2);
            preparedStatement.setInt(2, 2);
            preparedStatement.setInt(3, 2);
            preparedStatement.execute();
            fail("call mustn't work for function, use SELECT <function>");
        } catch (SQLSyntaxErrorException sqle) {
            assertTrue(sqle.getMessage().contains("PROCEDURE testj.stmtSimpleFunction does not exist"));
        }
    }

    @Test
    public void prepareStmtWithOutParameter() throws SQLException {
        Assume.assumeTrue(sharedUsePrepare());
        createProcedure("prepareStmtWithOutParameter", "(x int, INOUT y int)\n"
                + "BEGIN\n"
                + "SELECT 1;end\n");
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("{call prepareStmtWithOutParameter(?,?)}");
        preparedStatement.setInt(1, 2);
        preparedStatement.setInt(2, 3);
        preparedStatement.execute();
    }

    @Test
    public void prepareBatchMultiResultSets() throws Exception {
        PreparedStatement stmt = sharedConnection.prepareStatement("{call multiResultSets()}");
        stmt.addBatch();
        stmt.addBatch();
        try {
            stmt.executeBatch();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Select command are not permitted via executeBatch() command"));
        }
    }

    @Test
    public void stmtMultiResultSets() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("{call multiResultSets()}");
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        assertTrue(stmt.getMoreResults());
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void prepareStmtMultiResultSets() throws Exception {
        PreparedStatement stmt = sharedConnection.prepareStatement("{call multiResultSets()}");
        stmt.execute();
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        assertTrue(stmt.getMoreResults());
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void stmtInoutParam() throws SQLException {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("{call inOutParam(1)}");
            fail("must fail : statement cannot be use when there is out parameter");
        } catch (SQLSyntaxErrorException e) {
            assertTrue(e.getMessage().contains("OUT or INOUT argument 1 for routine testj.inOutParam is not a variable "
                    + "or NEW pseudo-variable in BEFORE trigger\n"
                    + "Query is : call inOutParam(1)"));
        }
    }

    @Test
    public void prepareStmtInoutParam() throws SQLException {
        Assume.assumeTrue(sharedUsePrepare());
        //must work, but out parameter isn't accessible
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("{call inOutParam(?)}");
        preparedStatement.setInt(1, 1);
        preparedStatement.execute();
    }

    @Test
    public void getProcedures() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, "testGetProc%");
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            md.getColumnLabel(i);
        }

        while (rs.next()) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                rs.getObject(i);
            }
        }
    }

    @Test
    public void meta() throws Exception {
        createProcedure("callabletest1", "()\nBEGIN\nSELECT 1;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, "callabletest1");
        if (rs.next()) {
            assertTrue("callabletest1".equals(rs.getString(3)));
        } else {
            fail();
        }
    }


    @Test
    public void testMetaWildcard() throws Exception {
        createProcedure("testMetaWildcard", "(x int, out y int)\n"
                + "BEGIN\n"
                + "SELECT 1;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedureColumns(null, null, "testMetaWildcard%", "%");
        if (rs.next()) {
            assertEquals("testMetaWildcard", rs.getString(3));
            assertEquals("x", rs.getString(4));

            assertTrue(rs.next());
            assertEquals("testMetaWildcard", rs.getString(3));
            assertEquals("y", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMetaCatalog() throws Exception {
        createProcedure("testMetaCatalog", "(x int, out y int)\nBEGIN\nSELECT 1;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedures(sharedConnection.getCatalog(), null, "testMetaCatalog");
        if (rs.next()) {
            assertTrue("testMetaCatalog".equals(rs.getString(3)));
            assertFalse(rs.next());
        } else {
            fail();
        }
        //test with bad catalog
        rs = sharedConnection.getMetaData().getProcedures("yahoooo", null, "testMetaCatalog");
        assertFalse(rs.next());

        //test without catalog
        rs = sharedConnection.getMetaData().getProcedures(null, null, "testMetaCatalog");
        if (rs.next()) {
            assertTrue("testMetaCatalog".equals(rs.getString(3)));
            assertFalse(rs.next());
        } else {
            fail();
        }
    }

    @Test
    public void prepareWithNoParameters() throws SQLException {
        createProcedure("prepareWithNoParameters", "()\n"
                + "begin\n"
                + "    SELECT 'mike';"
                + "end\n");

        PreparedStatement preparedStatement = sharedConnection.prepareStatement("{call prepareWithNoParameters()}");
        ResultSet rs = preparedStatement.executeQuery();
        rs.next();
        Assert.assertEquals("mike", rs.getString(1));
    }

    @Test
    public void testCallWithFetchSize() throws SQLException {
        createProcedure("testCallWithFetchSize", "()\nBEGIN\nSELECT 1;SELECT 2;\nEND");
        try (Statement statement = sharedConnection.createStatement()) {
            statement.setFetchSize(1);
            try (ResultSet resultSet = statement.executeQuery("CALL testCallWithFetchSize()")) {
                int rowCount = 0;
                while (resultSet.next()) {
                    rowCount++;
                }
                Assert.assertEquals(1, rowCount);
            }
            statement.execute("SELECT 1");
        }
    }

}
