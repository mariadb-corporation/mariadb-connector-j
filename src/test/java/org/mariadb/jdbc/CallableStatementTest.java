package org.mariadb.jdbc;


import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class CallableStatementTest extends BaseTest {
    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createProcedure("withResultSet", "(a int) begin select a; end");
        createProcedure("useParameterName", "(a int) begin select a; end");
        createProcedure("useWrongParameterName", "(a int) begin select a; end");
        createProcedure("multiResultSets", "() BEGIN  SELECT 1; SELECT 2; END");
        createProcedure("inoutParam", "(INOUT p1 INT) begin set p1 = p1 + 1; end\n");
        createProcedure("testGetProcedures", "(INOUT p1 INT) begin set p1 = p1 + 1; end\n");
        createProcedure("withStrangeParameter", "(IN a DECIMAL(10,2)) begin select a; end");
    }

    @Before
    public void checkSp() throws SQLException {
        requireMinimumVersion(5, 0);
    }

    @Test
    public void callSimple() throws SQLException {
        CallableStatement st = sharedConnection.prepareCall("{?=call pow(?,?)}");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        int result = st.getInt(1);
        assertEquals(result, 4);

    }


    @Test
    public void withResultSet() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call withResultSet(?)}");
        stmt.setInt(1, 1);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        int res = rs.getInt(1);
        assertEquals(res, 1);
    }

    @Test
    public void useParameterName() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call useParameterName(?)}");
        stmt.setInt("a", 1);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        int res = rs.getInt(1);
        assertEquals(res, 1);
    }

    @Test(expected = SQLException.class)
    public void useWrongParameterName() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call useParameterName(?)}");
        stmt.setInt("b", 1);
        fail("must fail");
    }


    @Test
    public void multiResultSets() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call multiResultSets()}");
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
    public void inoutParam() throws SQLException {
        CallableStatement storedProc = null;


        storedProc = sharedConnection.prepareCall("{call inOutParam(?)}");

        storedProc.setInt(1, 1);
        storedProc.registerOutParameter(1, Types.INTEGER);
        storedProc.execute();
        assertEquals(2, storedProc.getObject(1));
    }

    @Test
    public void getProcedures() throws SQLException {


        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, "testGetProc%");

        ResultSetMetaData md = rs.getMetaData();

        for (int i = 1; i <= md.getColumnCount(); i++) {
            log.trace(md.getColumnLabel(i));
        }
        while (rs.next()) {

            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                log.trace(rs.getObject(i) + " ");
            }

        }
    }

    @Test
    public void withStrangeParameter() throws SQLException {
        CallableStatement stmt = sharedConnection.prepareCall("{call withStrangeParameter(?)}");
        double expected = 5.43;
        stmt.setDouble("a", expected);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        double res = rs.getDouble(1);
        assertEquals(expected, res, 0);
        // now fail due to three decimals
        double tooMuch = 34.987;
        stmt.setDouble("a", tooMuch);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertThat(rs.getDouble(1), is(not(tooMuch)));
        rs.close();
        stmt.close();
    }
}
