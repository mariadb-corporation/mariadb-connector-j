    package org.mariadb.jdbc;


    import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static junit.framework.Assert.*;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

    public class CallableStatementTest extends BaseTest{
        @Before
        public  void checkSP() throws SQLException{
          requireMinimumVersion(5,0);
        }

        @Test
        public void CallSimple()throws SQLException {
            CallableStatement st = connection.prepareCall("{?=call pow(?,?)}");
            st.setInt(2,2);
            st.setInt(3,2);
            st.execute();
            int result = st.getInt(1);
            assertEquals(result, 4);

        }

        private void create(String objType, String name, String body) throws SQLException{
            Statement st = connection.createStatement();
            try {
                st.execute("drop " + objType + " " + name);
            }
            catch(Exception e) {
                // eat exception
            }
            st.execute("create  "+ objType + " " + name + body);
        }
        private void createProcedure(String name, String body) throws SQLException{
            create("procedure", name, body);
        }

        @Test
        public void withResultSet() throws Exception {
            createProcedure("withResultSet", "(a int) begin select a; end");
            CallableStatement stmt = connection.prepareCall("{call withResultSet(?)}");
            stmt.setInt(1,1);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            int res = rs.getInt(1);
            assertEquals(res, 1);
        }

        @Test
        public void useParameterName() throws Exception {
            createProcedure("useParameterName", "(a int) begin select a; end");
            CallableStatement stmt = connection.prepareCall("{call useParameterName(?)}");
            stmt.setInt("a",1);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            int res = rs.getInt(1);
            assertEquals(res, 1);
        }

        @Test
        public void useWrongParameterName() throws Exception {
            createProcedure("useWrongParameterName", "(a int) begin select a; end");
            CallableStatement stmt = connection.prepareCall("{call useParameterName(?)}");

            try {
                stmt.setInt("b",1);
                fail("must fail");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().equals("there is no parameter with the name b"));
            }
        }
        @Test
        public void multiResultSets() throws Exception {
            createProcedure("multiResultSets", "() BEGIN  SELECT 1; SELECT 2; END");
            CallableStatement stmt = connection.prepareCall("{call multiResultSets()}");
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
        public void inoutParam() throws SQLException{
            CallableStatement storedProc = null;

            createProcedure("inoutParam",
                    "(INOUT p1 INT) begin set p1 = p1 + 1; end\n");

            storedProc = connection.prepareCall("{call inOutParam(?)}");

            storedProc.setInt(1,1);
            storedProc.registerOutParameter(1, Types.INTEGER);
            storedProc.execute();
            assertEquals(2, storedProc.getObject(1));
        }
        
        @Test
        public void getProcedures() throws SQLException{

            createProcedure("testGetProcedures",
                    "(INOUT p1 INT) begin set p1 = p1 + 1; end\n");

            ResultSet rs = connection.getMetaData().getProcedures(null, null, "testGetProc%");
            
            ResultSetMetaData md = rs.getMetaData();
            
        	for(int i = 1; i <= md.getColumnCount();i++) {
        		log.fine(md.getColumnLabel(i));
        	}
            while(rs.next()) {

            	for(int i = 1; i <= rs.getMetaData().getColumnCount();i++) {
                    log.fine(rs.getObject(i)+ " ");
            	}

            }
        }
        
        @Test
        public void withStrangeParameter() throws SQLException {
        	createProcedure("withStrangeParameter", "(IN a DECIMAL(10,2)) begin select a; end");
            CallableStatement stmt = connection.prepareCall("{call withStrangeParameter(?)}");
            double expected = 5.43;
            stmt.setDouble("a", expected);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            double res = rs.getDouble(1);
            assertEquals(expected, res);
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
