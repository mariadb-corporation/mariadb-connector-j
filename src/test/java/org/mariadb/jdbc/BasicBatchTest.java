package org.mariadb.jdbc;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;

import java.sql.*;

import static org.junit.Assert.*;

public class BasicBatchTest extends BaseTest {

    /**
     * Tables initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("test_batch", "id int not null primary key auto_increment, test varchar(10)");
        createTable("test_batch2", "id int not null primary key auto_increment, test varchar(10)");
        createTable("test_batch3", "id int not null primary key auto_increment, test varchar(10)");
        createTable("batchUpdateException", "i int,PRIMARY KEY (i)");
        createTable("batchPrepareUpdateException", "i int,PRIMARY KEY (i)");
        createTable("rewritetest", "id int not null primary key, a varchar(10), b int", "engine=innodb");
        createTable("rewritetest2", "id int not null primary key, a varchar(10), b int", "engine=innodb");
        createTable("bug501452", "id int not null primary key, value varchar(20)");
    }

    @Test
    public void batchTest() throws SQLException {
        Assume.assumeFalse(sharedIsRewrite());
        PreparedStatement ps = sharedConnection.prepareStatement("insert into test_batch values (null, ?)",
                Statement.RETURN_GENERATED_KEYS);
        ps.setString(1, "aaa");
        ps.addBatch();
        ps.setString(1, "bbb");
        ps.addBatch();
        ps.setString(1, "ccc");
        ps.addBatch();
        int[] batchResult = ps.executeBatch();
        ResultSet rs1 = ps.getGeneratedKeys();
        for (int count = 1; count <= 3; count++) {
            assertTrue(rs1.next());
            assertTrue(String.valueOf(count).equalsIgnoreCase(rs1.getString(1)));
        }
        for (int unitInsertNumber : batchResult) {
            assertEquals(1, unitInsertNumber);
        }
        ps.setString(1, "aaa");
        ps.addBatch();
        ps.setString(1, "bbb");
        ps.addBatch();
        ps.setString(1, "ccc");
        ps.addBatch();
        batchResult = ps.executeBatch();
        for (int unitInsertNumber : batchResult) {
            assertEquals(1, unitInsertNumber);
        }
        final ResultSet rs = sharedConnection.createStatement().executeQuery("select * from test_batch");
        ps.executeQuery("SELECT 1");
        rs1 = ps.getGeneratedKeys();
        assertEquals(MariaSelectResultSet.createEmptyResultSet(), rs1);
        assertEquals(true, rs.next());
        assertEquals("aaa", rs.getString(2));
        assertEquals(true, rs.next());
        assertEquals("bbb", rs.getString(2));
        assertEquals(true, rs.next());
        assertEquals("ccc", rs.getString(2));

    }

    @Test
    public void batchTestStmt() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.addBatch("insert into test_batch2 values (null, 'hej1')");
        stmt.addBatch("insert into test_batch2 values (null, 'hej2')");
        stmt.addBatch("insert into test_batch2 values (null, 'hej3')");
        stmt.addBatch("insert into test_batch2 values (null, 'hej4')");
        stmt.executeBatch();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from test_batch2");
        for (int i = 1; i <= 4; i++) {
            assertEquals(true, rs.next());
            assertEquals(i, rs.getInt(1));
            assertEquals("hej" + i, rs.getString(2));
        }
        assertEquals(false, rs.next());

    }

    @Test
    public void batchUpdateException() throws Exception {
        Statement st = sharedConnection.createStatement();
        st.addBatch("insert into batchUpdateException values(1)");
        st.addBatch("insert into batchUpdateException values(2)");
        st.addBatch("insert into batchUpdateException values(1)"); // will fail, duplicate primary key
        st.addBatch("insert into batchUpdateException values(3)");

        try {
            st.executeBatch();
            fail("exception should be throw above");
        } catch (BatchUpdateException bue) {
            int[] updateCounts = bue.getUpdateCounts();
            assertEquals(4, updateCounts.length);
            if (sharedIsRewrite()) {
                assertEquals(1, updateCounts[0]);
                assertEquals(1, updateCounts[1]);
                assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
                assertEquals(Statement.EXECUTE_FAILED, updateCounts[3]);
            } else {
                //prepare or allowMultiQueries options
                assertEquals(1, updateCounts[0]);
                assertEquals(1, updateCounts[1]);
                assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
                assertEquals(1, updateCounts[3]);
            }
            assertTrue(bue.getCause() instanceof SQLIntegrityConstraintViolationException);
        }
    }

    @Test
    public void batchPrepareUpdateException() throws Exception {
        PreparedStatement st = sharedConnection.prepareStatement("insert into batchPrepareUpdateException values(?)");
        st.setInt(1, 1);
        st.addBatch();
        st.setInt(1, 2);
        st.addBatch();
        st.setInt(1, 1); // will fail, duplicate primary key
        st.addBatch();
        st.setInt(1, 3);
        st.addBatch();

        try {
            st.executeBatch();
            fail("exception should be throw above");
        } catch (BatchUpdateException bue) {
            int[] updateCounts = bue.getUpdateCounts();
            assertEquals(4, updateCounts.length);
            if (sharedIsRewrite()) {
                assertEquals(Statement.EXECUTE_FAILED, updateCounts[0]);
                assertEquals(Statement.EXECUTE_FAILED, updateCounts[1]);
                assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
                assertEquals(Statement.EXECUTE_FAILED, updateCounts[3]);
            } else {
                //prepare or allowMultiQueries options
                assertEquals(1, updateCounts[0]);
                assertEquals(1, updateCounts[1]);
                assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
                assertEquals(1, updateCounts[3]);
            }
            assertTrue(bue.getCause() instanceof SQLIntegrityConstraintViolationException);
        }

    }

    @Test
    public void testBatchLoop() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into rewritetest values (?,?,?)");
        for (int i = 0; i < 10; i++) {
            ps.setInt(1, i);
            ps.setString(2, "bbb" + i);
            ps.setInt(3, 30 + i);
            ps.addBatch();
        }
        ps.executeBatch();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from rewritetest");
        int counter = 0;
        while (rs.next()) {
            assertEquals(counter++, rs.getInt("id"));
        }
        assertEquals(10, counter);
    }

    @Test
    public void testBatchLoopWithDupKey() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement(
                "insert into rewritetest2 values (?,?,?) on duplicate key update a=values(a)");
        for (int i = 0; i < 2; i++) {
            ps.setInt(1, 0);
            ps.setString(2, "bbb" + i);
            ps.setInt(3, 30 + i);
            ps.addBatch();
        }
        ps.executeBatch();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from rewritetest2");
        int counter = 0;
        while (rs.next()) {
            assertEquals(counter++, rs.getInt("id"));
        }
        assertEquals(1, counter);
    }


    @Test
    public void testBug501452() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into bug501452 (id,value) values (?,?)");
        ps.setObject(1, 1);
        ps.setObject(2, "value for 1");
        ps.addBatch();

        ps.executeBatch();

        ps.setObject(1, 2);
        ps.setObject(2, "value for 2");
        ps.addBatch();

        ps.executeBatch();
    }


    @Test
    public void testMultipleStatementBatch() throws SQLException {

        Statement stmt = sharedConnection.createStatement();
        stmt.addBatch("INSERT INTO test_batch3(test) value ('a')");
        stmt.addBatch("INSERT INTO test_batch3(test) value ('b')");
        stmt.addBatch("INSERT INTO test_batch3(test) value ('a')");
        stmt.addBatch("UPDATE test_batch3 set test='c' WHERE test = 'a'");
        stmt.addBatch("UPDATE test_batch3 set test='d' WHERE test = 'b'");
        stmt.addBatch("INSERT INTO test_batch3(test) value ('e')");

        int[] updateCount = stmt.executeBatch();
        assertEquals(6, updateCount.length);
        assertEquals(1, updateCount[0]);
        assertEquals(1, updateCount[1]);
        assertEquals(1, updateCount[2]);
        assertEquals(2, updateCount[3]);
        assertEquals(1, updateCount[4]);
        assertEquals(1, updateCount[5]);

        assertEquals(1, stmt.getUpdateCount());
        assertTrue(stmt.getMoreResults());
        assertEquals(1, stmt.getUpdateCount());
        assertTrue(stmt.getMoreResults());
        assertEquals(1, stmt.getUpdateCount());
        assertTrue(stmt.getMoreResults());
        assertEquals(2, stmt.getUpdateCount());
        assertTrue(stmt.getMoreResults());
        assertEquals(1, stmt.getUpdateCount());
        assertTrue(stmt.getMoreResults());
        assertEquals(1, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults());

        ResultSet resultSet = stmt.getGeneratedKeys();
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getInt(1));
        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getInt(1));
        assertTrue(resultSet.next());
        assertEquals(4, resultSet.getInt(1));
        assertFalse(resultSet.next());
    }

}
