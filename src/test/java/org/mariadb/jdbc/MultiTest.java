package org.mariadb.jdbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;


public class MultiTest{
    private static Connection connection;

    public MultiTest() throws SQLException {

    }

    @BeforeClass
    public static void before() throws SQLException {
        connection = DriverManager.getConnection("jdbc:mysql:thin://localhost:3306/test?allowMultiQueries=true&user=root");
        Statement st = connection.createStatement();
        st.executeUpdate("drop table if exists t1");
        st.executeUpdate("drop table if exists t2");
        st.executeUpdate("create table t2(id int, test varchar(100))");
        st.executeUpdate("create table t1(id int, test varchar(100))");
        st.execute("insert into t1 values(1,'a'),(2,'a')");
        st.execute("insert into t2 values(1,'a'),(2,'a')");
    }

    @AfterClass
    public static void after() throws SQLException {

        try {
            Statement st = connection.createStatement();
            st.executeUpdate("drop table if exists t1");
            st.executeUpdate("drop table if exists t2");

        } catch (Exception e) {
            // eat
        }
        finally {
            try  {
                connection.close();
            }catch (Exception e) {

            }
        }
    }
    @Test
    public void basicTest() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from t1;select * from t2;");
        int count = 0;
        while(rs.next()) {
            count++;
        }
        assertTrue(count > 0);
        assertTrue(statement.getMoreResults());
        rs = statement.getResultSet();
        count=0;
        while(rs.next()) {
            count++;
        }
        assertTrue(count>0);
        assertFalse(statement.getMoreResults());



    }

    @Test
    public void updateTest() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("update t1 set test='a "+System.currentTimeMillis()+"' where id = 2;select * from t2;");
        assertTrue(statement.getUpdateCount() == 1);
        assertTrue(statement.getMoreResults());
        ResultSet rs = statement.getResultSet();
        int count = 0;
        while(rs.next()) {
            count++;
        }
        assertTrue(count>0);
        assertFalse(statement.getMoreResults());
    }
    @Test
    public void updateTest2() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("select * from t2;update t1 set test='a "+System.currentTimeMillis()+"' where id = 2;");
        ResultSet rs = statement.getResultSet();
        int count = 0;
        while(rs.next()) { count++;}
        assertTrue(count>0);
        statement.getMoreResults();
        
        assertEquals(1,statement.getUpdateCount());
    }

    @Test
    public void selectTest() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("select * from t2;select * from t1;");
        ResultSet rs = statement.getResultSet();
        int count = 0;
        while(rs.next()) { count++;}
        assertTrue(count>0);
        rs = statement.executeQuery("select * from t1");
        count = 0;
        while(rs.next()) { count++;}
        assertTrue(count>0);
    }

   @Test
    public void setMaxRowsMulti() throws Exception {
        Statement st = connection.createStatement();
        assertEquals(0, st.getMaxRows());

        st.setMaxRows(1);
        assertEquals(1, st.getMaxRows());

        /* Check 3 rows are returned if maxRows is limited to 3, in every result set in batch */

       /* Check first result set for at most 3 rows*/
        ResultSet rs = st.executeQuery("select 1 union select 2;select 1 union select 2");
        int cnt=0;

        while(rs.next()) {
            cnt++;
        }
        rs.close();
        assertEquals(1, cnt);

       /* Check second result set for at most 3 rows*/
        assertTrue(st.getMoreResults());
        rs = st.getResultSet();
        cnt = 0;
        while(rs.next()) {
            cnt++;
        }
        rs.close();
        assertEquals(1, cnt);
   }
   
   
   /**
    * CONJ-99: rewriteBatchedStatements parameter.
    * @throws SQLException
    */
   @Test
   public void rewriteBatchedStatementsInsertTest() throws SQLException  {
	   connection = DriverManager.getConnection("jdbc:mysql:thin://localhost:3306/test?rewriteBatchedStatements=true&user=root");
       int cycles = 3000;
       PreparedStatement preparedStatement = prepareStatementBatch(cycles);
       int[] updateCounts = preparedStatement.executeBatch();
       int totalUpdates = 0;
       for (int count=0; count<updateCounts.length; count++) {
    	   assertTrue(updateCounts[count] > 0);
    	   totalUpdates += updateCounts[count];
       }
       assertEquals(cycles, totalUpdates);
       connection.createStatement().execute("TRUNCATE t1");
       Statement statement = connection.createStatement();
       for (int i = 0; i < cycles; i++) {
           statement.addBatch("INSERT INTO t1 VALUES (" + i + ", 'testValue" + i + "')");
       }
       updateCounts = statement.executeBatch();
       totalUpdates = 0;
       for (int count=0; count<updateCounts.length; count++) {
    	   assertTrue(updateCounts[count] > 0);
    	   totalUpdates += updateCounts[count];
       }
       assertEquals(cycles, totalUpdates);
   }
   
   private PreparedStatement prepareStatementBatch(int size) throws SQLException {
	   PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO t1 VALUES (?, ?)");
       for (int i = 0; i < size; i++) {
           preparedStatement.setInt(1, i);
           preparedStatement.setString(2, "testValue" + i);
           preparedStatement.addBatch();
       }
	return preparedStatement;
   }
   
   /**
    * CONJ-99: rewriteBatchedStatements parameter.
    * @throws SQLException
    */
   @Test
   public void rewriteBatchedStatementsUpdateTest() throws SQLException  {
	   connection.createStatement().execute("TRUNCATE t1");
       int cycles = 1000;
	   prepareStatementBatch(cycles).executeBatch();  // populate the table
       PreparedStatement preparedStatement = connection.prepareStatement("UPDATE t1 SET test = ? WHERE id = ?");
       for (int i = 0; i < cycles; i++) {
           preparedStatement.setString(1, "updated testValue" + i);
           preparedStatement.setInt(2, i);
           preparedStatement.addBatch();    
       }
       int[] updateCounts = preparedStatement.executeBatch();
       int totalUpdates = 0;
       for (int count=0; count<updateCounts.length; count++) {
    	   assertTrue(updateCounts[count] > 0);
    	   totalUpdates += updateCounts[count];
       }
       assertEquals(cycles, totalUpdates);
   }
   
}
