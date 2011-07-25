package org.skysql.jdbc;

import org.junit.Test;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static junit.framework.Assert.assertEquals;

/**
 * User: marcuse
 * Date: Feb 26, 2009
 * Time: 10:12:52 PM
 */
public class DatabaseMetadataTest {
    static { Logger.getLogger("").setLevel(Level.OFF); }
    private Connection connection;
    public DatabaseMetadataTest() throws ClassNotFoundException, SQLException {
        connection = DriverManager.getConnection("jdbc:drizzle://root@"+DriverTest.host+":3306/test");
    }
    @Test
    public void primaryKeysTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop table if exists pk_test");
        stmt.execute("create table pk_test (id1 int not null, id2 int not null, val varchar(20), primary key(id1, id2))");
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getPrimaryKeys(null,"test","pk_test");
        int i=0;
        while(rs.next()) {
            i++;
            assertEquals(null,rs.getString("table_cat"));
            assertEquals("test",rs.getString("table_schem"));
            assertEquals("pk_test",rs.getString("table_name"));
            assertEquals("id"+i,rs.getString("column_name"));
            assertEquals(i,rs.getShort("key_seq"));
        }
        assertEquals(2,i);
    }
    @Test
    public void exportedKeysTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");


        stmt.execute("create table prim_key (id int not null primary key, " +
                                            "val varchar(20))");
        stmt.execute("create table fore_key0 (id int not null primary key, " +
                                            "id_ref0 int, foreign key (id_ref0) references prim_key(id))");
        stmt.execute("create table fore_key1 (id int not null primary key, " +
                                            "id_ref1 int, foreign key (id_ref1) references prim_key(id) on update cascade)");


        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getExportedKeys("","test","prim_key");
        int i =0 ;
        while(rs.next()) {
            assertEquals("id",rs.getString("pkcolumn_name"));
            assertEquals("fore_key"+i,rs.getString("fktable_name"));
            assertEquals("id_ref"+i,rs.getString("fkcolumn_name"));
            i++;

        }
        assertEquals(2,i);
    }
    @Test
    public void importedKeysTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");

        stmt.execute("create table prim_key (id int not null primary key, " +
                                            "val varchar(20))");
        stmt.execute("create table fore_key0 (id int not null primary key, " +
                                            "id_ref0 int, foreign key (id_ref0) references prim_key(id))");
        stmt.execute("create table fore_key1 (id int not null primary key, " +
                                            "id_ref1 int, foreign key (id_ref1) references prim_key(id) on update cascade)");

        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getImportedKeys("","test","fore_key0");
        int i = 0;
        while(rs.next()) {
            assertEquals("id",rs.getString("pkcolumn_name"));
            assertEquals("prim_key",rs.getString("pktable_name"));
            i++;
        }
        assertEquals(1,i);
    }
    @Test
    public void testGetSchemas() throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getSchemas(null,"information_schema");
        assertEquals(true,rs.next());
        assertEquals("information_schema",rs.getString("table_schem").toLowerCase());
        assertEquals(false,rs.next());
        rs = dbmd.getSchemas(null,"test");
        assertEquals(true,rs.next());
        assertEquals("test",rs.getString("table_schem"));
        assertEquals(false,rs.next());
    }
    
    @Test
    public void testGetTables() throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getTables(null,null,"prim_key",null);
        assertEquals(true,rs.next());
        rs = dbmd.getTables(null,"test","prim_key",null);
        assertEquals(true,rs.next());
    }
    @Test
    public void testGetTables2() throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getTables(null,"information_schema","TABLE_PRIVILEGES",new String[]{"SYSTEM VIEW"});
        assertEquals(true, rs.next());
        assertEquals(false, rs.next());
        rs = dbmd.getTables(null,null,"TABLE_PRIVILEGES",new String[]{"TABLE"});
        assertEquals(false, rs.next());

    }
    @Test
    public void testGetColumns() throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getColumns(null,null,"t1",null);
        while(rs.next()){
            System.out.println(rs.getString(3));
        }
    }
    @Test
    public void testGetSchemas2() throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getSchemas();
        boolean foundTestUnitsJDBC = false;
        while(rs.next()) {
            if(rs.getString(1).equals("test"))
                foundTestUnitsJDBC=true;
        }
        assertEquals(true,foundTestUnitsJDBC);
    }
    @Test
    public void dbmetaTest() throws SQLException {
        DatabaseMetaData dmd = connection.getMetaData();
        dmd.getBestRowIdentifier(null,"test","t1",DatabaseMetaData.bestRowSession, true);
    }
}