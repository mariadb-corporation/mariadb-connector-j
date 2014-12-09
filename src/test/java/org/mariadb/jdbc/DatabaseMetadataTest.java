package org.mariadb.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class DatabaseMetadataTest extends BaseTest{
    static { Logger.getLogger("").setLevel(Level.OFF); }

    @Before
    public void checkSupported() throws SQLException {
        requireMinimumVersion(5,1);
    }

    @Test
    public void primaryKeysTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop table if exists pk_test");
        stmt.execute("create table pk_test (val varchar(20), id1 int not null, id2 int not null,primary key(id1, id2)) engine=innodb");
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getPrimaryKeys("test",null,"pk_test");
        int i=0;
        while(rs.next()) {
            i++;
            assertEquals("test",rs.getString("table_cat"));
            assertEquals(null,rs.getString("table_schem"));
            assertEquals("pk_test",rs.getString("table_name"));
            assertEquals("id"+i,rs.getString("column_name"));
            assertEquals(i,rs.getShort("key_seq"));
        }
        assertEquals(2,i);
    }

    @Test
    public void primaryKeyTest2() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop table if exists t2");
        stmt.execute("drop table if exists t1");
        stmt.execute("CREATE TABLE t1 ( id1 integer, constraint pk primary key(id1))");
        stmt.execute("CREATE TABLE t2 (id2a integer, id2b integer, constraint pk primary key(id2a, id2b), constraint fk1 foreign key(id2a) references t1(id1),  constraint fk2 foreign key(id2b) references t1(id1))");

        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getPrimaryKeys("test",null,"t2");
        int i=0;
        while(rs.next()) {
            i++;
            assertEquals("test",rs.getString("table_cat"));
            assertEquals(null,rs.getString("table_schem"));
            assertEquals("t2",rs.getString("table_name"));
            assertEquals(i,rs.getShort("key_seq"));
        }
        assertEquals(2,i);
        stmt.execute("drop table if exists t2");
        stmt.execute("drop table if exists t1");
    }
    @Test
    public void datetimeTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop table if exists datetime_test");
        stmt.execute("create table datetime_test (dt datetime)");
        ResultSet rs = stmt.executeQuery("select * from datetime_test");
        assertEquals(93,rs.getMetaData().getColumnType(1));

    }

    @Test 
    public void functionColumns() throws SQLException {
      Statement stmt = connection.createStatement();
      DatabaseMetaData md = connection.getMetaData();
      
      if (md.getDatabaseMajorVersion() < 5)
    	  return;
      if (md.getDatabaseMajorVersion() == 5 && md.getDatabaseMinorVersion() < 5)
    	  return;
      
      stmt.execute("DROP FUNCTION IF EXISTS hello");
      stmt.execute("CREATE FUNCTION hello (s CHAR(20), i int) RETURNS CHAR(50) DETERMINISTIC  RETURN CONCAT('Hello, ',s,'!')");
      ResultSet rs = connection.getMetaData().getFunctionColumns(null, null, "hello", null);
      
      rs.next();
      /* First row is for return value */
      assertEquals(rs.getString("FUNCTION_CAT"),connection.getCatalog());
      assertEquals(rs.getString("FUNCTION_SCHEM"), null);
      assertEquals(rs.getString("COLUMN_NAME"), null); /* No name, since it is return value */
      assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionReturn);
      assertEquals(rs.getInt("DATA_TYPE"), java.sql.Types.CHAR);
      assertEquals(rs.getString("TYPE_NAME"), "char");
      
      rs.next();
      assertEquals(rs.getString("COLUMN_NAME"), "s"); /* input parameter 's' (CHAR) */
      assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionColumnIn);
      assertEquals(rs.getInt("DATA_TYPE"), java.sql.Types.CHAR);
      assertEquals(rs.getString("TYPE_NAME"), "char");
      
      rs.next();
      assertEquals(rs.getString("COLUMN_NAME"), "i"); /* input parameter 'i' (INT) */
      assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionColumnIn);
      assertEquals(rs.getInt("DATA_TYPE"), java.sql.Types.INTEGER);
      assertEquals(rs.getString("TYPE_NAME"), "int");
    }


     /** Same as getImportedKeys, with one foreign key in a table in another catalog */
     @Test
     public void getImportedKeys() throws Exception{
           Statement st  = connection.createStatement();

           st.execute("DROP TABLE IF EXISTS product_order");
           st.execute("DROP TABLE IF EXISTS t1.product ");
           st.execute("DROP TABLE IF EXISTS `cus``tomer`");
           st.execute("DROP DATABASE IF EXISTS test1");

           st.execute("CREATE DATABASE IF NOT EXISTS t1");

           st.execute("CREATE TABLE t1.product (\n" +
                   "    category INT NOT NULL, id INT NOT NULL,\n" +
                   "    price DECIMAL,\n" +
                   "    PRIMARY KEY(category, id)\n" +
                   ")   ENGINE=INNODB");

           st.execute("CREATE TABLE `cus``tomer` (\n" +
                   "    id INT NOT NULL,\n" +
                   "    PRIMARY KEY (id)\n" +
                   ")   ENGINE=INNODB");

           st.execute("CREATE TABLE product_order (\n" +
                   "    no INT NOT NULL AUTO_INCREMENT,\n" +
                   "    product_category INT NOT NULL,\n" +
                   "    product_id INT NOT NULL,\n" +
                   "    customer_id INT NOT NULL,\n" +
                   "\n" +
                   "    PRIMARY KEY(no),\n" +
                   "    INDEX (product_category, product_id),\n" +
                   "    INDEX (customer_id),\n" +
                   "\n" +
                   "    FOREIGN KEY (product_category, product_id)\n" +
                   "      REFERENCES t1.product(category, id)\n" +
                   "      ON UPDATE CASCADE ON DELETE RESTRICT,\n" +
                   "\n" +
                   "    FOREIGN KEY (customer_id)\n" +
                   "      REFERENCES `cus``tomer`(id)\n" +
                   ")   ENGINE=INNODB;"
                   )   ;


           /*
            Test that I_S implementation is equivalent to parsing "show create table" .
             Get result sets using either method and compare (ignore minor differences INT vs SMALLINT
           */
           ResultSet rs1 = ((MySQLDatabaseMetaData)connection.getMetaData()).getImportedKeysUsingShowCreateTable("test", null, "product_order");
           ResultSet rs2 = ((MySQLDatabaseMetaData)connection.getMetaData()).getImportedKeysUsingInformationSchema("test", null, "product_order");
           assertEquals(rs1.getMetaData().getColumnCount(), rs2.getMetaData().getColumnCount());


           while(rs1.next()) {
               assertTrue(rs2.next());
               for (int i = 1; i <= rs1.getMetaData().getColumnCount(); i++) {
                   Object s1 = rs1.getObject(i);
                   Object s2 = rs2.getObject(i);
                   if (s1 instanceof  Number && s2 instanceof Number) {
                       assertEquals(((Number)s1).intValue(), ((Number)s2).intValue());
                   } else {
                       if (s1 != null && s2 != null && !s1.equals(s2)) {
                          assertTrue(false);
                       }
                       assertEquals(s1,s2);
                   }
               }
           }

           /* Also compare metadata */
           ResultSetMetaData md1 =  rs1.getMetaData();
           ResultSetMetaData md2 =  rs2.getMetaData();
           for (int i = 1; i <= md1.getColumnCount(); i++) {
               assertEquals(md1.getColumnLabel(i),md2.getColumnLabel(i));
           }
       }
    @Test
    public void exportedKeysTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");


        stmt.execute("create table prim_key (id int not null primary key, " +
                                            "val varchar(20)) engine=innodb");
        stmt.execute("create table fore_key0 (id int not null primary key, " +
                                            "id_ref0 int, foreign key (id_ref0) references prim_key(id)) engine=innodb");
        stmt.execute("create table fore_key1 (id int not null primary key, " +
                                            "id_ref1 int, foreign key (id_ref1) references prim_key(id) on update cascade) engine=innodb");


        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getExportedKeys("test",null,"prim_key");
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
                                            "val varchar(20)) engine=innodb");
        stmt.execute("create table fore_key0 (id int not null primary key, " +
                                            "id_ref0 int, foreign key (id_ref0) references prim_key(id)) engine=innodb");
        stmt.execute("create table fore_key1 (id int not null primary key, " +
                                            "id_ref1 int, foreign key (id_ref1) references prim_key(id) on update cascade) engine=innodb");

        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getImportedKeys(connection.getCatalog(),null,"fore_key0");
        int i = 0;
        while(rs.next()) {
            assertEquals("id",rs.getString("pkcolumn_name"));
            assertEquals("prim_key",rs.getString("pktable_name"));
            i++;
        }
        assertEquals(1,i);
    }
    @Test
    public void testGetCatalogs() throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        
        ResultSet rs = dbmd.getCatalogs();
        boolean haveMysql = false;
        boolean haveInformationSchema = false;
        while(rs.next()) {
        	String cat = rs.getString(1);
        	
        	if (cat.equalsIgnoreCase("mysql"))
        		haveMysql = true;
        	else if (cat.equalsIgnoreCase("information_schema"))
        		haveInformationSchema = true;
        }
        assertTrue(haveMysql);
        assertTrue(haveInformationSchema);
    }
    
    @Test
    public void testGetTables() throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getTables(null,null,"prim_key",null);
        assertEquals(true,rs.next());
        rs = dbmd.getTables("", null,"prim_key",null);
        assertEquals(true,rs.next());
    }
    @Test
    public void testGetTables2() throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = 
        		dbmd.getTables("information_schema",null,"TABLE_PRIVILEGES",new String[]{"SYSTEM VIEW"});
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
            // System.out.println(rs.getString(3));
            assertEquals("t1", rs.getString(3));
        }
    }
    
    void testResultSetColumns(ResultSet rs, String spec) throws SQLException {
   	 	ResultSetMetaData rsmd = rs.getMetaData();
   	 	String[] tokens   = spec.split(",");
   	 	
   	 	for(int i = 0; i < tokens.length; i++) {
   	 		String[] a = tokens[i].trim().split(" ");
   	 		String label = a[0];
   	 		String type = a[1];
   	 		
   	 		int col = i +1;
   	 		assertEquals(label,rsmd.getColumnLabel(col));
   	 		int t = rsmd.getColumnType(col);
   	 		if (type.equals("String")) {
   	 			assertTrue("invalid type  " + t + " for " + rsmd.getColumnLabel(col) + ",expected String",
   	 					t == java.sql.Types.VARCHAR || t == java.sql.Types.NULL || t == Types.LONGVARCHAR );
   	 		} else if (type.equals("int") || type.equals("short")) {
   	 			
   	 			assertTrue("invalid type  " + t + "( " + rsmd.getColumnTypeName(col) + " ) for " + rsmd.getColumnLabel(col) + ",expected numeric",
   	 					t == java.sql.Types.BIGINT || t == java.sql.Types.INTEGER ||
   	 					t == java.sql.Types.SMALLINT || t == java.sql.Types.TINYINT);
   	 			
   	 		} else if (type.equals("boolean")) {
   	 		    assertTrue("invalid type  " + t + "( " + rsmd.getColumnTypeName(col) + " ) for "  + rsmd.getColumnLabel(col) + ",expected boolean",
                    t == java.sql.Types.BOOLEAN || t == java.sql.Types.BIT);
   	 		    
   	 		} else if (type.equals("null")){
   	 		        assertTrue("invalid type  " + t + " for " + rsmd.getColumnLabel(col) + ",expected null",
   	 					t == java.sql.Types.NULL);	
   	 		} else {
   	 			assertTrue("invalid type '"+ type + "'", false);
   	 		}
   	 	}
    }
    
    @Test 
    public void getAttributesBasic()throws Exception {
    	 testResultSetColumns(
    		 connection.getMetaData().getAttributes(null, null, null, null),
			 "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String," 
			 +"ATTR_NAME String,DATA_TYPE int,ATTR_TYPE_NAME String,ATTR_SIZE int,DECIMAL_DIGITS int," 
			 +"NUM_PREC_RADIX int,NULLABLE int,REMARKS String,ATTR_DEF String,SQL_DATA_TYPE int,SQL_DATETIME_SUB int," 
			 +"CHAR_OCTET_LENGTH int,ORDINAL_POSITION int,IS_NULLABLE String,SCOPE_CATALOG String,SCOPE_SCHEMA String,"
			 +"SCOPE_TABLE String,SOURCE_DATA_TYPE short");
    }

    @Test
    public void identifierCaseSensitivity() throws Exception {
        connection.createStatement().execute("drop table if exists aB");
        connection.createStatement().execute("drop table if exists AB");
        if (connection.getMetaData().supportsMixedCaseIdentifiers()) {
            /* Case-sensitive identifier handling, we can create both t1 and T1 */
            connection.createStatement().execute("create table aB(i int)");
            connection.createStatement().execute("create table AB(i int)");
            /* Check there is an entry for both T1 and t1 in getTables */
            ResultSet rs = connection.getMetaData().getTables(null, null, "aB", null);
            assertTrue(rs.next());
            assertFalse(rs.next());
            rs = connection.getMetaData().getTables(null, null, "AB", null);
            assertTrue(rs.next());
            assertFalse(rs.next());
        }

        if (connection.getMetaData().storesMixedCaseIdentifiers()) {
            /* Case-insensitive, case-preserving */
            connection.createStatement().execute("create table aB(i int)");
            try {
                connection.createStatement().execute("create table AB(i int)");
                fail("should not get there, since names are case-insensitive");
            } catch (SQLException e) {
            }

            /* Check that table is stored case-preserving */
            ResultSet rs = connection.getMetaData().getTables(null, null, "aB%", null);
            while(rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                if (tableName.length() == 2) {
                   assertEquals("aB",tableName);
                }
            }

            rs = connection.getMetaData().getTables(null, null, "AB", null);
            assertTrue(rs.next());
            assertFalse(rs.next());
        }

        if (connection.getMetaData().storesLowerCaseIdentifiers()) {
            /* case-insensitive, identifiers converted to lowercase */
              /* Case-insensitive, case-preserving */
            connection.createStatement().execute("create table aB(i int)");
            try {
                connection.createStatement().execute("create table AB(i int)");
                fail("should not get there, since names are case-insensitive");
            } catch (SQLException e) {
            }

            /* Check that table is stored lowercase */
            ResultSet rs = connection.getMetaData().getTables(null, null, "aB%", null);
            while(rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                if (tableName.length() == 2) {
                   assertEquals("ab",tableName);
                }
            }

            rs = connection.getMetaData().getTables(null, null, "AB", null);
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
        assertFalse(connection.getMetaData().storesUpperCaseIdentifiers());
    }
    
    @Test
    public void getBestRowIdentifierBasic()throws SQLException {
    	testResultSetColumns(
    		connection.getMetaData().getBestRowIdentifier(null, null, "", 0, true), 
    		"SCOPE short,COLUMN_NAME String,DATA_TYPE int, TYPE_NAME String,"
    		+"COLUMN_SIZE int,BUFFER_LENGTH int,"
    		+"DECIMAL_DIGITS short,PSEUDO_COLUMN short"); 
    }
    
    @Test 
    public void getClientInfoPropertiesBasic() throws Exception {
    	testResultSetColumns(
    		connection.getMetaData().getClientInfoProperties(),
    		"NAME String, MAX_LEN int, DEFAULT_VALUE String, DESCRIPTION String");
    }

    @Test
    public void getCatalogsBasic()throws SQLException  {
    	testResultSetColumns(
    		connection.getMetaData().getCatalogs(),
    		"TABLE_CAT String");
    }
    
    
    @Test
    public void getColumnsBasic()throws SQLException {
    	testResultSetColumns(connection.getMetaData().getColumns(null, null, null, null),
			"TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,"  
			+"DATA_TYPE int,TYPE_NAME String,COLUMN_SIZE int,BUFFER_LENGTH int," 
			+"DECIMAL_DIGITS int,NUM_PREC_RADIX int,NULLABLE int," 
	        +"REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE int," 
			+"SQL_DATETIME_SUB int, CHAR_OCTET_LENGTH int," 
			+"ORDINAL_POSITION int,IS_NULLABLE String," 
			+"SCOPE_CATALOG String,SCOPE_SCHEMA String," 
			+"SCOPE_TABLE String,SOURCE_DATA_TYPE null");
    }
    
    @Test
    public void getProcedureColumnsBasic() throws SQLException {
    	testResultSetColumns(connection.getMetaData().getProcedureColumns(null, null, null, null),
		"PROCEDURE_CAT String,PROCEDURE_SCHEM String,PROCEDURE_NAME String,COLUMN_NAME String ,COLUMN_TYPE short," 
		+"DATA_TYPE int,TYPE_NAME String,PRECISION int,LENGTH int,SCALE short,RADIX short,NULLABLE short," 
		+"REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE int,SQL_DATETIME_SUB int ,CHAR_OCTET_LENGTH int,"
		+"ORDINAL_POSITION int,IS_NULLABLE String,SPECIFIC_NAME String");
    	
    }
    
    @Test
    public void getFunctionColumnsBasic() throws SQLException {
    	testResultSetColumns(connection.getMetaData().getFunctionColumns(null, null, null, null),
		 "FUNCTION_CAT String,FUNCTION_SCHEM String,FUNCTION_NAME String,COLUMN_NAME String,COLUMN_TYPE short,"
    	+"DATA_TYPE int,TYPE_NAME String,PRECISION int,LENGTH int,SCALE short,RADIX short,NULLABLE short,REMARKS String,"
	    +"CHAR_OCTET_LENGTH int,ORDINAL_POSITION int,IS_NULLABLE String,SPECIFIC_NAME String");
    	
    }
    
    @Test
    public void getColumnPrivilegesBasic()throws SQLException {
    	testResultSetColumns(
		 connection.getMetaData().getColumnPrivileges(null, null,"", null),
		 "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String," +
		 "GRANTOR String,GRANTEE String,PRIVILEGE String,IS_GRANTABLE String");
    }
    
    @Test
    public void getTablePrivilegesBasic()throws SQLException {
    	testResultSetColumns(
		 connection.getMetaData().getTablePrivileges(null, null, null),
		 "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,GRANTOR String," 
		 +"GRANTEE String,PRIVILEGE String,IS_GRANTABLE String");
    	
    }

    @Test 
    public void getVersionColumnsBasic()throws SQLException {
   	 	testResultSetColumns(
		 connection.getMetaData().getVersionColumns(null, null, null),
		 "SCOPE short, COLUMN_NAME String,DATA_TYPE int,TYPE_NAME String,"
		 +"COLUMN_SIZE int,BUFFER_LENGTH int,DECIMAL_DIGITS short,"
		 +"PSEUDO_COLUMN short");
    }
    @Test
    public void getPrimaryKeysBasic()throws SQLException {
   	 	testResultSetColumns(
		 connection.getMetaData().getPrimaryKeys(null, null, null),
		"TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,KEY_SEQ short,PK_NAME String"
		 ); 
    }
    @Test
    public void getImportedKeysBasic()throws SQLException {
    	testResultSetColumns(
   			 connection.getMetaData().getImportedKeys(null, null, ""),
        "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME String,FKTABLE_CAT String," 
    	+"FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME String,KEY_SEQ short,UPDATE_RULE short," 
    	+"DELETE_RULE short,FK_NAME String,PK_NAME String,DEFERRABILITY short");

    }
   
    
    @Test
    public void getExportedKeysBasic()throws SQLException {
    	testResultSetColumns(
   			 connection.getMetaData().getExportedKeys(null, null, ""),
        "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME String,FKTABLE_CAT String," 
    	+"FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME String,KEY_SEQ short,UPDATE_RULE short," 
    	+"DELETE_RULE short,FK_NAME String,PK_NAME String,DEFERRABILITY short");

    }
    
    @Test 
    public void getCrossReferenceBasic()throws SQLException {
    	testResultSetColumns(
        connection.getMetaData().getCrossReference(null, null, "", null, null, ""),
        "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME String,FKTABLE_CAT String," 
       	+"FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME String,KEY_SEQ short,UPDATE_RULE short," 
       	+"DELETE_RULE short,FK_NAME String,PK_NAME String,DEFERRABILITY short");
    }
    
    @Test 
    public void getUDTsBasic() throws SQLException {
    	testResultSetColumns(
    	connection.getMetaData().getUDTs(null, null, null, null),
	    "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,CLASS_NAME String,DATA_TYPE int,"
    	+"REMARKS String,BASE_TYPE short" );
    }
    
    @Test
    public void getSuperTypesBasic() throws SQLException {
    	testResultSetColumns(
    	connection.getMetaData().getSuperTypes(null, null, null),
    	"TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,SUPERTYPE_CAT String," 
    	+"SUPERTYPE_SCHEM String,SUPERTYPE_NAME String");
    }
    
    @Test
    public void getFunctionsBasic() throws SQLException {
    	testResultSetColumns(
    	connection.getMetaData().getFunctions(null, null, null),
    	"FUNCTION_CAT String, FUNCTION_SCHEM String,FUNCTION_NAME String,REMARKS String,FUNCTION_TYPE short, "
    	+"SPECIFIC_NAME String");
    }
    
    
    @Test 
    public void getSuperTablesBasic() throws SQLException {
    	testResultSetColumns(
    	connection.getMetaData().getSuperTables(null, null, null),
    	"TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String, SUPERTABLE_NAME String") ;
    }
    
    @Test
    public void testGetSchemas2() throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet rs = dbmd.getCatalogs();
        boolean foundTestUnitsJDBC = false;
        while(rs.next()) {
            if(rs.getString(1).equals("test"))
                foundTestUnitsJDBC=true;
        }
        assertEquals(true,foundTestUnitsJDBC);
    }
    

    /* Verify default behavior for nullCatalogMeansCurrent (=true) */
    @Test 
    public void nullCatalogMeansCurrent() throws Exception {
        String catalog = connection.getCatalog();
        ResultSet rs = connection.getMetaData().getColumns(null, null, null, null);
        while(rs.next()) {
            assertTrue(rs.getString("TABLE_CAT").equalsIgnoreCase(catalog));
        }
    }
    
    /* Verify that "nullCatalogMeansCurrent=false" works (i.e information_schema columns are returned)*/
    @Test 
    public void nullCatalogMeansCurrent2() throws Exception {
        setConnection("&nullCatalogMeansCurrent=false");
        boolean haveInformationSchema = false;
        try {
            ResultSet rs = connection.getMetaData().getColumns(null, null, null, null);
            while(rs.next()) {
                if (rs.getString("TABLE_CAT").equalsIgnoreCase("information_schema")) {
                    haveInformationSchema = true;
                    break;
                }
            }
        } finally {
        }
        assertTrue(haveInformationSchema);
    }
 
    @Test 
    public void testGetTypeInfoBasic() throws SQLException {
        testResultSetColumns(
        connection.getMetaData().getTypeInfo(),
        "TYPE_NAME String,DATA_TYPE int,PRECISION int,LITERAL_PREFIX String," 
        + "LITERAL_SUFFIX String,CREATE_PARAMS String, NULLABLE short,CASE_SENSITIVE boolean," 
        + "SEARCHABLE short,UNSIGNED_ATTRIBUTE boolean,FIXED_PREC_SCALE boolean, AUTO_INCREMENT boolean,"
        + "LOCAL_TYPE_NAME String,MINIMUM_SCALE short,MAXIMUM_SCALE short,SQL_DATA_TYPE int,SQL_DATETIME_SUB int,"
        + "NUM_PREC_RADIX int");   
    }

    static void checkType(String name, int actualType, String colName, int expectedType)
    {
       if (name.equals(colName))
           assertEquals(actualType, expectedType);
    }
    @Test
     public void getColumnsTest() throws SQLException {
        connection.createStatement().execute(
                        "CREATE TABLE  IF NOT EXISTS `manycols` (\n" +
                        "  `tiny` tinyint(4) DEFAULT NULL,\n" +
                        "  `tiny_uns` tinyint(3) unsigned DEFAULT NULL,\n" +
                        "  `small` smallint(6) DEFAULT NULL,\n" +
                        "  `small_uns` smallint(5) unsigned DEFAULT NULL,\n" +
                        "  `medium` mediumint(9) DEFAULT NULL,\n" +
                        "  `medium_uns` mediumint(8) unsigned DEFAULT NULL,\n" +
                        "  `int_col` int(11) DEFAULT NULL,\n" +
                        "  `int_col_uns` int(10) unsigned DEFAULT NULL,\n" +
                        "  `big` bigint(20) DEFAULT NULL,\n" +
                        "  `big_uns` bigint(20) unsigned DEFAULT NULL,\n" +
                        "  `decimal_col` decimal(10,5) DEFAULT NULL,\n" +
                        "  `fcol` float DEFAULT NULL,\n" +
                        "  `fcol_uns` float unsigned DEFAULT NULL,\n" +
                        "  `dcol` double DEFAULT NULL,\n" +
                        "  `dcol_uns` double unsigned DEFAULT NULL,\n" +
                        "  `date_col` date DEFAULT NULL,\n" +
                        "  `time_col` time DEFAULT NULL,\n" +
                        "  `timestamp_col` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE\n" +
                        "CURRENT_TIMESTAMP,\n" +
                        "  `year_col` year(4) DEFAULT NULL,\n" +
                        "  `bit_col` bit(5) DEFAULT NULL,\n" +
                        "  `char_col` char(5) DEFAULT NULL,\n" +
                        "  `varchar_col` varchar(10) DEFAULT NULL,\n" +
                        "  `binary_col` binary(10) DEFAULT NULL,\n" +
                        "  `varbinary_col` varbinary(10) DEFAULT NULL,\n" +
                        "  `tinyblob_col` tinyblob,\n" +
                        "  `blob_col` blob,\n" +
                        "  `mediumblob_col` mediumblob,\n" +
                        "  `longblob_col` longblob,\n" +
                        "  `text_col` text,\n" +
                        "  `mediumtext_col` mediumtext,\n" +
                        "  `longtext_col` longtext\n" +
                        ")"
        );
        DatabaseMetaData dmd = connection.getMetaData();
        ResultSet rs = dmd.getColumns(connection.getCatalog(), null, "manycols", null);
        while(rs.next()) {
            String columnName = rs.getString("column_name");
            int type = rs.getInt("data_type");
            String typeName = rs.getString("type_name");
            assertTrue(typeName.indexOf("(") == -1);
            for(char c : typeName.toCharArray()) {
            	assertTrue("bad typename " + typeName, c == ' ' || Character.isUpperCase(c));
            }
            checkType(columnName, type, "tiny", Types.TINYINT);
            checkType(columnName, type, "tiny_uns", Types.TINYINT);
            checkType(columnName, type, "small", Types.SMALLINT);
            checkType(columnName, type, "small_uns", Types.SMALLINT);
            checkType(columnName, type, "medium", Types.INTEGER);
            checkType(columnName, type, "medium_uns", Types.INTEGER);
            checkType(columnName, type, "int_col", Types.INTEGER);
            checkType(columnName, type, "int_col_uns", Types.INTEGER);
            checkType(columnName, type, "big", Types.BIGINT);
            checkType(columnName, type, "big_uns", Types.BIGINT);
            checkType(columnName, type, "decimal_col",Types.DECIMAL);
            checkType(columnName, type, "fcol", Types.REAL);
            checkType(columnName, type, "fcol_uns", Types.REAL);
            checkType(columnName, type, "dcol", Types.DOUBLE);
            checkType(columnName, type, "dcol_uns", Types.DOUBLE);
            checkType(columnName, type, "date_col", Types.DATE);
            checkType(columnName, type, "time_col", Types.TIME);
            checkType(columnName, type, "timestamp_col", Types.TIMESTAMP);
            checkType(columnName, type, "year_col", Types.DATE);
            checkType(columnName, type, "bit_col", Types.BIT);
            checkType(columnName, type, "char_col", Types.CHAR);
            checkType(columnName, type, "varchar_col", Types.VARCHAR);
            checkType(columnName, type, "binary_col", Types.BINARY);
            checkType(columnName, type, "tinyblob_col", Types.LONGVARBINARY);
            checkType(columnName, type, "blob_col", Types.LONGVARBINARY);
            checkType(columnName, type, "longblob_col", Types.LONGVARBINARY);
            checkType(columnName, type, "mediumblob_col", Types.LONGVARBINARY);
            checkType(columnName, type, "text_col", Types.LONGVARCHAR);
            checkType(columnName, type, "mediumtext_col", Types.LONGVARCHAR);
            checkType(columnName, type, "longtext_col", Types.LONGVARCHAR);
        }
    }

    @Test
    public void yearIsShortType() throws Exception {
        setConnection("&yearIsDateType=false");
        try {
            connection.createStatement().execute("CREATE TABLE  IF NOT EXISTS ytab (y year)");
            connection.createStatement().execute("insert into ytab values(72)");
            ResultSet rs = connection.getMetaData().getColumns(connection.getCatalog(), null, "ytab", null);
            assertTrue(rs.next());
            assertEquals(rs.getInt("DATA_TYPE"),Types.SMALLINT);
            ResultSet rs1 = connection.createStatement().executeQuery("select * from ytab");
            assertEquals(rs1.getMetaData().getColumnType(1), Types.SMALLINT);
            assertTrue(rs1.next());
            assertTrue(rs1.getObject(1) instanceof Short);
            assertEquals(rs1.getShort(1), 1972);
        } finally {
        }
    }

    /* CONJ-15 */
    @Test
    public void maxCharLengthUTF8() throws Exception {
         connection.createStatement().execute("CREATE TABLE  IF NOT EXISTS maxcharlength (maxcharlength char(1)) character set utf8");
         DatabaseMetaData dmd = connection.getMetaData();
         ResultSet rs = dmd.getColumns(connection.getCatalog(), null, "maxcharlength", null);
         assertTrue(rs.next());
         assertEquals(rs.getInt("COLUMN_SIZE"),1);
    }
    @Test
    public void conj72() throws Exception {
        setConnection("&tinyInt1isBit=true");
        try {
            connection.createStatement().execute("CREATE TABLE  IF NOT EXISTS conj72 (t tinyint(1))");
            connection.createStatement().execute("insert into conj72 values(1)");
            ResultSet rs = connection.getMetaData().getColumns(connection.getCatalog(), null, "conj72", null);
            assertTrue(rs.next());
            assertEquals(rs.getInt("DATA_TYPE"),Types.BIT);
            ResultSet rs1 = connection.createStatement().executeQuery("select * from conj72");
            assertEquals(rs1.getMetaData().getColumnType(1), Types.BIT);
        } finally {
        }
    }
}
