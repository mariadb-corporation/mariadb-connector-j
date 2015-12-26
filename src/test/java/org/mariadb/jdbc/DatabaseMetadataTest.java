package org.mariadb.jdbc;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;


public class DatabaseMetadataTest extends BaseTest {
    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("dbpk_test", "val varchar(20), id1 int not null, id2 int not null,primary key(id1, id2)",
                "engine=innodb");
        createTable("datetime_test", "dt datetime");
        createTable("`manycols`",
                "  `tiny` tinyint(4) DEFAULT NULL,"
                        + "  `tiny_uns` tinyint(3) unsigned DEFAULT NULL,"
                        + "  `small` smallint(6) DEFAULT NULL,"
                        + "  `small_uns` smallint(5) unsigned DEFAULT NULL,"
                        + "  `medium` mediumint(9) DEFAULT NULL,"
                        + "  `medium_uns` mediumint(8) unsigned DEFAULT NULL,"
                        + "  `int_col` int(11) DEFAULT NULL,"
                        + "  `int_col_uns` int(10) unsigned DEFAULT NULL,"
                        + "  `big` bigint(20) DEFAULT NULL,"
                        + "  `big_uns` bigint(20) unsigned DEFAULT NULL,"
                        + "  `decimal_col` decimal(10,5) DEFAULT NULL,"
                        + "  `fcol` float DEFAULT NULL,"
                        + "  `fcol_uns` float unsigned DEFAULT NULL,"
                        + "  `dcol` double DEFAULT NULL,"
                        + "  `dcol_uns` double unsigned DEFAULT NULL,"
                        + "  `date_col` date DEFAULT NULL,"
                        + "  `time_col` time DEFAULT NULL,"
                        + "  `timestamp_col` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                        + "  `year_col` year(4) DEFAULT NULL,"
                        + "  `bit_col` bit(5) DEFAULT NULL,"
                        + "  `char_col` char(5) DEFAULT NULL,"
                        + "  `varchar_col` varchar(10) DEFAULT NULL,"
                        + "  `binary_col` binary(10) DEFAULT NULL,"
                        + "  `varbinary_col` varbinary(10) DEFAULT NULL,"
                        + "  `tinyblob_col` tinyblob,"
                        + "  `blob_col` blob,"
                        + "  `mediumblob_col` mediumblob,"
                        + "  `longblob_col` longblob,"
                        + "  `text_col` text,"
                        + "  `mediumtext_col` mediumtext,"
                        + "  `longtext_col` longtext"
        );
        createTable("ytab", "y year");
        createTable("maxcharlength", "maxcharlength char(1)", "character set utf8");
        createTable("conj72", "t tinyint(1)");
    }

    private static void checkType(String name, int actualType, String colName, int expectedType) {
        if (name.equals(colName)) {
            assertEquals(actualType, expectedType);
        }
    }

    @Before
    public void checkSupported() throws SQLException {
        requireMinimumVersion(5, 1);
    }


    @Test
    public void primaryKeysTest() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getPrimaryKeys("testj", null, "dbpk_test");
        int counter = 0;
        while (rs.next()) {
            counter++;
            assertEquals("testj", rs.getString("table_cat"));
            assertEquals(null, rs.getString("table_schem"));
            assertEquals("dbpk_test", rs.getString("table_name"));
            assertEquals("id" + counter, rs.getString("column_name"));
            assertEquals(counter, rs.getShort("key_seq"));
        }
        assertEquals(2, counter);
    }

    @Test
    public void primaryKeyTest2() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop table if exists t2");
        stmt.execute("drop table if exists t1");
        stmt.execute("CREATE TABLE t1 ( id1 integer, constraint pk primary key(id1))");
        stmt.execute("CREATE TABLE t2 (id2a integer, id2b integer, constraint pk primary key(id2a, id2b), "
                + "constraint fk1 foreign key(id2a) references t1(id1),  constraint fk2 foreign key(id2b) "
                + "references t1(id1))");

        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getPrimaryKeys("testj", null, "t2");
        int counter = 0;
        while (rs.next()) {
            counter++;
            assertEquals("testj", rs.getString("table_cat"));
            assertEquals(null, rs.getString("table_schem"));
            assertEquals("t2", rs.getString("table_name"));
            assertEquals(counter, rs.getShort("key_seq"));
        }
        assertEquals(2, counter);
        stmt.execute("drop table if exists t2");
        stmt.execute("drop table if exists t1");
    }

    @Test
    public void datetimeTest() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from datetime_test");
        assertEquals(93, rs.getMetaData().getColumnType(1));

    }

    @Test
    public void functionColumns() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        DatabaseMetaData md = sharedConnection.getMetaData();

        if (md.getDatabaseMajorVersion() < 5) {
            return;
        } else if (md.getDatabaseMajorVersion() == 5 && md.getDatabaseMinorVersion() < 5) {
            return;
        }

        stmt.execute("DROP FUNCTION IF EXISTS hello");
        stmt.execute("CREATE FUNCTION hello (s CHAR(20), i int) RETURNS CHAR(50) DETERMINISTIC  "
                + "RETURN CONCAT('Hello, ',s,'!')");
        ResultSet rs = sharedConnection.getMetaData().getFunctionColumns(null, null, "hello", null);

        rs.next();
      /* First row is for return value */
        assertEquals(rs.getString("FUNCTION_CAT"), sharedConnection.getCatalog());
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
        stmt.execute("DROP FUNCTION IF EXISTS hello");
    }


    /**
     * Same as getImportedKeys, with one foreign key in a table in another catalog.
     */
    @Test
    public void getImportedKeys() throws Exception {
        Statement st = sharedConnection.createStatement();

        st.execute("DROP TABLE IF EXISTS product_order");
        st.execute("DROP TABLE IF EXISTS t1.product ");
        st.execute("DROP TABLE IF EXISTS `cus``tomer`");
        st.execute("DROP DATABASE IF EXISTS test1");

        st.execute("CREATE DATABASE IF NOT EXISTS t1");

        st.execute("CREATE TABLE t1.product ( category INT NOT NULL, id INT NOT NULL, price DECIMAL,"
                + " PRIMARY KEY(category, id) )   ENGINE=INNODB");

        st.execute("CREATE TABLE `cus``tomer` (id INT NOT NULL, PRIMARY KEY (id))   ENGINE=INNODB");

        st.execute("CREATE TABLE product_order (\n"
                        + "    no INT NOT NULL AUTO_INCREMENT,\n"
                        + "    product_category INT NOT NULL,\n"
                        + "    product_id INT NOT NULL,\n"
                        + "    customer_id INT NOT NULL,\n"
                        + "    PRIMARY KEY(no),\n"
                        + "    INDEX (product_category, product_id),\n"
                        + "    INDEX (customer_id),\n"
                        + "    FOREIGN KEY (product_category, product_id)\n"
                        + "      REFERENCES t1.product(category, id)\n"
                        + "      ON UPDATE CASCADE ON DELETE RESTRICT,\n"
                        + "    FOREIGN KEY (customer_id)\n"
                        + "      REFERENCES `cus``tomer`(id)\n"
                        + ")   ENGINE=INNODB;"
        );


           /*
            Test that I_S implementation is equivalent to parsing "show create table" .
             Get result sets using either method and compare (ignore minor differences INT vs SMALLINT
           */
        ResultSet rs1 = ((MariaDbDatabaseMetaData) sharedConnection.getMetaData())
                .getImportedKeysUsingShowCreateTable("testj", null, "product_order");
        ResultSet rs2 = ((MariaDbDatabaseMetaData) sharedConnection.getMetaData())
                .getImportedKeysUsingInformationSchema("testj", null, "product_order");
        assertEquals(rs1.getMetaData().getColumnCount(), rs2.getMetaData().getColumnCount());


        while (rs1.next()) {
            assertTrue(rs2.next());
            for (int i = 1; i <= rs1.getMetaData().getColumnCount(); i++) {
                Object s1 = rs1.getObject(i);
                Object s2 = rs2.getObject(i);
                if (s1 instanceof Number && s2 instanceof Number) {
                    assertEquals(((Number) s1).intValue(), ((Number) s2).intValue());
                } else {
                    if (s1 != null && s2 != null && !s1.equals(s2)) {
                        assertTrue(false);
                    }
                    assertEquals(s1, s2);
                }
            }
        }

           /* Also compare metadata */
        ResultSetMetaData md1 = rs1.getMetaData();
        ResultSetMetaData md2 = rs2.getMetaData();
        for (int i = 1; i <= md1.getColumnCount(); i++) {
            assertEquals(md1.getColumnLabel(i), md2.getColumnLabel(i));
        }
        st.execute("DROP TABLE IF EXISTS product_order");
        st.execute("DROP TABLE IF EXISTS t1.product ");
        st.execute("DROP TABLE IF EXISTS `cus``tomer`");
        st.execute("DROP DATABASE IF EXISTS test1");
    }

    @Test
    public void exportedKeysTest() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");


        stmt.execute("create table prim_key (id int not null primary key, "
                + "val varchar(20)) engine=innodb");
        stmt.execute("create table fore_key0 (id int not null primary key, "
                + "id_ref0 int, foreign key (id_ref0) references prim_key(id)) engine=innodb");
        stmt.execute("create table fore_key1 (id int not null primary key, "
                + "id_ref1 int, foreign key (id_ref1) references prim_key(id) on update cascade) engine=innodb");


        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getExportedKeys("testj", null, "prim_key");
        int counter = 0;
        while (rs.next()) {
            assertEquals("id", rs.getString("pkcolumn_name"));
            assertEquals("fore_key" + counter, rs.getString("fktable_name"));
            assertEquals("id_ref" + counter, rs.getString("fkcolumn_name"));
            counter++;
        }
        assertEquals(2, counter);
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");
    }

    @Test
    public void importedKeysTest() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");

        stmt.execute("create table prim_key (id int not null primary key, "
                + "val varchar(20)) engine=innodb");
        stmt.execute("create table fore_key0 (id int not null primary key, "
                + "id_ref0 int, foreign key (id_ref0) references prim_key(id)) engine=innodb");
        stmt.execute("create table fore_key1 (id int not null primary key, "
                + "id_ref1 int, foreign key (id_ref1) references prim_key(id) on update cascade) engine=innodb");

        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getImportedKeys(sharedConnection.getCatalog(), null, "fore_key0");
        int counter = 0;
        while (rs.next()) {
            assertEquals("id", rs.getString("pkcolumn_name"));
            assertEquals("prim_key", rs.getString("pktable_name"));
            counter++;
        }
        assertEquals(1, counter);
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");
    }

    @Test
    public void testGetCatalogs() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();

        ResultSet rs = dbmd.getCatalogs();
        boolean haveMysql = false;
        boolean haveInformationSchema = false;
        while (rs.next()) {
            String cat = rs.getString(1);

            if (cat.equalsIgnoreCase("mysql")) {
                haveMysql = true;
            } else if (cat.equalsIgnoreCase("information_schema")) {
                haveInformationSchema = true;
            }
        }
        assertTrue(haveMysql);
        assertTrue(haveInformationSchema);
    }

    @Test
    public void testGetTables() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");


        stmt.execute("create table prim_key (id int not null primary key, "
                + "val varchar(20)) engine=innodb");
        stmt.execute("create table fore_key0 (id int not null primary key, "
                + "id_ref0 int, foreign key (id_ref0) references prim_key(id)) engine=innodb");
        stmt.execute("create table fore_key1 (id int not null primary key, "
                + "id_ref1 int, foreign key (id_ref1) references prim_key(id) on update cascade) engine=innodb");

        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getTables(null, null, "prim_key", null);

        assertEquals(true, rs.next());
        rs = dbmd.getTables("", null, "prim_key", null);
        assertEquals(true, rs.next());
    }

    @Test
    public void testGetTables2() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs =
                dbmd.getTables("information_schema", null, "TABLE_PRIVILEGES", new String[]{"SYSTEM VIEW"});
        assertEquals(true, rs.next());
        assertEquals(false, rs.next());
        rs = dbmd.getTables(null, null, "TABLE_PRIVILEGES", new String[]{"TABLE"});
        assertEquals(false, rs.next());

    }

    @Test
    public void testGetColumns() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getColumns(null, null, "t1", null);
        while (rs.next()) {
            // System.out.println(rs.getString(3));
            assertEquals("t1", rs.getString(3));
        }
    }

    void testResultSetColumns(ResultSet rs, String spec) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        String[] tokens = spec.split(",");

        for (int i = 0; i < tokens.length; i++) {
            String[] splitTokens = tokens[i].trim().split(" ");
            String label = splitTokens[0];
            String type = splitTokens[1];

            int col = i + 1;
            assertEquals(label, rsmd.getColumnLabel(col));
            int columnType = rsmd.getColumnType(col);
            if (type.equals("String")) {
                assertTrue("invalid type  " + columnType + " for " + rsmd.getColumnLabel(col) + ",expected String",
                        columnType == java.sql.Types.VARCHAR
                                || columnType == java.sql.Types.NULL
                                || columnType == Types.LONGVARCHAR);
            } else if ("int".equals(type) || "short".equals(type)) {

                assertTrue("invalid type  " + columnType + "( " + rsmd.getColumnTypeName(col) + " ) for "
                                + rsmd.getColumnLabel(col) + ",expected numeric",
                        columnType == java.sql.Types.BIGINT
                                || columnType == java.sql.Types.INTEGER
                                || columnType == java.sql.Types.SMALLINT
                                || columnType == java.sql.Types.TINYINT);

            } else if (type.equals("boolean")) {
                assertTrue("invalid type  " + columnType + "( " + rsmd.getColumnTypeName(col) + " ) for "
                                + rsmd.getColumnLabel(col) + ",expected boolean",
                        columnType == java.sql.Types.BOOLEAN || columnType == java.sql.Types.BIT);

            } else if (type.equals("null")) {
                assertTrue("invalid type  " + columnType + " for " + rsmd.getColumnLabel(col) + ",expected null",
                        columnType == java.sql.Types.NULL);
            } else {
                assertTrue("invalid type '" + type + "'", false);
            }
        }
    }

    @Test
    public void getAttributesBasic() throws Exception {
        testResultSetColumns(
                sharedConnection.getMetaData().getAttributes(null, null, null, null),
                "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,"
                        + "ATTR_NAME String,DATA_TYPE int,ATTR_TYPE_NAME String,ATTR_SIZE int,DECIMAL_DIGITS int,"
                        + "NUM_PREC_RADIX int,NULLABLE int,REMARKS String,ATTR_DEF String,SQL_DATA_TYPE int,"
                        + "SQL_DATETIME_SUB int, CHAR_OCTET_LENGTH int,ORDINAL_POSITION int,IS_NULLABLE String,"
                        + "SCOPE_CATALOG String,SCOPE_SCHEMA String,"
                        + "SCOPE_TABLE String,SOURCE_DATA_TYPE short");
    }

    @Test
    public void identifierCaseSensitivity() throws Exception {
        if (sharedConnection.getMetaData().supportsMixedCaseIdentifiers()) {
            /* Case-sensitive identifier handling, we can create both t1 and T1 */
            createTable("aB", "i int");
            createTable("AB", "i int");
            /* Check there is an entry for both T1 and t1 in getTables */
            ResultSet rs = sharedConnection.getMetaData().getTables(null, null, "aB", null);
            assertTrue(rs.next());
            assertFalse(rs.next());
            rs = sharedConnection.getMetaData().getTables(null, null, "AB", null);
            assertTrue(rs.next());
            assertFalse(rs.next());
        }

        if (sharedConnection.getMetaData().storesMixedCaseIdentifiers()) {
            /* Case-insensitive, case-preserving */
            createTable("aB", "i int");
            try {
                createTable("AB", "i int");
                fail("should not get there, since names are case-insensitive");
            } catch (SQLException e) {
                //normal error
            }

            /* Check that table is stored case-preserving */
            ResultSet rs = sharedConnection.getMetaData().getTables(null, null, "aB%", null);
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                if (tableName.length() == 2) {
                    assertEquals("aB", tableName);
                }
            }

            rs = sharedConnection.getMetaData().getTables(null, null, "AB", null);
            assertTrue(rs.next());
            assertFalse(rs.next());
        }

        if (sharedConnection.getMetaData().storesLowerCaseIdentifiers()) {
            /* case-insensitive, identifiers converted to lowercase */
              /* Case-insensitive, case-preserving */
            createTable("aB", "i int");
            try {
                sharedConnection.createStatement().execute("create table AB(i int)");
                fail("should not get there, since names are case-insensitive");
            } catch (SQLException e) {
                //normal error
            }

            /* Check that table is stored lowercase */
            ResultSet rs = sharedConnection.getMetaData().getTables(null, null, "aB%", null);
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                if (tableName.length() == 2) {
                    assertEquals("ab", tableName);
                }
            }

            rs = sharedConnection.getMetaData().getTables(null, null, "AB", null);
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
        assertFalse(sharedConnection.getMetaData().storesUpperCaseIdentifiers());
    }

    @Test
    public void getBestRowIdentifierBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getBestRowIdentifier(null, null, "", 0, true),
                "SCOPE short,COLUMN_NAME String,DATA_TYPE int, TYPE_NAME String,"
                        + "COLUMN_SIZE int,BUFFER_LENGTH int,"
                        + "DECIMAL_DIGITS short,PSEUDO_COLUMN short");
    }

    @Test
    public void getClientInfoPropertiesBasic() throws Exception {
        testResultSetColumns(
                sharedConnection.getMetaData().getClientInfoProperties(),
                "NAME String, MAX_LEN int, DEFAULT_VALUE String, DESCRIPTION String");
    }

    @Test
    public void getCatalogsBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getCatalogs(),
                "TABLE_CAT String");
    }

    @Test
    public void getColumnsBasic() throws SQLException {
        cancelForVersion(10, 1, 8); //due to server error MDEV-8984
        cancelForVersion(10, 1, 9);
        cancelForVersion(10, 1, 10);
        testResultSetColumns(sharedConnection.getMetaData().getColumns(null, null, null, null),
                "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,"
                        + "DATA_TYPE int,TYPE_NAME String,COLUMN_SIZE int,BUFFER_LENGTH int,"
                        + "DECIMAL_DIGITS int,NUM_PREC_RADIX int,NULLABLE int,"
                        + "REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE int,"
                        + "SQL_DATETIME_SUB int, CHAR_OCTET_LENGTH int,"
                        + "ORDINAL_POSITION int,IS_NULLABLE String,"
                        + "SCOPE_CATALOG String,SCOPE_SCHEMA String,"
                        + "SCOPE_TABLE String,SOURCE_DATA_TYPE null");
    }

    @Test
    public void getProcedureColumnsBasic() throws SQLException {
        testResultSetColumns(sharedConnection.getMetaData().getProcedureColumns(null, null, null, null),
                "PROCEDURE_CAT String,PROCEDURE_SCHEM String,PROCEDURE_NAME String,COLUMN_NAME String ,"
                        + "COLUMN_TYPE short,DATA_TYPE int,TYPE_NAME String,PRECISION int,LENGTH int,SCALE short,"
                        + "RADIX short,NULLABLE short,REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE int,"
                        + "SQL_DATETIME_SUB int ,CHAR_OCTET_LENGTH int,"
                        + "ORDINAL_POSITION int,IS_NULLABLE String,SPECIFIC_NAME String");

    }

    @Test
    public void getFunctionColumnsBasic() throws SQLException {
        testResultSetColumns(sharedConnection.getMetaData().getFunctionColumns(null, null, null, null),
                "FUNCTION_CAT String,FUNCTION_SCHEM String,FUNCTION_NAME String,COLUMN_NAME String,COLUMN_TYPE short,"
                        + "DATA_TYPE int,TYPE_NAME String,PRECISION int,LENGTH int,SCALE short,RADIX short,"
                        + "NULLABLE short,REMARKS String,CHAR_OCTET_LENGTH int,ORDINAL_POSITION int,"
                        + "IS_NULLABLE String,SPECIFIC_NAME String");

    }

    @Test
    public void getColumnPrivilegesBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getColumnPrivileges(null, null, "", null),
                "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,"
                        + "GRANTOR String,GRANTEE String,PRIVILEGE String,IS_GRANTABLE String");
    }

    @Test
    public void getTablePrivilegesBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getTablePrivileges(null, null, null),
                "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,GRANTOR String,"
                        + "GRANTEE String,PRIVILEGE String,IS_GRANTABLE String");

    }

    @Test
    public void getVersionColumnsBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getVersionColumns(null, null, null),
                "SCOPE short, COLUMN_NAME String,DATA_TYPE int,TYPE_NAME String,"
                        + "COLUMN_SIZE int,BUFFER_LENGTH int,DECIMAL_DIGITS short,"
                        + "PSEUDO_COLUMN short");
    }

    @Test
    public void getPrimaryKeysBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getPrimaryKeys(null, null, null),
                "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,KEY_SEQ short,PK_NAME String"
        );
    }

    @Test
    public void getImportedKeysBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getImportedKeys(null, null, ""),
                "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME String,FKTABLE_CAT String,"
                        + "FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME String,KEY_SEQ short,"
                        + "UPDATE_RULE short,DELETE_RULE short,FK_NAME String,PK_NAME String,DEFERRABILITY short");

    }

    @Test
    public void getExportedKeysBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getExportedKeys(null, null, ""),
                "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME String,FKTABLE_CAT String,"
                        + "FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME String,KEY_SEQ short,"
                        + "UPDATE_RULE short, DELETE_RULE short,FK_NAME String,PK_NAME String,DEFERRABILITY short");

    }

    @Test
    public void getCrossReferenceBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getCrossReference(null, null, "", null, null, ""),
                "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME String,FKTABLE_CAT String,"
                        + "FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME String,KEY_SEQ short,"
                        + "UPDATE_RULE short,DELETE_RULE short,FK_NAME String,PK_NAME String,DEFERRABILITY short");
    }

    @Test
    public void getUdtsBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getUDTs(null, null, null, null),
                "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,CLASS_NAME String,DATA_TYPE int,"
                        + "REMARKS String,BASE_TYPE short");
    }

    @Test
    public void getSuperTypesBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getSuperTypes(null, null, null),
                "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,SUPERTYPE_CAT String,"
                        + "SUPERTYPE_SCHEM String,SUPERTYPE_NAME String");
    }

    @Test
    public void getFunctionsBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getFunctions(null, null, null),
                "FUNCTION_CAT String, FUNCTION_SCHEM String,FUNCTION_NAME String,REMARKS String,FUNCTION_TYPE short, "
                        + "SPECIFIC_NAME String");
    }

    @Test
    public void getSuperTablesBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getSuperTables(null, null, null),
                "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String, SUPERTABLE_NAME String");
    }

    @Test
    public void testGetSchemas2() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getCatalogs();
        boolean foundTestUnitsJdbc = false;
        while (rs.next()) {
            if (rs.getString(1).equals("testj")) {
                foundTestUnitsJdbc = true;
            }
        }
        assertEquals(true, foundTestUnitsJdbc);
    }

    /* Verify default behavior for nullCatalogMeansCurrent (=true) */
    @Test
    public void nullCatalogMeansCurrent() throws Exception {
        String catalog = sharedConnection.getCatalog();
        ResultSet rs = sharedConnection.getMetaData().getColumns(null, null, null, null);
        while (rs.next()) {
            assertTrue(rs.getString("TABLE_CAT").equalsIgnoreCase(catalog));
        }
    }

    /* Verify that "nullCatalogMeansCurrent=false" works (i.e information_schema columns are returned)*/
    @Test
    public void nullCatalogMeansCurrent2() throws Exception {
        Connection connection = null;
        try {
            connection = setConnection("&nullCatalogMeansCurrent=false");
            boolean haveInformationSchema = false;
            ResultSet rs = connection.getMetaData().getColumns(null, null, null, null);
            while (rs.next()) {
                if (rs.getString("TABLE_CAT").equalsIgnoreCase("information_schema")) {
                    haveInformationSchema = true;
                    break;
                }
            }
            assertTrue(haveInformationSchema);
        } finally {
            connection.close();
        }

    }

    @Test
    public void testGetTypeInfoBasic() throws SQLException {
        testResultSetColumns(
                sharedConnection.getMetaData().getTypeInfo(),
                "TYPE_NAME String,DATA_TYPE int,PRECISION int,LITERAL_PREFIX String,"
                        + "LITERAL_SUFFIX String,CREATE_PARAMS String, NULLABLE short,CASE_SENSITIVE boolean,"
                        + "SEARCHABLE short,UNSIGNED_ATTRIBUTE boolean,FIXED_PREC_SCALE boolean, "
                        + "AUTO_INCREMENT boolean, LOCAL_TYPE_NAME String,MINIMUM_SCALE short,MAXIMUM_SCALE short,"
                        + "SQL_DATA_TYPE int,SQL_DATETIME_SUB int, NUM_PREC_RADIX int");
    }

    @Test
    public void getColumnsTest() throws SQLException {

        DatabaseMetaData dmd = sharedConnection.getMetaData();
        ResultSet rs = dmd.getColumns(sharedConnection.getCatalog(), null, "manycols", null);
        while (rs.next()) {
            String columnName = rs.getString("column_name");
            int type = rs.getInt("data_type");
            String typeName = rs.getString("type_name");
            assertTrue(typeName.indexOf("(") == -1);
            for (char c : typeName.toCharArray()) {
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
            checkType(columnName, type, "decimal_col", Types.DECIMAL);
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
        Connection connection = null;
        try {
            connection = setConnection("&yearIsDateType=false");
            connection.createStatement().execute("insert into ytab values(72)");
            ResultSet rs = connection.getMetaData().getColumns(connection.getCatalog(), null, "ytab", null);
            assertTrue(rs.next());
            assertEquals(rs.getInt("DATA_TYPE"), Types.SMALLINT);
            ResultSet rs1 = connection.createStatement().executeQuery("select * from ytab");
            assertEquals(rs1.getMetaData().getColumnType(1), Types.SMALLINT);
            assertTrue(rs1.next());
            assertTrue(rs1.getObject(1) instanceof Short);
            assertEquals(rs1.getShort(1), 1972);
        } finally {
            connection.close();
        }
    }

    /* CONJ-15 */
    @Test
    public void maxCharLengthUtf8() throws Exception {
        DatabaseMetaData dmd = sharedConnection.getMetaData();
        ResultSet rs = dmd.getColumns(sharedConnection.getCatalog(), null, "maxcharlength", null);
        assertTrue(rs.next());
        assertEquals(rs.getInt("COLUMN_SIZE"), 1);
    }

    @Test
    public void conj72() throws Exception {
        Connection connection = null;
        try {
            connection = setConnection("&tinyInt1isBit=true");
            connection.createStatement().execute("insert into conj72 values(1)");
            ResultSet rs = connection.getMetaData().getColumns(connection.getCatalog(), null, "conj72", null);
            assertTrue(rs.next());
            assertEquals(rs.getInt("DATA_TYPE"), Types.BIT);
            ResultSet rs1 = connection.createStatement().executeQuery("select * from conj72");
            assertEquals(rs1.getMetaData().getColumnType(1), Types.BIT);
        } finally {
            connection.close();
        }
    }
}
