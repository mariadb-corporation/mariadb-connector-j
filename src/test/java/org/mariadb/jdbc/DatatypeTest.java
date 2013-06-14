package org.mariadb.jdbc;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Types;

import static junit.framework.Assert.assertEquals;


public class DatatypeTest extends BaseTest {
    static final String ddl =
        "CREATE TABLE datatypetest (" +
                      "bit1 BIT(1) default 0," +
                      "bit2 BIT(2) default 1," +
                      "tinyint1 TINYINT(1) default 0," +
                      "tinyint2 TINYINT(2) default 1," +
                      "bool0 BOOL default 0,"  +
                      "smallint0 SMALLINT default 1," +
                      "smallint_unsigned SMALLINT UNSIGNED default 0," +
                      "mediumint0 MEDIUMINT default 1,"+
                      "mediumint_unsigned MEDIUMINT UNSIGNED default 0," +
                      "int0 INT default 1," +
                      "int_unsigned INT UNSIGNED default 0," +
                      "bigint0 BIGINT default 1," +
                      "bigint_unsigned BIGINT UNSIGNED default 0," +
                      "float0 FLOAT default 0," +
                      "double0 DOUBLE default 1," +
                      "decimal0 DECIMAL default 0," +
                      "date0 DATE default '2001-01-01'," +
                      "datetime0 DATETIME default '2001-01-01 00:00:00',"+
                      "timestamp0 TIMESTAMP default  '2001-01-01 00:00:00'," +
                      "time0 TIME default '22:11:00'," +
                      "year2 YEAR(2) default 99," +
                      "year4 YEAR(4) default 2011," +
                      "char0 CHAR(1) default '0'," +
                      "char_binary CHAR (1) binary default '0'," +
                      "varchar0 VARCHAR(1) default '1'," +
                      "varchar_binary VARCHAR(10) BINARY default 0x1," +
                      "binary0 BINARY(10) default 0x1," +
                      "varbinary0 VARBINARY(10) default 0x1," +
                      "tinyblob0 TINYBLOB," +
                      "tinytext0 TINYTEXT," +
                      "blob0 BLOB," +
                      "text0 TEXT," +
                      "mediumblob0 MEDIUMBLOB," +
                      "mediumtext0 MEDIUMTEXT," +
                      "longblob0 LONGBLOB," +
                      "longtext0 LONGTEXT," +
                      "enum0 ENUM('a','b') default 'a'," +
                      "set0 SET('a','b') default 'a' )";

    ResultSet resultSet;

    void checkClass(String column, Class<?> clazz, String mysqlType, int javaSqlType) throws Exception{
        int index = resultSet.findColumn(column);

        if(resultSet.getObject(column) != null) {
            assertEquals("Unexpected class for column " + column, clazz, resultSet.getObject(column).getClass());
        }
        assertEquals("Unexpected class name for column " + column, clazz.getName(), resultSet.getMetaData().getColumnClassName(index));
        assertEquals("Unexpected MySQL type for column " + column, mysqlType,resultSet.getMetaData().getColumnTypeName(index));
        assertEquals("Unexpected java sql type for column " + column, javaSqlType, resultSet.getMetaData().getColumnType(index));
    }


    public void datatypes(Connection c, boolean tinyInt1isBit, boolean yearIsDateType) throws Exception{
        c.createStatement().execute("drop table if exists datatypetest");
        c.createStatement().execute(ddl);
        c.createStatement().execute("insert into datatypetest (tinyblob0,mediumblob0,blob0,longblob0,"+
                "tinytext0,mediumtext0,text0, longtext0) values(0x1,0x1,0x1,0x1, 'a', 'a', 'a', 'a')");

        resultSet = c.createStatement().executeQuery("select * from datatypetest");
        resultSet.next();

        Class<?> byteArrayClass = (new byte[0]).getClass();

        checkClass("bit1",Boolean.class,"BIT", Types.BIT);
        checkClass("bit2", byteArrayClass, "BIT", Types.VARBINARY);

        checkClass("tinyint1",
                tinyInt1isBit?Boolean.class:Integer.class, "TINYINT",
                tinyInt1isBit?Types.BIT:Types.TINYINT);
        checkClass("tinyint2",Integer.class, "TINYINT", Types.TINYINT);
        checkClass("bool0", tinyInt1isBit?Boolean.class:Integer.class,"TINYINT",
                tinyInt1isBit?Types.BIT:Types.TINYINT);
        checkClass("smallint0", Integer.class, "SMALLINT", Types.SMALLINT);
        checkClass("smallint_unsigned", Integer.class, "SMALLINT UNSIGNED", Types.INTEGER);
        checkClass("mediumint0", Integer.class, "MEDIUMINT", Types.INTEGER);
        checkClass("mediumint_unsigned", Integer.class, "MEDIUMINT UNSIGNED", Types.INTEGER);
        checkClass("int0", Integer.class, "INTEGER", Types.INTEGER);
        checkClass("int_unsigned", Long.class, "INTEGER UNSIGNED", Types.BIGINT);
        checkClass("bigint0", Long.class, "BIGINT", Types.BIGINT);
        checkClass("bigint_unsigned", BigInteger.class, "BIGINT UNSIGNED", Types.BIGINT);
        checkClass("float0", Float.class, "FLOAT",Types.FLOAT);
        checkClass("double0", Double.class, "DOUBLE", Types.DOUBLE);
        checkClass("decimal0", BigDecimal.class, "DECIMAL", Types.DECIMAL);
        checkClass("date0", java.sql.Date.class, "DATE", Types.DATE);
        checkClass("time0", java.sql.Time.class, "TIME", Types.TIME);
        checkClass("timestamp0", java.sql.Timestamp.class, "TIMESTAMP", Types.TIMESTAMP);
        checkClass("year2",
                yearIsDateType? java.sql.Date.class: Short.class, "YEAR",
                yearIsDateType? Types.DATE: Types.SMALLINT );
        checkClass("year4",
                yearIsDateType? java.sql.Date.class: Short.class, "YEAR",
                yearIsDateType? Types.DATE: Types.SMALLINT);
        checkClass("char0", String.class, "CHAR", Types.CHAR);
        checkClass("char_binary", String.class, "CHAR", Types.CHAR);
        checkClass("varchar0", String.class, "VARCHAR", Types.VARCHAR);
        checkClass("varchar_binary",String.class, "VARCHAR", Types.VARCHAR);
        checkClass("binary0", byteArrayClass, "BINARY", Types.BINARY);
        checkClass("varbinary0", byteArrayClass, "VARBINARY", Types.VARBINARY);
        checkClass("tinyblob0",byteArrayClass, "TINYBLOB", Types.VARBINARY);
        checkClass("tinytext0", String.class, "VARCHAR", Types.VARCHAR);
        checkClass("blob0", byteArrayClass, "BLOB",Types.VARBINARY);
        checkClass("text0", String.class, "VARCHAR", Types.VARCHAR);
        checkClass("mediumblob0", byteArrayClass, "MEDIUMBLOB", Types.VARBINARY);
        checkClass("mediumtext0", String.class,"VARCHAR", Types.VARCHAR);
        checkClass("longblob0", byteArrayClass, "LONGBLOB", Types.LONGVARBINARY);
        checkClass("longtext0", String.class, "VARCHAR", Types.LONGVARCHAR);
        checkClass("enum0", String.class, "CHAR",Types.CHAR);
        checkClass("set0", String.class, "CHAR",Types.CHAR);

        resultSet = c.createStatement().executeQuery("select NULL as foo");
        resultSet.next();
        checkClass("foo", String.class, "NULL",Types.NULL);

    }

    @Test
    public void datatypes1() throws Exception {
       datatypes(connection, true, true);
    }

    @Test
    public void datatypes2() throws Exception {
        Connection c = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&tinyInt1isBit=0&yearIsDateType=0");
        datatypes(c, false, false);
    }

    @Test
    public void datatypes3() throws Exception {
        Connection c = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&tinyInt1isBit=1&yearIsDateType=0");
        datatypes(c, true, false);
    }

    @Test
    public void datatypes4() throws Exception {
        Connection c = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&tinyInt1isBit=0&yearIsDateType=1");
        datatypes(c, false, true);
    }


}
