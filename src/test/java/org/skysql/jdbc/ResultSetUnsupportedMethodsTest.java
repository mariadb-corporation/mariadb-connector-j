package org.skysql.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Sep 9, 2009
 * Time: 7:20:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class ResultSetUnsupportedMethodsTest {
    private  ResultSet rs;
    Connection connection;

    static { Logger.getLogger("").setLevel(Level.OFF); }

    public ResultSetUnsupportedMethodsTest() throws SQLException {

    }

    @Before
    public void before() throws SQLException{
        connection = DriverManager.getConnection("jdbc:drizzle://root@localhost:3306/test");
        rs = connection.createStatement().executeQuery("select 1");
    }

    @After
    public void after() throws SQLException {
        connection.close();
    }

    /*    @Test
public void runTests() throws SQLException, InvocationTargetException, IllegalAccessException {
ResultSet rs = connection.createStatement().executeQuery("SELECT 1");

Method[] methods = rs.getClass().getMethods();
int i = 0;
for(Method method : methods) {
if(method.getName().startsWith("update")) {
System.out.println("@Test(expected=SQLFeatureNotSupportedException.class)");
String args = "";
Class[] c = method.getParameterTypes();
if(c.length > 0) {
if(c[0] == String.class) {
args = "\"a\"";
} else {
args = "1";
}

if(c.length > 1) {
if(c[1].getName().equals("int") ||
c[1].getName().equals("long") ||
c[1].getName().equals("short")||
c[1].getName().equals("byte")||
c[1].getName().equals("float")||
c[1].getName().equals("double")  )  {
args+=",("+c[1].getName()+")1";
} else {
args+=",("+c[1].getName()+")null";
}


}
System.out.println("public void test"+method.getName()+(i++)+"() throws SQLException {");
System.out.println("   rs."+method.getName()+"("+args+");");
System.out.println("}");
}
}
}
}


                                            */
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testGetRef() throws SQLException {
       rs.getRef(1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testGetRef2() throws SQLException {
       rs.getRef("");
    }

    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testRowID() throws SQLException {
       rs.getRowId(1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testRowId2() throws SQLException {
       rs.getRowId("");
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testGetArray() throws SQLException {
       rs.getArray(1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testInsertRow() throws SQLException {
       rs.insertRow();
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testDeleteRow() throws SQLException {
       rs.deleteRow();
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testUpdateRow() throws SQLException {
       rs.updateRow();
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testRefreshRow() throws SQLException {
       rs.refreshRow();
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testCancelRowUpdates() throws SQLException {
       rs.cancelRowUpdates();
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testMoveToInsertRow() throws SQLException {
       rs.moveToInsertRow();
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testMoveToCurrentRow() throws SQLException {
       rs.moveToCurrentRow();
    }

    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream() throws SQLException {
       rs.updateBinaryStream(1,null,0);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream2() throws SQLException {
       rs.updateBinaryStream("",null,0);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateObject() throws SQLException {
       rs.updateObject(1,null,0);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateObject2() throws SQLException {
       rs.updateObject("",null,0);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateCharStream() throws SQLException {
       rs.updateCharacterStream(1,null,0);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateCharStream2() throws SQLException {
       rs.updateCharacterStream("",null,0);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream() throws SQLException {
       rs.updateAsciiStream(1,null,0);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream2() throws SQLException {
       rs.updateAsciiStream("a",null,0);
    }

    @Test(expected=SQLFeatureNotSupportedException.class)
    public void getRowUpdated() throws SQLException {
       rs.rowUpdated();
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void getRowDeleted() throws SQLException {
       rs.rowDeleted();
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void getRowInserte() throws SQLException {
       rs.rowInserted();
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void getCursorName() throws SQLException {
       rs.getCursorName();
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNull0() throws SQLException {
       rs.updateNull(1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNull1() throws SQLException {
       rs.updateNull("a");
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBoolean2() throws SQLException {
       rs.updateBoolean(1,false);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBoolean3() throws SQLException {
       rs.updateBoolean("a",false);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateByte4() throws SQLException {
       rs.updateByte(1,(byte)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateByte5() throws SQLException {
       rs.updateByte("a",(byte)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateShort6() throws SQLException {
       rs.updateShort(1,(short)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateShort7() throws SQLException {
       rs.updateShort("a",(short)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateInt8() throws SQLException {
       rs.updateInt(1,(int)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateInt9() throws SQLException {
       rs.updateInt("a",(int)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateLong10() throws SQLException {
       rs.updateLong(1,(long)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateLong11() throws SQLException {
       rs.updateLong("a",(long)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateFloat12() throws SQLException {
       rs.updateFloat(1,(float)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateFloat13() throws SQLException {
       rs.updateFloat("a",(float)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateDouble14() throws SQLException {
       rs.updateDouble(1,(double)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateDouble15() throws SQLException {
       rs.updateDouble("a",(double)1);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBigDecimal16() throws SQLException {
       rs.updateBigDecimal(1,(java.math.BigDecimal)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBigDecimal17() throws SQLException {
       rs.updateBigDecimal("a",(java.math.BigDecimal)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateString18() throws SQLException {
       rs.updateString(1,(java.lang.String)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateString19() throws SQLException {
       rs.updateString("a",(java.lang.String)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBytes20() throws SQLException {
       rs.updateBytes(1,null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBytes21() throws SQLException {
       rs.updateBytes("a",null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateDate22() throws SQLException {
       rs.updateDate(1,(java.sql.Date)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateDate23() throws SQLException {
       rs.updateDate("a",(java.sql.Date)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateTime24() throws SQLException {
       rs.updateTime(1,(java.sql.Time)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateTime25() throws SQLException {
       rs.updateTime("a",(java.sql.Time)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateTimestamp26() throws SQLException {
       rs.updateTimestamp(1,(java.sql.Timestamp)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateTimestamp27() throws SQLException {
       rs.updateTimestamp("a",(java.sql.Timestamp)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream28() throws SQLException {
       rs.updateAsciiStream(1,(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream29() throws SQLException {
       rs.updateAsciiStream("a",(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream30() throws SQLException {
       rs.updateAsciiStream(1,(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream31() throws SQLException {
       rs.updateAsciiStream("a",(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream32() throws SQLException {
       rs.updateAsciiStream(1,(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream33() throws SQLException {
       rs.updateAsciiStream("a",(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream34() throws SQLException {
       rs.updateBinaryStream(1,(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream35() throws SQLException {
       rs.updateBinaryStream("a",(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream36() throws SQLException {
       rs.updateBinaryStream(1,(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream37() throws SQLException {
       rs.updateBinaryStream("a",(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream38() throws SQLException {
       rs.updateBinaryStream(1,(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream39() throws SQLException {
       rs.updateBinaryStream("a",(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream40() throws SQLException {
       rs.updateCharacterStream(1,(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream41() throws SQLException {
       rs.updateCharacterStream("a",(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream42() throws SQLException {
       rs.updateCharacterStream(1,(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream43() throws SQLException {
       rs.updateCharacterStream("a",(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream44() throws SQLException {
       rs.updateCharacterStream(1,(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream45() throws SQLException {
       rs.updateCharacterStream("a",(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateObject46() throws SQLException {
       rs.updateObject(1,(java.lang.Object)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateObject47() throws SQLException {
       rs.updateObject(1,(java.lang.Object)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateObject48() throws SQLException {
       rs.updateObject("a",(java.lang.Object)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateObject49() throws SQLException {
       rs.updateObject("a",(java.lang.Object)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateRef50() throws SQLException {
       rs.updateRef(1,(java.sql.Ref)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateRef51() throws SQLException {
       rs.updateRef("a",(java.sql.Ref)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBlob52() throws SQLException {
       rs.updateBlob(1,(java.sql.Blob)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBlob53() throws SQLException {
       rs.updateBlob("a",(java.sql.Blob)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBlob54() throws SQLException {
       rs.updateBlob(1,(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBlob55() throws SQLException {
       rs.updateBlob("a",(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBlob56() throws SQLException {
       rs.updateBlob(1,(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateBlob57() throws SQLException {
       rs.updateBlob("a",(java.io.InputStream)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateClob58() throws SQLException {
       rs.updateClob(1,(java.sql.Clob)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateClob59() throws SQLException {
       rs.updateClob("a",(java.sql.Clob)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateClob60() throws SQLException {
       rs.updateClob(1,(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateClob61() throws SQLException {
       rs.updateClob("a",(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateClob62() throws SQLException {
       rs.updateClob(1,(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateClob63() throws SQLException {
       rs.updateClob("a",(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateArray64() throws SQLException {
       rs.updateArray(1,(java.sql.Array)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateArray65() throws SQLException {
       rs.updateArray("a",(java.sql.Array)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateRowId66() throws SQLException {
       rs.updateRowId(1,(java.sql.RowId)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateRowId67() throws SQLException {
       rs.updateRowId("a",(java.sql.RowId)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNString68() throws SQLException {
       rs.updateNString(1,(java.lang.String)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNString69() throws SQLException {
       rs.updateNString("a",(java.lang.String)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNClob70() throws SQLException {
       rs.updateNClob(1,(java.sql.NClob)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNClob71() throws SQLException {
       rs.updateNClob("a",(java.sql.NClob)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNClob72() throws SQLException {
       rs.updateNClob(1,(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNClob73() throws SQLException {
       rs.updateNClob("a",(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNClob74() throws SQLException {
       rs.updateNClob(1,(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNClob75() throws SQLException {
       rs.updateNClob("a",(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateSQLXML76() throws SQLException {
       rs.updateSQLXML(1,(java.sql.SQLXML)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateSQLXML77() throws SQLException {
       rs.updateSQLXML("a",(java.sql.SQLXML)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNCharacterStream78() throws SQLException {
       rs.updateNCharacterStream(1,(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNCharacterStream79() throws SQLException {
       rs.updateNCharacterStream("a",(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNCharacterStream80() throws SQLException {
       rs.updateNCharacterStream(1,(java.io.Reader)null);
    }
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testupdateNCharacterStream81() throws SQLException {
       rs.updateNCharacterStream("a",(java.io.Reader)null);
    }


}
