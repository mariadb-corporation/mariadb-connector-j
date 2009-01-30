package org.drizzle.jdbc;

import java.sql.*;
import java.math.BigDecimal;
import java.io.InputStream;
import java.io.Reader;
import java.util.Calendar;
import java.util.List;
import java.util.ArrayList;
import java.net.URL;

/**
 * User: marcuse
 * Date: Jan 27, 2009
 * Time: 10:49:42 PM
 */
public class DrizzlePreparedStatement extends DrizzleStatement implements PreparedStatement  {
    private final String query;
    private final List<Integer> replacementIndexes;
    private List<String> replacements;

    public DrizzlePreparedStatement(Protocol protocol, String query) {
        super(protocol);
        replacementIndexes = getQuestionMarkIndexes(query);
        this.query=query;
        this.replacements = new ArrayList<String>(replacementIndexes.size());
    }

    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(prepareQuery());
    }

    private String prepareQuery() {
        int i=0;
        int insertedOffset = 0;
        String realQuery=query;
        for(String replacement : replacements) {
            realQuery = insertStringAt(realQuery,replacement,replacementIndexes.get(i++) + insertedOffset);
            insertedOffset+=(replacement.length()-1);
        }
        return realQuery;
    }

    public int executeUpdate() throws SQLException {
        return super.executeUpdate(prepareQuery());
    }
    
    public boolean execute() throws SQLException {
        return super.execute(prepareQuery());
    }

    public void setNull(int i, int i1) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setBoolean(int column, boolean value) throws SQLException {
        if(column > replacements.size()+1)
            throw new SQLException("Index out of range");
        if(value)
            replacements.add(column-1,"TRUE");
        else
            replacements.add(column-1,"FALSE");
    }
    public void setString(int column, String s) throws SQLException {
        if(column > replacements.size()+1)
            throw new SQLException("Index out of range");
        replacements.add(column-1,"\""+sqlEscapeString(s)+"\"");
    }
    public void setInt(int column, int i) throws SQLException {
        if(column > replacements.size()+1)
            throw new SQLException("Index out of range");
        replacements.add(column-1, String.valueOf(i));
    }

    public void setByte(int column, byte b) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setShort(int column, short i1) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }


    public void setLong(int i, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setFloat(int i, float v) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setDouble(int i, double v) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }


    public void setBytes(int i, byte[] bytes) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setDate(int i, Date date) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setTime(int i, Time time) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setTimestamp(int i, Timestamp timestamp) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setUnicodeStream(int i, InputStream inputStream, int i1) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void clearParameters() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setObject(int i, Object o, int i1) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setObject(int i, Object o) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void addBatch() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setCharacterStream(int i, Reader reader, int i1) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setRef(int i, Ref ref) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setBlob(int i, Blob blob) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setClob(int i, Clob clob) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setArray(int i, Array array) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setDate(int i, Date date, Calendar calendar) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setTime(int i, Time time, Calendar calendar) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setNull(int i, int i1, String s) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setURL(int i, URL url) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setRowId(int i, RowId rowId) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setNString(int i, String s) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setNClob(int i, NClob nClob) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setClob(int i, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setBlob(int i, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setNClob(int i, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setObject(int i, Object o, int i1, int i2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setCharacterStream(int i, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setAsciiStream(int i, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setBinaryStream(int i, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setCharacterStream(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setNCharacterStream(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setClob(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setBlob(int i, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setNClob(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public static List<Integer> getQuestionMarkIndexes(String query) {
        int qmIndex=0;
        List<Integer> qmList = new ArrayList<Integer>();
        while((qmIndex = query.indexOf("?", qmIndex+1))!=-1) {
            qmList.add(qmIndex);
        }
        return qmList;
    }

    /**
     * TODO: make more efficient!
     * @param query the query to insert
     * @param replacement the string to insert
     * @param index where to put it
     * @return a string with 'replacement' instead of the ? at index 
     */
    public static String insertStringAt(String query,String replacement, int index) {
        String s1 = query.substring(0,index);
        if(index == query.length())
            return s1.substring(0,index)+replacement;
        String s2 = query.substring(index+1,query.length()); // "+1" to remove the question mark
        return s1+replacement+s2;
    }

    /**
     * TODO: yeah, do something
     * @param str the string to escape
     * @return an escaped string
     */
    public static String sqlEscapeString(String str) {
        return str;
    }
}