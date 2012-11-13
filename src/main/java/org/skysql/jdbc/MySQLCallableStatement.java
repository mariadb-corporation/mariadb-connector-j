package org.skysql.jdbc;

import org.skysql.jdbc.internal.common.Utils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Info about in/out parameters
 */
class CallParameter {
    boolean isInput;
    boolean isOutput;
    int sqlType;
    int outputSQLType;
    int scale;
    String typeName;
    boolean isSigned;
    int isNullable;
    int precision;
    String className;
    String name;
    public CallParameter() {
        sqlType = Types.OTHER;
        outputSQLType = Types.OTHER;
    }
}

class CallableParameterMetaData implements ParameterMetaData {

    CallParameter[] params;
    Connection con;
    String name;
    boolean valid;
    boolean isFunction;
    boolean noAccessToMetadata;
    static Pattern PARAMETER_PATTERN =
            Pattern.compile("\\s*(IN\\s+|OUT\\s+|INOUT\\s+)?([\\w\\d]+)\\s+(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d]+\\))?\\s*",
                    Pattern.CASE_INSENSITIVE);
    static Pattern RETURN_PATTERN =
            Pattern.compile("\\s*(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d]+\\))?\\s*", Pattern.CASE_INSENSITIVE);

    public CallableParameterMetaData(CallParameter[] params, Connection con, String name, boolean isFunction) {
        this.params = params;
        this.con = con;
        this.name = name;
        this.isFunction = isFunction;
    }

    public void readMetadataFromDBIfRequired() throws SQLException{
        if (noAccessToMetadata || valid)
            return;
        try {
            readMetadata();
            valid = true;
        } catch(SQLException e) {
            noAccessToMetadata = true;
            throw e;
        }
    }
    int mapMySQLTypeToJDBC(String t) {

        t = t.toUpperCase();

        if (t.equals("BIT")) return Types.BIT;
        if (t.equals("TINYINT")) return Types.TINYINT;
        if (t.equals("SMALLINT")) return Types.SMALLINT;
        if (t.equals("MEDIUMINT")) return Types.INTEGER;
        if (t.equals("INT")) return Types.INTEGER;
        if (t.equals("INTEGER")) return Types.INTEGER;
        if (t.equals("LONG")) return Types.INTEGER;
        if (t.equals("BIGINT")) return Types.BIGINT;
        if (t.equals("INT24")) return Types.INTEGER;
        if (t.equals("REAL")) return Types.DOUBLE;
        if (t.equals("FLOAT")) return Types.FLOAT;
        if (t.equals("DECIMAL")) return Types.DECIMAL;
        if (t.equals("NUMERIC")) return Types.NUMERIC;
        if (t.equals("DOUBLE")) return Types.DOUBLE;
        if (t.equals("CHAR")) return Types.CHAR;
        if (t.equals("VARCHAR")) return Types.VARCHAR;
        if (t.equals("DATE")) return Types.DATE;
        if (t.equals("TIME")) return Types.TIME;
        if (t.equals("YEAR")) return Types.SMALLINT;
        if (t.equals("TIMESTAMP")) return Types.TIMESTAMP;
        if (t.equals("DATETIME")) return Types.TIMESTAMP;
        if (t.equals("TINYBLOB")) return Types.BINARY;
        if (t.equals("BLOB")) return Types.LONGVARBINARY;
        if (t.equals("MEDIUMBLOB")) return Types.LONGVARBINARY;
        if (t.equals("LONGBLOB")) return Types.LONGVARBINARY;
        if (t.equals("TINYTEXT")) return Types.VARCHAR;
        if (t.equals("TEXT")) return Types.LONGVARCHAR;
        if (t.equals("MEDIUMTEXT")) return Types.LONGVARCHAR;
        if (t.equals("LONGTEXT")) return Types.LONGVARCHAR;
        if (t.equals("ENUM")) return Types.VARCHAR;
        if (t.equals("SET")) return Types.VARCHAR;
        if (t.equals("GEOMETRY")) return Types.LONGVARBINARY;
        if (t.equals("VARBINARY")) return Types.VARBINARY;
        if (t.equals("BIT")) return Types.BIT;

        return Types.OTHER;
    }
    /*
    Read procedure metadata from mysql.proc table(column param_list)
     */
    public void  readMetadata() throws SQLException{
        if (noAccessToMetadata || valid)
            return;

        boolean noBackslashEscapes  = false;

        if (con instanceof MySQLConnection) {
            noBackslashEscapes =   ((MySQLConnection)con).noBackslashEscapes;
        }

        String dbname= "database()";
        String procedureNameNoDb=name;

        int dotIndex = name.indexOf('.');
        if(dotIndex > 0) {
            dbname = name.substring(0, dotIndex);
            dbname = dbname.replace("`", "");
            dbname = "'" + Utils.sqlEscapeString(dbname,noBackslashEscapes)  + "'";
            procedureNameNoDb= name.substring(dotIndex+1);
        }

        procedureNameNoDb = procedureNameNoDb.replace("`","");
        procedureNameNoDb = "'" + Utils.sqlEscapeString(procedureNameNoDb,noBackslashEscapes) + "'";

        Statement st = con.createStatement();
        ResultSet rs = null;
        String paramList;
        String functionReturn;
        try {
            String q = "select param_list,returns from mysql.proc where db="
                    + dbname + " and name=" + procedureNameNoDb;
            rs = st.executeQuery(q);
            if(!rs.next()) {
                throw new SQLException("procedure or function " + name + "does not exist");
            }
            paramList = rs.getString(1);
            functionReturn = rs.getString(2);

        } finally {
            if(rs != null)
                rs.close();
            st.close();
        }

        // parse type of the return value (for functions)
        if (isFunction) {
            if (functionReturn == null || functionReturn.length() == 0) {
                throw new SQLException(name + "is not a function returning value");
            }
            Matcher m = RETURN_PATTERN.matcher(functionReturn);
            if (!m.matches()) {
                throw new SQLException("can not parse return value definition :" + functionReturn);
            }
            CallParameter p = params[1];
            p.isOutput = true;
            p.isSigned = (m.group(1) == null);
            p.typeName = m.group(2).trim();
            p.sqlType  = mapMySQLTypeToJDBC(p.typeName);
            String scale = m.group(3);
            if (scale != null) {
                scale = scale.replace("(","").replace(")","").replace(" ","");
                p.scale = Integer.valueOf(scale).intValue();
            }

        }

        StringTokenizer tokenizer = new StringTokenizer(paramList,",", false);
        int paramIndex = isFunction?2:1;

        while(tokenizer.hasMoreTokens()){
            if (paramIndex >= params.length) {
                throw new SQLException("Invalid placeholder count in CallableStatement");
            }
            String paramDef = tokenizer.nextToken();

            Matcher m = PARAMETER_PATTERN.matcher(paramDef);
            if (!m.matches())
                throw new SQLException("cannot parse parameter definition :" + paramDef);
            String direction = m.group(1);
            if(direction!= null)
                direction = direction.trim();
            String paramName = m.group(2);
            paramName = paramName.trim();
            boolean isSigned = (m.group(3) == null);
            String dataType = m.group(4);
            dataType = dataType.trim();
            String scale = m.group(5);
            if (scale != null)
                scale = scale.trim();

            CallParameter p = params[paramIndex];
            if (direction == null || direction.equalsIgnoreCase("IN")) {
                p.isInput = true;
            }
            else if (direction.equalsIgnoreCase("OUT")) {
                p.isOutput= true;
            }
            else if (direction.equalsIgnoreCase("INOUT")) {
                p.isInput = p.isOutput = true;
            } else {
                throw new SQLException("unknown parameter direction " + direction + "for " + paramName);
            }
            p.name = paramName;
            p.typeName = dataType.toUpperCase();
            p.sqlType = mapMySQLTypeToJDBC(p.typeName);
            p.isSigned = isSigned;
            if (scale != null) {
                scale = scale.replace("(","").replace(")","").replace(" ","");
                p.scale = Integer.valueOf(scale).intValue();
            }
            paramIndex++;
        }
    }

    public int getParameterCount() throws SQLException {
        return params.length -1;
    }

    CallParameter getParam(int index) throws SQLException{
        if (index < 1 || index >= params.length)
            throw new SQLException("invalid parameter index "+index);
        readMetadataFromDBIfRequired();
        return params[index];
    }
    public int isNullable(int param) throws SQLException {
        return getParam(param).isNullable;
    }
    public boolean isSigned(int param) throws SQLException {
        return getParam(param).isSigned;
    }

    public int getPrecision(int param) throws SQLException {
        return getParam(param).precision;
    }

    public int getScale(int param) throws SQLException {
        return getParam(param).scale;
    }
    public int getParameterType(int param) throws SQLException {
        return getParam(param).sqlType;
    }

    public String getParameterTypeName(int param) throws SQLException {
        return getParam(param).typeName;
    }

    public String getParameterClassName(int param) throws SQLException {
        return getParam(param).className;
    }

    public int getParameterMode(int param) throws SQLException {
        CallParameter p = getParam(param);
        if (p.isInput && p.isOutput)
            return parameterModeInOut;
        if (p.isInput)
            return parameterModeIn;
        if (p.isOutput)
            return parameterModeOut;
        return parameterModeUnknown;
    }

    public String getName(int param) throws SQLException{
        return getParam(param).name;
    }
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}


/*
 * Implementation of JDBC CallableStatement, using user variables for input and output parameters.
 *
 * Mode of operation:
 * 1. '?' placeholders in the CALL query are replaced by user variables @_jdbc_var_$i where $i is the 1..N index
 * 2. If query has input parameters, they are set prior to execution using prepared statement
 * in the form "SET @_jdbc_var_1=?, ... , jdbc_var_N=?"
 * 3. then CALL statement is executed
 * 4. Output parameters are fetched when necessary using
 *  "SELECT @_jdbc_var_1, ... , @_jdbc_var_N"
 *
 * Technicalities:
 * - Queries that return result, i.e have the form {?=CALL PROC(?)} are MySQL stored functions, and they are transformed
 * to SELECT PROC(@_jdbc_var_2) INTO @_jdb_var_1
 * - Queries that do not return result and have the form {CALL PROC(?)} are transformed into CALL PROC(@_jdbc_var_1)
 *
 * Stored procedure metadata are not normally available or needed. When necessary, the whole SP text is fetched and parsed
 * If it cannot be fetched (e.g privilege issue) then some functionality won't be available, for example named parameters
 * will not work.
 */
public class MySQLCallableStatement implements CallableStatement
{
    /**
     *  Database connection
     */
    Connection con;

    /**
     *   Prepared statement, typically used to set input variables, in which case it has the form
     *   set _jdbc_var1=?,...,jdbc_var_N=?
     *   Also, handles addBatch/executeBatch() statements, even if there is no input parameters.
     */
    PreparedStatement preparedStatement;

    /** Current count of batch statements */
    int batchCount;

    /**
     * In case parameters are used, some results of the batch statement need to be ignored, since we generate
     *  extra statements for setting input parameters. batchIgnoreResult is a bitmap for ignored results.
     */
    BitSet batchIgnoreResult;

    /**
     * Resolved query with ? placeholders replaces by user variables ,
     * e.g {call proc(?,?)} would be resolved as CALL proc(@_jdbc_var_1,@_jdbc_var_2)
     */
    String callQuery;
    Statement callStatement;

    /**
     * Result set containing output parameters.
     * We generate query SELECT @_jdbc_var1, ... @_jdbc_var_N to fetch the results.
     */
    ResultSet rsOutputParameters;

    /**
     * Information about parameters, merely from registerOutputParameter() and setXXX() calls.
     */
    CallParameter params[];
    CallableParameterMetaData parameterMetadata;
    int parametersCount;

    /**
     * Pattern  to check the correctness of callable statement query string
     * Legal queries, as documented in JDK have the form:
     * {[?=]call[(arg1,..,,argn)]}
     */
    static Pattern CALLABLE_STATEMENT_PATTERN =
            Pattern.compile("^\\s*\\{?\\s*(\\?\\s*=)?\\s*call\\s*([\\w.]+)(\\(.*\\))?\\s*}?", Pattern.CASE_INSENSITIVE);

    public MySQLCallableStatement(Connection connection, String query) throws SQLException{
        con = connection;

        query = Utils.nativeSQL(query, ((MySQLConnection)connection).noBackslashEscapes);
        batchIgnoreResult = new BitSet();
        Matcher m = CALLABLE_STATEMENT_PATTERN.matcher(query);
        if(!m.matches())
            throw new SQLSyntaxErrorException("invalid callable syntax");

        //
        boolean isFunction = (m.group(1) != null);
        String procedureName = m.group(2);
        String arguments = m.group(3);
        if (!isFunction) {
            // real stored procedure, generate "CALL PROC(args)", with ? in args replaces by variables
            callQuery = "call " + procedureName + resolveArguments(arguments,1);
        }
        else {
            // function returning result, generate "SELECT FUNC(args) into @_jdbc_var_1
            callQuery = "select " + procedureName + resolveArguments(arguments, 2) + " into " + getVariableName(1);
        }
        callStatement = con.createStatement();

        // Generate set _jdbc_var1=?,....set jdbc_var_n=? prepared statement
        if (parametersCount != 0) {
            StringBuffer sb = new StringBuffer("set ");
            for(int i = 1; i <= parametersCount; i++) {
                if(i > 1) {
                    sb.append(",");
                }
                sb.append(getVariableName(i));
                sb.append("=?");
            }
            preparedStatement = con.prepareStatement(sb.toString());
            for(int i=1; i<= parametersCount; i++) {
                preparedStatement.setNull(i, Types.NULL);
            }
        }
        params = new CallParameter[parametersCount+1];
        for(int i=1 ; i <= parametersCount; i++) {
            params[i] = new CallParameter();
        }
        if (isFunction) {
            // the query was in the form {?=call function()}, so the first parameter is always output
            params[1].isOutput = true;
        }
        parameterMetadata = new CallableParameterMetaData(params, con, procedureName, isFunction);
    }

    /**
     *  Return user variable name corresponding to a parameter.
     * @param index
     * @return use variable name
     */
    private String getVariableName(int index) {
        return "@_jdbc_var_"+index;
    }


    /**
     * Fetch output variables
     * @return ResultSet from "select @_jdbc_var_1...,@_jdbc_var_N
     * @throws SQLException
     */
    ResultSet outputParameters() throws SQLException {
        if(parametersCount == 0)
            throw new SQLException("no output parameters");
        if (rsOutputParameters == null) {
            // Make sure there are no more results left from the call statement
            while(callStatement.getMoreResults()) {};

            StringBuffer sb = new StringBuffer("select ");
            for(int i=1; i<= parametersCount; i++) {
                if(i != 1) {
                    sb.append(",");
                }
                if(!params[i].isOutput) {
                    sb.append("NULL");
                }
                else {
                    sb.append(getVariableName(i));
                }
            }
            Statement st= con.createStatement();
            rsOutputParameters = st.executeQuery(sb.toString());
            rsOutputParameters.next();
        }
        return rsOutputParameters;
    }

    /**
     * Get prepared statement to set input parameters
     * @return PreparedStatement (for query in the form SET @_jdbc_var_1=?, ...., _jdbc_var_N=?)
     * @throws SQLException
     */
    PreparedStatement inputParameters() throws SQLException{
        if(parametersCount == 0)
            throw new SQLException("no input parameters");
        return preparedStatement;
    }

    /**
     * Convert parameter name to parameter index in the query.
     *
     * @param parameterName
     * @return
     * @throws SQLException
     */
    private int nameToIndex(String parameterName) throws SQLException{
        if(callStatement != null) {
            while(callStatement.getMoreResults()) {}
        }
        for(int i=1; i<= parameterMetadata.getParameterCount(); i++) {
            String name = parameterMetadata.getName(i);
            if(name != null && name.equalsIgnoreCase(parameterName))
                return i;
        }
        throw new SQLException("there is no parameter with the name "+parameterName);
    }

    /**
     * Replace placeholder in query string with corresponding user variables.
     * Takes case of string literal and comments
     * @param args
     * @param startingIndex (1 for stored procedure, 2 for stored function)
     * @return
     */
    String resolveArguments(String args, int  startingIndex) {
        if (args == null) {
            parametersCount = 0;
            return "()";
        }

        StringBuffer sb = new StringBuffer();
        int index = startingIndex;

        boolean inQuote = false;
        boolean inComment =false;
        char quoteChar=0;
        char prevChar=0;
        boolean slashStarComment= false;
        boolean inEscape = false;

        for (char c : args.toCharArray()) {
            if(c == '\\' && !inComment && !inQuote) {
                inEscape = !inEscape;
            }
            if (inEscape) {
                sb.append(c);
                inEscape = false;
                continue;
            }

            switch(c) {
                case '?':
                    if (!inQuote && !inComment) {
                        /* Replace placeholder with variable */
                        sb.append(getVariableName(index++));
                    } else {
                        sb.append(c);
                    }
                    break;
                case '"':
                case '\'':
                    if(!inComment) {
                        if (inQuote) {
                            if (quoteChar == c)
                                inQuote = false;
                        }
                        else {
                            inQuote = true;
                            quoteChar = c;
                        }
                    }
                    sb.append(c);
                    break;
                case '*':
                    if(prevChar == '/' && !inQuote) {
                        inComment = true;
                        slashStarComment = true;
                    }
                    sb.append(c);
                    break;
                case '/':
                    if (prevChar == '*' && inComment && slashStarComment) {
                        inComment = false;
                        slashStarComment =false;
                    } else if (prevChar == '/' && !inQuote) {
                        inComment = true;
                    }
                    sb.append(c);
                    break;
                case '\n':
                    if(inComment && !slashStarComment) {
                        // End-of-line, end of slashslash comment
                        inComment = false;
                    }
                    sb.append(c);
                    break;
                default:
                    sb.append(c);
                    break;
            }

            prevChar = c;
        }

        parametersCount = index - 1;
        return sb.toString();
    }



    CallParameter getParameter(int index) throws SQLException{
        if(index > params.length || index < 1)
            throw new SQLException("No parameter with index " + index);
        return params[index];
    }
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        registerOutParameter(parameterIndex, sqlType, -1);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        CallParameter p = getParameter(parameterIndex);
        p.isOutput = true;
        p.outputSQLType = sqlType;
        p.scale = scale;
    }

    public boolean wasNull() throws SQLException {
        return outputParameters().wasNull();
    }

    public String getString(int parameterIndex) throws SQLException {
        return outputParameters().getString(parameterIndex);
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        return outputParameters().getBoolean(parameterIndex);
    }

    public byte getByte(int parameterIndex) throws SQLException {
        return outputParameters().getByte(parameterIndex);
    }

    public short getShort(int parameterIndex) throws SQLException {
        return outputParameters().getShort(parameterIndex);
    }

    public int getInt(int parameterIndex) throws SQLException {
        return outputParameters().getInt(parameterIndex);
    }

    public long getLong(int parameterIndex) throws SQLException {
        return outputParameters().getLong(parameterIndex);
    }

    public float getFloat(int parameterIndex) throws SQLException {
        return outputParameters().getFloat(parameterIndex);
    }

    public double getDouble(int parameterIndex) throws SQLException {
        return outputParameters().getDouble(parameterIndex);
    }

    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return outputParameters().getBigDecimal(parameterIndex);
    }

    public byte[] getBytes(int parameterIndex) throws SQLException {
        return  outputParameters().getBytes(parameterIndex);
    }

    public Date getDate(int parameterIndex) throws SQLException {
        return outputParameters().getDate(parameterIndex);
    }

    public Time getTime(int parameterIndex) throws SQLException {
        return outputParameters().getTime(parameterIndex);
    }

    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return outputParameters().getTimestamp(parameterIndex);
    }

    public Object getObject(int parameterIndex) throws SQLException {
        switch(params[parameterIndex].outputSQLType) {
           case Types.OTHER:
               return getObject(parameterIndex);
           case Types.ARRAY:
               return getArray(parameterIndex);
           case Types.BIGINT:
               return getLong(parameterIndex);
           case Types.BINARY:
               return getBytes(parameterIndex);
           case Types.BIT:
               return getInt(parameterIndex);
           case Types.BOOLEAN:
               return getBoolean(parameterIndex);
           case Types.CHAR:
               return getString(parameterIndex);
           case Types.CLOB:
               return getClob(parameterIndex);
           case Types.DATALINK:
               return getString(parameterIndex);
           case Types.DATE:
               return getDate(parameterIndex);
           case Types.DECIMAL:
               return getBigDecimal(parameterIndex);
           case Types.DISTINCT:
               return getString(parameterIndex);
           case Types.DOUBLE:
               return getDouble(parameterIndex);
           case Types.INTEGER:
               return getInt(parameterIndex);
           case Types.JAVA_OBJECT:
               return getObject(parameterIndex);
           case Types.LONGNVARCHAR:
               return getString(parameterIndex);
           case Types.LONGVARBINARY:
               return getBytes(parameterIndex);
           case Types.LONGVARCHAR:
               return getString(parameterIndex);
           case Types.NCHAR:
               return getString(parameterIndex);
           case Types.NCLOB:
               return getNClob(parameterIndex);
           case Types.NULL:
               return null;
           case Types.NUMERIC:
               return getBigDecimal(parameterIndex);
           case Types.NVARCHAR:
               return getString(parameterIndex);
           case Types.REAL:
               return getDouble(parameterIndex);
           case Types.REF:
               return getRef(parameterIndex);
           case Types.ROWID:
               return getRowId(parameterIndex);
           case Types.SMALLINT:
               return getShort(parameterIndex);
           case Types.SQLXML:
               return getSQLXML(parameterIndex);
           case Types.STRUCT:
               return getBytes(parameterIndex);
           case Types.TIME:
               return getTime(parameterIndex);
           case Types.TIMESTAMP:
               return getTimestamp(parameterIndex);
           case Types.TINYINT:
               return getByte(parameterIndex);
           case Types.VARBINARY:
               return getBytes(parameterIndex);
           case Types.VARCHAR:
               return getString(parameterIndex);

        }
        return outputParameters().getObject(parameterIndex);
    }

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return outputParameters().getBigDecimal(parameterIndex);
    }

    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return outputParameters().getObject(parameterIndex, map);
    }

    public Ref getRef(int parameterIndex) throws SQLException {
        return outputParameters().getRef(parameterIndex);
    }

    public Blob getBlob(int parameterIndex) throws SQLException {
        return outputParameters().getBlob(parameterIndex);
    }

    public Clob getClob(int parameterIndex) throws SQLException {
        return outputParameters().getClob(parameterIndex);
    }

    public Array getArray(int parameterIndex) throws SQLException {
        return outputParameters().getArray(parameterIndex);
    }

    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return outputParameters().getDate(parameterIndex, cal);
    }

    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return outputParameters().getTime(parameterIndex, cal);
    }

    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return outputParameters().getTimestamp(parameterIndex, cal);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        CallParameter p = getParameter(parameterIndex);
        p.sqlType = sqlType;
        p.typeName = typeName;
    }

    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        registerOutParameter(nameToIndex(parameterName), sqlType);
    }

    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        registerOutParameter(nameToIndex(parameterName), sqlType, scale);
    }

    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        registerOutParameter(nameToIndex(parameterName), sqlType, typeName);
    }

    public URL getURL(int parameterIndex) throws SQLException {
        return outputParameters().getURL(parameterIndex);
    }

    public void setURL(String parameterName, URL val) throws SQLException {
        setURL(nameToIndex(parameterName), val);
    }

    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(nameToIndex(parameterName), sqlType);
    }

    public void setBoolean(String parameterName, boolean x) throws SQLException {
        setBoolean(nameToIndex(parameterName), x);
    }

    public void setByte(String parameterName, byte x) throws SQLException {
        setByte(nameToIndex(parameterName), x);
    }

    public void setShort(String parameterName, short x) throws SQLException {
        setShort(nameToIndex(parameterName), x);
    }

    public void setInt(String parameterName, int x) throws SQLException {
        setInt(nameToIndex(parameterName), x);
    }

    public void setLong(String parameterName, long x) throws SQLException {
        setLong(nameToIndex(parameterName), x);
    }

    public void setFloat(String parameterName, float x) throws SQLException {
        setFloat(nameToIndex(parameterName), x);
    }

    public void setDouble(String parameterName, double x) throws SQLException {
        setDouble(nameToIndex(parameterName),x);
    }

    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        setBigDecimal(nameToIndex(parameterName), x);
    }

    public void setString(String parameterName, String x) throws SQLException {
        setString(nameToIndex(parameterName), x);
    }

    public void setBytes(String parameterName, byte[] x) throws SQLException {
        setBytes(nameToIndex(parameterName), x);
    }

    public void setDate(String parameterName, Date x) throws SQLException {
        setDate(nameToIndex(parameterName), x);
    }

    public void setTime(String parameterName, Time x) throws SQLException {
        setTime(nameToIndex(parameterName), x);
    }

    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        setTimestamp(nameToIndex(parameterName), x);
    }

    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        setAsciiStream(nameToIndex(parameterName), x, length);
    }

    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        setBinaryStream(nameToIndex(parameterName), x, length);
    }

    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        setObject(nameToIndex(parameterName), x, targetSqlType, scale);
    }

    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        setObject(nameToIndex(parameterName), x, targetSqlType);
    }

    public void setObject(String parameterName, Object x) throws SQLException {
        setObject(nameToIndex(parameterName), x);
    }

    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        setCharacterStream(nameToIndex(parameterName), reader, length);
    }

    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        setDate(nameToIndex(parameterName), x, cal);
    }

    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        setTime(nameToIndex(parameterName), x , cal);
    }

    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(nameToIndex(parameterName), x, cal);
    }

    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setNull(nameToIndex(parameterName), sqlType, typeName);
    }

    public String getString(String parameterName) throws SQLException {
        return getString(nameToIndex(parameterName));
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        return getBoolean(nameToIndex(parameterName));
    }

    public byte getByte(String parameterName) throws SQLException {
        return getByte(nameToIndex(parameterName));
    }

    public short getShort(String parameterName) throws SQLException {
        return getShort(nameToIndex(parameterName));
    }

    public int getInt(String parameterName) throws SQLException {
        return getInt(nameToIndex(parameterName));
    }

    public long getLong(String parameterName) throws SQLException {
        return getLong(nameToIndex(parameterName));
    }

    public float getFloat(String parameterName) throws SQLException {
        return getFloat(nameToIndex(parameterName));
    }

    public double getDouble(String parameterName) throws SQLException {
        return getDouble(nameToIndex(parameterName));
    }

    public byte[] getBytes(String parameterName) throws SQLException {
        return getBytes(nameToIndex(parameterName));
    }

    public Date getDate(String parameterName) throws SQLException {
        return getDate(nameToIndex(parameterName));
    }

    public Time getTime(String parameterName) throws SQLException {
        return getTime(nameToIndex(parameterName));
    }

    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return getTimestamp(nameToIndex(parameterName));
    }

    public Object getObject(String parameterName) throws SQLException {
        return getObject(nameToIndex(parameterName));
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getBigDecimal(nameToIndex(parameterName));
    }

    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return getObject(nameToIndex(parameterName), map);
    }

    public Ref getRef(String parameterName) throws SQLException {
        return getRef(nameToIndex(parameterName));
    }

    public Blob getBlob(String parameterName) throws SQLException {
        return getBlob(nameToIndex(parameterName));
    }

    public Clob getClob(String parameterName) throws SQLException {
        return getClob(nameToIndex(parameterName));
    }

    public Array getArray(String parameterName) throws SQLException {
        return getArray(nameToIndex(parameterName));
    }

    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getDate(nameToIndex(parameterName), cal);
    }

    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getTime(nameToIndex(parameterName), cal);
    }

    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getTimestamp(nameToIndex(parameterName), cal);
    }

    public URL getURL(String parameterName) throws SQLException {
        return getURL(nameToIndex(parameterName));
    }

    public RowId getRowId(int parameterIndex) throws SQLException {
        return outputParameters().getRowId(parameterIndex);
    }

    public RowId getRowId(String parameterName) throws SQLException {
        return getRowId(nameToIndex(parameterName));
    }

    public void setRowId(String parameterName, RowId x) throws SQLException {
        setRowId(nameToIndex(parameterName), x);
    }

    public void setNString(String parameterName, String value) throws SQLException {
        setNString(nameToIndex(parameterName), value);
    }

    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        setNCharacterStream(nameToIndex(parameterName), value, length);
    }

    public void setNClob(String parameterName, NClob value) throws SQLException {
        setNClob(nameToIndex(parameterName), value);
    }

    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        setClob(nameToIndex(parameterName), reader, length);
    }

    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        setBlob(nameToIndex(parameterName),inputStream, length);
    }

    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        setNClob(nameToIndex(parameterName),reader, length);
    }

    public NClob getNClob(int parameterIndex) throws SQLException {
        return outputParameters().getNClob(parameterIndex);
    }

    public NClob getNClob(String parameterName) throws SQLException {
        return getNClob(nameToIndex(parameterName));
    }

    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        setSQLXML(nameToIndex(parameterName), xmlObject);
    }

    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return outputParameters().getSQLXML(parameterIndex);
    }

    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return getSQLXML(nameToIndex(parameterName));
    }

    public String getNString(int parameterIndex) throws SQLException {
        return outputParameters().getNString(parameterIndex);
    }

    public String getNString(String parameterName) throws SQLException {
        return getNString(nameToIndex(parameterName));
    }

    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return  outputParameters().getNCharacterStream(parameterIndex);
    }

    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return getNCharacterStream(nameToIndex(parameterName));
    }

    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return outputParameters().getCharacterStream(parameterIndex);

    }

    public Reader getCharacterStream(String parameterName) throws SQLException {
        return getCharacterStream(nameToIndex(parameterName));
    }

    public void setBlob(String parameterName, Blob x) throws SQLException {
        inputParameters().setBlob(nameToIndex(parameterName), x);
    }

    public void setClob(String parameterName, Clob x) throws SQLException {
        inputParameters().setClob(nameToIndex(parameterName), x);
    }

    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        inputParameters().setAsciiStream(nameToIndex(parameterName), x, length);
    }

    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        inputParameters().setBinaryStream(nameToIndex(parameterName), x , length);
    }

    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        inputParameters().setCharacterStream(nameToIndex(parameterName), reader, length);
    }

    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        inputParameters().setAsciiStream(nameToIndex(parameterName),x);
    }

    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        inputParameters().setBinaryStream(nameToIndex(parameterName), x);
    }

    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        inputParameters().setCharacterStream(nameToIndex(parameterName), reader);
    }

    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        inputParameters().setNCharacterStream(nameToIndex(parameterName), value);
    }

    public void setClob(String parameterName, Reader reader) throws SQLException {
        inputParameters().setClob(nameToIndex(parameterName), reader);
    }

    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        inputParameters().setBlob(nameToIndex(parameterName), inputStream);
    }

    public void setNClob(String parameterName, Reader reader) throws SQLException {
        inputParameters().setNClob(nameToIndex(parameterName), reader);
    }

    public ResultSet executeQuery() throws SQLException {
        if (rsOutputParameters != null) {
            rsOutputParameters.close();
            rsOutputParameters = null;
        }
        if(parametersCount > 0)  {
            preparedStatement.execute();
        }
        return callStatement.executeQuery(callQuery);
    }

    public int executeUpdate() throws SQLException {
        if (rsOutputParameters != null) {
            rsOutputParameters.close();
            rsOutputParameters = null;
        }
        if(parametersCount > 0)  {
            preparedStatement.execute();
        }
        return callStatement.executeUpdate(callQuery);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        inputParameters().setNull(parameterIndex, sqlType);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        inputParameters().setBoolean(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        inputParameters().setByte(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        inputParameters().setShort(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        inputParameters().setInt(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        inputParameters().setLong(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        inputParameters().setFloat(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        inputParameters().setDouble(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        inputParameters().setBigDecimal(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        inputParameters().setString(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        inputParameters().setBytes(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        inputParameters().setDate(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        inputParameters().setTime(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        inputParameters().setTimestamp(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        inputParameters().setAsciiStream(parameterIndex, x, length);
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        inputParameters().setBinaryStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        inputParameters().setBinaryStream(parameterIndex, x , length);
    }

    public void clearParameters() throws SQLException {
        inputParameters().clearParameters();
        for(int i=1; i <= parametersCount; i++){
            inputParameters().setNull(i, Types.NULL);
        }
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        inputParameters().setObject(parameterIndex, x, targetSqlType);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        inputParameters().setObject(parameterIndex, x);
    }

    public boolean execute() throws SQLException {
        if (rsOutputParameters != null) {
            rsOutputParameters.close();
            rsOutputParameters = null;
        }
        if(parametersCount > 0)  {
            preparedStatement.execute();
        }
        return callStatement.execute(callQuery);
    }

    public void addBatch() throws SQLException {
        if (preparedStatement == null) {
            preparedStatement = con.prepareStatement(callQuery);
            preparedStatement.addBatch();
        }
        else if (parametersCount == 0) {
            preparedStatement.addBatch();
        }
        else {
            /* add statement to set input parameters, mark it as ignorable */
            preparedStatement.addBatch();
            batchIgnoreResult.set(batchCount);
            batchCount++;

            preparedStatement.addBatch(callQuery);
            batchCount++;
        }
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        inputParameters().setCharacterStream(parameterIndex, reader, length);
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        inputParameters().setRef(parameterIndex, x);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        inputParameters().setBlob(parameterIndex, x);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        inputParameters().setClob(parameterIndex, x);
    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        inputParameters().setArray(parameterIndex, x);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSet rs = callStatement.getResultSet();
        if (rs != null)
            return rs.getMetaData();
        return null;
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        inputParameters().setDate(parameterIndex, x, cal);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        inputParameters().setTime(parameterIndex, x, cal);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        inputParameters().setTimestamp(parameterIndex, x, cal);
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        inputParameters().setNull(parameterIndex, sqlType, typeName);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        inputParameters().setURL(parameterIndex, x);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        parameterMetadata.readMetadataFromDBIfRequired();
        return parameterMetadata;
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        inputParameters().setRowId(parameterIndex, x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        inputParameters().setNString(parameterIndex, value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        inputParameters().setNCharacterStream(parameterIndex, value, length);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        inputParameters().setNClob(parameterIndex, value);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        inputParameters().setClob(parameterIndex, reader, length);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        inputParameters().setBlob(parameterIndex, inputStream, length);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        inputParameters().setNClob(parameterIndex, reader, length);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        inputParameters().setSQLXML(parameterIndex, xmlObject);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setAsciiStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBinaryStream(parameterIndex, x, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setAsciiStream(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setBinaryStream(parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setNCharacterStream(parameterIndex, value);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setClob(parameterIndex, reader);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setBlob(parameterIndex, inputStream);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setNClob(parameterIndex, reader);
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return callStatement.executeQuery(sql);
    }

    public int executeUpdate(String sql) throws SQLException {
        return callStatement.executeUpdate(sql);
    }

    public void close() throws SQLException {

        if(preparedStatement != null)
        {
            preparedStatement.close();
            preparedStatement = null;
        }
        if(rsOutputParameters != null) {
            rsOutputParameters.close();
            rsOutputParameters = null;
        }

        if (callStatement == null) {
            callStatement.close();
            callStatement = null;
        }
    }

    public int getMaxFieldSize() throws SQLException {
        return callStatement.getMaxFieldSize();
    }

    public void setMaxFieldSize(int max) throws SQLException {
        callStatement.setMaxFieldSize(max);
    }

    public int getMaxRows() throws SQLException {
        return callStatement.getMaxRows();
    }

    public void setMaxRows(int max) throws SQLException {
        callStatement.setMaxRows(max);
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        callStatement.setEscapeProcessing(enable);
    }

    public int getQueryTimeout() throws SQLException {
        return callStatement.getQueryTimeout();
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        callStatement.setQueryTimeout(seconds);
    }

    public void cancel() throws SQLException {
        callStatement.cancel();
    }

    public SQLWarning getWarnings() throws SQLException {
        return callStatement.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        callStatement.clearWarnings();
    }

    public void setCursorName(String name) throws SQLException {
        callStatement.setCursorName(name);
    }

    public boolean execute(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement does not support execute(String sql)");
    }

    public ResultSet getResultSet() throws SQLException {
        return callStatement.getResultSet();
    }

    public int getUpdateCount() throws SQLException {
        return callStatement.getUpdateCount();
    }

    public boolean getMoreResults() throws SQLException {
        return callStatement.getMoreResults();
    }

    public void setFetchDirection(int direction) throws SQLException {
        callStatement.setFetchDirection(direction);
    }

    public int getFetchDirection() throws SQLException {
        return callStatement.getFetchDirection();
    }

    public void setFetchSize(int rows) throws SQLException {
        callStatement.setFetchSize(rows);
    }

    public int getFetchSize() throws SQLException {
        return callStatement.getFetchSize();
    }

    public int getResultSetConcurrency() throws SQLException {
        return callStatement.getResultSetConcurrency();
    }

    public int getResultSetType() throws SQLException {
        return callStatement.getResultSetType();
    }

    public void addBatch(String sql) throws SQLException {
        if (parametersCount == 0) {
            if (preparedStatement == null) {
                preparedStatement = con.prepareStatement(sql);
                preparedStatement.addBatch();
            }
        }
        else {
            preparedStatement.addBatch(sql);
            batchCount++;
        }
    }

    public void clearBatch() throws SQLException {
        if(preparedStatement != null) {
            preparedStatement.clearBatch();
            if(parametersCount == 0) {
                preparedStatement.close();
                preparedStatement = null;
            }
        }
        batchCount = 0;
        batchIgnoreResult.clear();
    }

    public int[] executeBatch() throws SQLException {
        if (preparedStatement != null) {
            int [] unfilteredResult = preparedStatement.executeBatch();
            if(batchIgnoreResult.cardinality() == 0) {
                return unfilteredResult;
            }
            int [] filteredResult = new int[unfilteredResult.length - batchIgnoreResult.cardinality()];
            int index=0;
            for(int i=0; i< unfilteredResult.length; i++) {
                if(!batchIgnoreResult.get(i)) {
                    filteredResult[index++] = unfilteredResult[i];
                }
            }
            return filteredResult;
        }
        else {
            return new int[0];
        }
    }

    public Connection getConnection() throws SQLException {
        return con;
    }

    public boolean getMoreResults(int current) throws SQLException {
        return callStatement.getMoreResults(current);
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return callStatement.getGeneratedKeys();
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return callStatement.executeUpdate(sql, autoGeneratedKeys);
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return callStatement.executeUpdate(sql, columnIndexes);
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return callStatement.executeUpdate(sql, columnNames);
    }


    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return callStatement.execute(sql, autoGeneratedKeys);
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return callStatement.execute(sql, columnIndexes);
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return callStatement.execute(sql, columnNames);
    }

    public int getResultSetHoldability() throws SQLException {
        return callStatement.getResultSetHoldability();
    }

    public boolean isClosed() throws SQLException {
        return callStatement.isClosed();
    }

    public void setPoolable(boolean poolable) throws SQLException {

    }

    public boolean isPoolable() throws SQLException {
        return false;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
}