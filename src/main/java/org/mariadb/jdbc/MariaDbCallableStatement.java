package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.util.Utils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;




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
public class MariaDbCallableStatement implements CallableStatement {
    /**
     * Pattern  to check the correctness of callable statement query string
     * Legal queries, as documented in JDK have the form:
     * {[?=]call[(arg1,..,,argn)]}
     */
    private static Pattern CALLABLE_STATEMENT_PATTERN =
            Pattern.compile("^\\s*\\{?\\s*(\\?\\s*=)?\\s*call\\s*([\\w.]+)(\\(.*\\))?\\s*}?", Pattern.CASE_INSENSITIVE);
    /**
     * Database connection.
     */
    private MariaDbConnection con;
    /**
     * Prepared statement, typically used to set input variables, in which case it has the form
     * set _jdbc_var1=?,...,jdbc_var_N=?
     * Also, handles addBatch/executeBatch() statements, even if there is no input parameters.
     */
    private PreparedStatement preparedStatement;
    /**
     * Current count of batch statements.
     */
    private int batchCount;
    /**
     * In case parameters are used, some results of the batch statement need to be ignored, since we generate
     * extra statements for setting input parameters. batchIgnoreResult is a bitmap for ignored results.
     */
    private BitSet batchIgnoreResult;
    /**
     * Resolved query with ? placeholders replaces by user variables ,
     * e.g {call proc(?,?)} would be resolved as CALL proc(@_jdbc_var_1,@_jdbc_var_2)
     */
    private String callQuery;
    private Statement callStatement;
    /**
     * Result set containing output parameters.
     * We generate query SELECT @_jdbc_var1, ... @_jdbc_var_N to fetch the results.
     */
    private ResultSet rsOutputParameters;
    /**
     * Information about parameters, merely from registerOutputParameter() and setXXX() calls.
     */
    private CallParameter[] params;
    private CallableParameterMetaData parameterMetadata;
    private int parametersCount;

    /**
     * Constructor.
     * @param connection current connection
     * @param query query
     * @throws SQLException exception
     */
    public MariaDbCallableStatement(MariaDbConnection connection, String query) throws SQLException {
        con = connection;

        query = Utils.nativeSql(query, connection.noBackslashEscapes);
        batchIgnoreResult = new BitSet();
        Matcher matcher = CALLABLE_STATEMENT_PATTERN.matcher(query);
        if (!matcher.matches()) {
            throw new SQLSyntaxErrorException("invalid callable syntax");
        }

        //
        boolean isFunction = (matcher.group(1) != null);
        String procedureName = matcher.group(2);
        String arguments = matcher.group(3);
        if (!isFunction) {
            // real stored procedure, generate "CALL PROC(args)", with ? in args replaces by variables
            callQuery = "call " + procedureName + resolveArguments(arguments, 1);
        } else {
            // function returning result, generate "SELECT FUNC(args) into @_jdbc_var_1
            callQuery = "select " + procedureName + resolveArguments(arguments, 2) + " into " + getVariableName(1);
        }
        callStatement = con.createStatement();

        // Generate set _jdbc_var1=?,....set jdbc_var_n=? prepared statement
        if (parametersCount != 0) {
            StringBuffer sb = new StringBuffer("set ");
            for (int i = 1; i <= parametersCount; i++) {
                if (i > 1) {
                    sb.append(",");
                }
                sb.append(getVariableName(i));
                sb.append("=?");
            }
            preparedStatement = con.prepareStatement(sb.toString());
            for (int i = 1; i <= parametersCount; i++) {
                preparedStatement.setNull(i, Types.NULL);
            }
        }
        params = new CallParameter[parametersCount + 1];
        for (int i = 1; i <= parametersCount; i++) {
            params[i] = new CallParameter();
        }
        if (isFunction) {
            // the query was in the form {?=call function()}, so the first parameter is always output
            params[1].isOutput = true;
        }
        parameterMetadata = new CallableParameterMetaData(params, con, procedureName, isFunction);
    }

    boolean hasOutputParameters() {
        for (int i = 1; i < params.length; i++) {
            if (params[i].isOutput) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return user variable name corresponding to a parameter.
     *
     * @param index index
     * @return use variable name
     */
    private String getVariableName(int index) {
        return "@_jdbc_var_" + index;
    }


    void readOutputParameters() throws SQLException {
        if (callStatement.getFetchSize() == Integer.MIN_VALUE) {
            // For streaming queries
            // make sure there are no more results left from the call statement
            while (callStatement.getMoreResults()) {
            }
            ;
        }

        StringBuffer sb = new StringBuffer("SELECT ");
        for (int i = 1; i <= parametersCount; i++) {
            if (i != 1) {
                sb.append(",");
            }
            if (!params[i].isOutput) {
                sb.append("NULL");
            } else {
                sb.append(getVariableName(i));
            }
        }
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(sb.toString());
        rs.next();
        rsOutputParameters = rs;
    }

    /**
     * Fetch output variables
     *
     * @return ResultSet from "select @_jdbc_var_1...,@_jdbc_var_N
     * @throws SQLException exception
     */
    ResultSet outputParameters() throws SQLException {
        if (parametersCount == 0) {
            throw new SQLException("no output parameters");
        }
        if (rsOutputParameters == null) {
            readOutputParameters();
        }
        return rsOutputParameters;
    }

    /**
     * Get prepared statement to set input parameters
     *
     * @return PreparedStatement (for query in the form SET @_jdbc_var_1=?, ...., _jdbc_var_N=?)
     * @throws SQLException exception
     */
    PreparedStatement inputParameters() throws SQLException {
        if (parametersCount == 0) {
            throw new SQLException("no input parameters");
        }
        return preparedStatement;
    }

    /**
     * Convert parameter name to parameter index in the query.
     *
     * @param parameterName name
     * @return index
     * @throws SQLException exception
     */
    private int nameToIndex(String parameterName) throws SQLException {
        if (callStatement != null) {
            while (callStatement.getMoreResults()) {
            }
        }
        for (int i = 1; i <= parameterMetadata.getParameterCount(); i++) {
            String name = parameterMetadata.getName(i);
            if (name != null && name.equalsIgnoreCase(parameterName)) {
                return i;
            }
        }
        throw new SQLException("there is no parameter with the name " + parameterName);
    }

    /**
     * Replace placeholder in query string with corresponding user variables.
     * Takes case of string literal and comments
     *
     * @param args arguments
     * @param startingIndex (1 for stored procedure, 2 for stored function)
     * @return String
     */
    String resolveArguments(String args, int startingIndex) {
        if (args == null) {
            parametersCount = 0;
            return "()";
        }

        StringBuffer sb = new StringBuffer();
        int index = startingIndex;

        boolean inQuote = false;
        boolean inComment = false;
        char quoteChar = 0;
        char prevChar = 0;
        boolean slashStarComment = false;
        boolean inEscape = false;

        for (char c : args.toCharArray()) {
            if (c == '\\' && !inComment && !inQuote) {
                inEscape = !inEscape;
            }
            if (inEscape) {
                sb.append(c);
                inEscape = false;
                continue;
            }

            switch (c) {
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
                    if (!inComment) {
                        if (inQuote) {
                            if (quoteChar == c) {
                                inQuote = false;
                            }
                        } else {
                            inQuote = true;
                            quoteChar = c;
                        }
                    }
                    sb.append(c);
                    break;
                case '*':
                    if (prevChar == '/' && !inQuote) {
                        inComment = true;
                        slashStarComment = true;
                    }
                    sb.append(c);
                    break;
                case '/':
                    if (prevChar == '*' && inComment && slashStarComment) {
                        inComment = false;
                        slashStarComment = false;
                    } else if (prevChar == '/' && !inQuote) {
                        inComment = true;
                    }
                    sb.append(c);
                    break;
                case '\n':
                    if (inComment && !slashStarComment) {
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


    CallParameter getParameter(int index) throws SQLException {
        if (index > params.length || index < 1) {
            throw new SQLException("No parameter with index " + index);
        }
        return params[index];
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        registerOutParameter(parameterIndex, sqlType, -1);
    }
    /**
     * Registers the parameter in ordinal position
     * <code>parameterIndex</code> to be of JDBC type
     * <code>sqlType</code>. All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.
     * <p>
     * This version of <code>registerOutParameter</code> should be
     * used when the parameter is of JDBC type <code>NUMERIC</code>
     * or <code>DECIMAL</code>.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param sqlType the SQL type code defined by <code>java.sql.Types</code>.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @exception SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type
     * @see Types
     */
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        CallParameter callParameter = getParameter(parameterIndex);
        callParameter.isOutput = true;
        callParameter.outputSqlType = sqlType;
        callParameter.scale = scale;
    }
    /**
     * Registers the designated output parameter.
     * This version of
     * the method <code>registerOutParameter</code>
     * should be used for a user-defined or <code>REF</code> output parameter.  Examples
     * of user-defined types include: <code>STRUCT</code>, <code>DISTINCT</code>,
     * <code>JAVA_OBJECT</code>, and named array types.
     *<p>
     * All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>  For a user-defined parameter, the fully-qualified SQL
     * type name of the parameter should also be given, while a <code>REF</code>
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-defined and <code>REF</code> parameters.
     *
     * Although it is intended for user-defined and <code>REF</code> parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-defined or <code>REF</code> type, the
     * <i>typeName</i> parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the getter method whose Java type corresponds to the
     * parameter's registered SQL type.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @param sqlType a value from {@link java.sql.Types}
     * @param typeName the fully-qualified name of an SQL structured type
     * @exception SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type
     * @see Types
     */
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        CallParameter callParameter = getParameter(parameterIndex);
        callParameter.sqlType = sqlType;
        callParameter.typeName = typeName;
        callParameter.isOutput = true;
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


    public boolean wasNull() throws SQLException {
        return outputParameters().wasNull();
    }

    public String getString(int parameterIndex) throws SQLException {
        return outputParameters().getString(parameterIndex);
    }


    public String getString(String parameterName) throws SQLException {
        return getString(nameToIndex(parameterName));
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        return outputParameters().getBoolean(parameterIndex);
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        return getBoolean(nameToIndex(parameterName));
    }

    public byte getByte(int parameterIndex) throws SQLException {
        return outputParameters().getByte(parameterIndex);
    }


    public byte getByte(String parameterName) throws SQLException {
        return getByte(nameToIndex(parameterName));
    }

    public short getShort(int parameterIndex) throws SQLException {
        return outputParameters().getShort(parameterIndex);
    }


    public short getShort(String parameterName) throws SQLException {
        return getShort(nameToIndex(parameterName));
    }

    public int getInt(int parameterIndex) throws SQLException {
        return outputParameters().getInt(parameterIndex);
    }


    public int getInt(String parameterName) throws SQLException {
        return getInt(nameToIndex(parameterName));
    }

    public long getLong(int parameterIndex) throws SQLException {
        return outputParameters().getLong(parameterIndex);
    }


    public long getLong(String parameterName) throws SQLException {
        return getLong(nameToIndex(parameterName));
    }

    public float getFloat(int parameterIndex) throws SQLException {
        return outputParameters().getFloat(parameterIndex);
    }

    public float getFloat(String parameterName) throws SQLException {
        return getFloat(nameToIndex(parameterName));
    }

    public double getDouble(int parameterIndex) throws SQLException {
        return outputParameters().getDouble(parameterIndex);
    }

    public double getDouble(String parameterName) throws SQLException {
        return getDouble(nameToIndex(parameterName));
    }

    /**
     * Bigdecimal with scale is deprecated.
     * @deprecated use <code>getBigDecimal(int parameterIndex)</code>
     * or <code>getBigDecimal(String parameterName)</code>
     */
    @Deprecated
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return outputParameters().getBigDecimal(parameterIndex);
    }

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return outputParameters().getBigDecimal(parameterIndex);
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getBigDecimal(nameToIndex(parameterName));
    }


    public byte[] getBytes(int parameterIndex) throws SQLException {
        return outputParameters().getBytes(parameterIndex);
    }

    public byte[] getBytes(String parameterName) throws SQLException {
        return getBytes(nameToIndex(parameterName));
    }


    public Date getDate(int parameterIndex) throws SQLException {
        return outputParameters().getDate(parameterIndex);
    }

    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return outputParameters().getDate(parameterIndex, cal);
    }

    public Date getDate(String parameterName) throws SQLException {
        return getDate(nameToIndex(parameterName));
    }

    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getDate(nameToIndex(parameterName), cal);
    }


    public Time getTime(int parameterIndex) throws SQLException {
        return outputParameters().getTime(parameterIndex);
    }


    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return outputParameters().getTime(parameterIndex, cal);
    }

    public Time getTime(String parameterName) throws SQLException {
        return getTime(nameToIndex(parameterName));
    }

    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getTime(nameToIndex(parameterName), cal);
    }

    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return outputParameters().getTimestamp(parameterIndex);
    }


    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return outputParameters().getTimestamp(parameterIndex, cal);
    }

    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return getTimestamp(nameToIndex(parameterName));
    }

    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getTimestamp(nameToIndex(parameterName), cal);
    }

    /**
     * <p>Gets the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * an <code>Object</code> in the Java programming language.
     *
     * <p>This method will return the value of the given column as a
     * Java object.  The type of the Java object will be the default
     * Java object type corresponding to the column's SQL type,
     * following the mapping for built-in types specified in the JDBC
     * specification. If the value is an SQL <code>NULL</code>,
     * the driver returns a Java <code>null</code>.
     *
     * <p>This method may also be used to read database-specific
     * abstract data types.
     *
     * In the JDBC 2.0 API, the behavior of method
     * <code>getObject</code> is extended to materialize
     * data of SQL user-defined types.
     * <p>
     * If <code>Connection.getTypeMap</code> does not throw a
     * <code>SQLFeatureNotSupportedException</code>,
     * then when a column contains a structured or distinct value,
     * the behavior of this method is as
     * if it were a call to: <code>getObject(columnIndex,
     * this.getStatement().getConnection().getTypeMap())</code>.
     *
     * If <code>Connection.getTypeMap</code> does throw a
     * <code>SQLFeatureNotSupportedException</code>,
     * then structured values are not supported, and distinct values
     * are mapped to the default Java class as determined by the
     * underlying SQL type of the DISTINCT type.
     *
     * @param parameterIndex the first column is 1, the second is 2, ...
     * @return a <code>java.lang.Object</code> holding the column value
     * @exception SQLException if the columnIndex is not valid;
     * if a database access error occurs or this method is
     *            called on a closed result set
     */
    public Object getObject(int parameterIndex) throws SQLException {
        if (!params[parameterIndex].isOutput) {
            throw new SQLException("Parameter " + parameterIndex + " is not an output parameter");
        }
        switch (params[parameterIndex].outputSqlType) {
            case Types.OTHER:
                throw new SQLException("unexpected Type returned");
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
            default:
                return outputParameters().getObject(parameterIndex);
        }
    }

    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return outputParameters().getObject(parameterIndex, map);
    }

    public Object getObject(String parameterName) throws SQLException {
        return getObject(nameToIndex(parameterName));
    }

    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return getObject(nameToIndex(parameterName), map);
    }

    public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }




    public Ref getRef(int parameterIndex) throws SQLException {
        return outputParameters().getRef(parameterIndex);
    }

    public Ref getRef(String parameterName) throws SQLException {
        return getRef(nameToIndex(parameterName));
    }


    public Blob getBlob(int parameterIndex) throws SQLException {
        return outputParameters().getBlob(parameterIndex);
    }

    public Blob getBlob(String parameterName) throws SQLException {
        return getBlob(nameToIndex(parameterName));
    }

    public Clob getClob(int parameterIndex) throws SQLException {
        return outputParameters().getClob(parameterIndex);
    }

    public Clob getClob(String parameterName) throws SQLException {
        return getClob(nameToIndex(parameterName));
    }

    public Array getArray(int parameterIndex) throws SQLException {
        return outputParameters().getArray(parameterIndex);
    }

    public Array getArray(String parameterName) throws SQLException {
        return getArray(nameToIndex(parameterName));
    }








    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return outputParameters().getURL(parameterIndex);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return getURL(nameToIndex(parameterName));
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        setURL(nameToIndex(parameterName), val);
    }

    @Override
    public void setURL(int parameterIndex, URL url) throws SQLException {
        inputParameters().setURL(parameterIndex, url);
    }

    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(nameToIndex(parameterName), sqlType);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        inputParameters().setNull(parameterIndex, sqlType);
    }


    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        inputParameters().setNull(parameterIndex, sqlType, typeName);
    }

    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setNull(nameToIndex(parameterName), sqlType, typeName);
    }

    public void setBoolean(int parameterIndex, boolean bool) throws SQLException {
        inputParameters().setBoolean(parameterIndex, bool);
    }

    public void setBoolean(String parameterName, boolean bool) throws SQLException {
        setBoolean(nameToIndex(parameterName), bool);
    }

    public void setByte(String parameterName, byte value) throws SQLException {
        setByte(nameToIndex(parameterName), value);
    }

    public void setByte(int parameterIndex, byte value) throws SQLException {
        inputParameters().setByte(parameterIndex, value);
    }

    public void setShort(String parameterName, short value) throws SQLException {
        setShort(nameToIndex(parameterName), value);
    }

    public void setShort(int parameterIndex, short value) throws SQLException {
        inputParameters().setShort(parameterIndex, value);
    }

    public void setInt(String parameterName, int value) throws SQLException {
        setInt(nameToIndex(parameterName), value);
    }

    public void setInt(int parameterIndex, int value) throws SQLException {
        inputParameters().setInt(parameterIndex, value);
    }

    public void setLong(String parameterName, long value) throws SQLException {
        setLong(nameToIndex(parameterName), value);
    }

    public void setLong(int parameterIndex, long value) throws SQLException {
        inputParameters().setLong(parameterIndex, value);
    }

    public void setFloat(String parameterName, float value) throws SQLException {
        setFloat(nameToIndex(parameterName), value);
    }

    public void setFloat(int parameterIndex, float value) throws SQLException {
        inputParameters().setFloat(parameterIndex, value);
    }

    public void setDouble(String parameterName, double value) throws SQLException {
        setDouble(nameToIndex(parameterName), value);
    }

    public void setDouble(int parameterIndex, double value) throws SQLException {
        inputParameters().setDouble(parameterIndex, value);
    }

    public void setBigDecimal(String parameterName, BigDecimal bigDecimal) throws SQLException {
        setBigDecimal(nameToIndex(parameterName), bigDecimal);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal bigDecimal) throws SQLException {
        inputParameters().setBigDecimal(parameterIndex, bigDecimal);
    }

    public void setString(String parameterName, String str) throws SQLException {
        setString(nameToIndex(parameterName), str);
    }

    public void setString(int parameterIndex, String str) throws SQLException {
        inputParameters().setString(parameterIndex, str);
    }

    public void setBytes(String parameterName, byte[] bytes) throws SQLException {
        setBytes(nameToIndex(parameterName), bytes);
    }

    public void setBytes(int parameterIndex, byte[] bytes) throws SQLException {
        inputParameters().setBytes(parameterIndex, bytes);
    }

    public void setDate(String parameterName, Date date, Calendar cal) throws SQLException {
        setDate(nameToIndex(parameterName), date, cal);
    }

    public void setDate(int parameterIndex, Date date) throws SQLException {
        inputParameters().setDate(parameterIndex, date);
    }

    public void setDate(int parameterIndex, Date date, Calendar cal) throws SQLException {
        inputParameters().setDate(parameterIndex, date, cal);
    }

    public void setDate(String parameterName, Date date) throws SQLException {
        setDate(nameToIndex(parameterName), date);
    }

    public void setTime(String parameterName, Time time, Calendar cal) throws SQLException {
        setTime(nameToIndex(parameterName), time, cal);
    }

    public void setTime(int parameterIndex, Time time) throws SQLException {
        inputParameters().setTime(parameterIndex, time);
    }

    public void setTime(int parameterIndex, Time time, Calendar cal) throws SQLException {
        inputParameters().setTime(parameterIndex, time, cal);
    }

    public void setTime(String parameterName, Time time) throws SQLException {
        setTime(nameToIndex(parameterName), time);
    }

    public void setTimestamp(String parameterName, Timestamp timestamp) throws SQLException {
        setTimestamp(nameToIndex(parameterName), timestamp);
    }

    public void setTimestamp(String parameterName, Timestamp timestamp, Calendar cal) throws SQLException {
        setTimestamp(nameToIndex(parameterName), timestamp, cal);
    }

    public void setTimestamp(int parameterIndex, Timestamp timestamp) throws SQLException {
        inputParameters().setTimestamp(parameterIndex, timestamp);
    }

    public void setTimestamp(int parameterIndex, Timestamp timestamp, Calendar cal) throws SQLException {
        inputParameters().setTimestamp(parameterIndex, timestamp, cal);
    }



    public void setAsciiStream(String parameterName, InputStream stream, int length) throws SQLException {
        setAsciiStream(nameToIndex(parameterName), stream, length);
    }

    public void setAsciiStream(String parameterName, InputStream stream, long length) throws SQLException {
        inputParameters().setAsciiStream(nameToIndex(parameterName), stream, length);
    }

    public void setAsciiStream(String parameterName, InputStream stream) throws SQLException {
        inputParameters().setAsciiStream(nameToIndex(parameterName), stream);
    }

    public void setAsciiStream(int parameterIndex, InputStream stream, int length) throws SQLException {
        inputParameters().setAsciiStream(parameterIndex, stream, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream stream, long length) throws SQLException {
        setAsciiStream(parameterIndex, stream, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream stream) throws SQLException {
        setAsciiStream(parameterIndex, stream);
    }

    public void setBinaryStream(String parameterName, InputStream stream, int length) throws SQLException {
        setBinaryStream(nameToIndex(parameterName), stream, length);
    }

    public void setBinaryStream(String parameterName, InputStream stream, long length) throws SQLException {
        inputParameters().setBinaryStream(nameToIndex(parameterName), stream, length);
    }

    public void setBinaryStream(String parameterName, InputStream stream) throws SQLException {
        inputParameters().setBinaryStream(nameToIndex(parameterName), stream);
    }

    public void setBinaryStream(int parameterIndex, InputStream stream, int length) throws SQLException {
        inputParameters().setBinaryStream(parameterIndex, stream, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream stream, long length) throws SQLException {
        setBinaryStream(parameterIndex, stream, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream stream) throws SQLException {
        setBinaryStream(parameterIndex, stream);
    }

    public void setObject(String parameterName, Object obj, int targetSqlType, int scale) throws SQLException {
        setObject(nameToIndex(parameterName), obj, targetSqlType, scale);
    }

    public void setObject(String parameterName, Object obj, int targetSqlType) throws SQLException {
        setObject(nameToIndex(parameterName), obj, targetSqlType);
    }

    public void setObject(String parameterName, Object obj) throws SQLException {
        setObject(nameToIndex(parameterName), obj);
    }


    public void setObject(int parameterIndex, Object obj, int targetSqlType) throws SQLException {
        inputParameters().setObject(parameterIndex, obj, targetSqlType);
    }

    public void setObject(int parameterIndex, Object obj) throws SQLException {
        inputParameters().setObject(parameterIndex, obj);
    }

    public void setObject(int parameterIndex, Object obj, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, obj, targetSqlType, scaleOrLength);
    }

    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        setCharacterStream(nameToIndex(parameterName), reader, length);
    }

    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        inputParameters().setCharacterStream(nameToIndex(parameterName), reader, length);
    }

    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        inputParameters().setCharacterStream(nameToIndex(parameterName), reader);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        inputParameters().setCharacterStream(parameterIndex, reader, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }









    public RowId getRowId(int parameterIndex) throws SQLException {
        return outputParameters().getRowId(parameterIndex);
    }

    public RowId getRowId(String parameterName) throws SQLException {
        return getRowId(nameToIndex(parameterName));
    }

    public void setRowId(String parameterName, RowId rowid) throws SQLException {
        setRowId(nameToIndex(parameterName), rowid);
    }

    public void setRowId(int parameterIndex, RowId rowid) throws SQLException {
        inputParameters().setRowId(parameterIndex, rowid);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        setNString(nameToIndex(parameterName), value);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        inputParameters().setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        setNCharacterStream(nameToIndex(parameterName), value, length);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        inputParameters().setNCharacterStream(nameToIndex(parameterName), value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        inputParameters().setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        setNClob(nameToIndex(parameterName), value);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        setNClob(nameToIndex(parameterName), reader, length);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        inputParameters().setNClob(nameToIndex(parameterName), reader);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        inputParameters().setNClob(parameterIndex, value);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        inputParameters().setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setNClob(parameterIndex, reader);
    }

    public void setClob(int parameterIndex, Clob clob) throws SQLException {
        inputParameters().setClob(parameterIndex, clob);
    }

    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        setClob(nameToIndex(parameterName), reader, length);
    }

    public void setClob(String parameterName, Clob clob) throws SQLException {
        inputParameters().setClob(nameToIndex(parameterName), clob);
    }

    public void setClob(String parameterName, Reader reader) throws SQLException {
        inputParameters().setClob(nameToIndex(parameterName), reader);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        inputParameters().setClob(parameterIndex, reader, length);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setClob(parameterIndex, reader);
    }


    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        setBlob(nameToIndex(parameterName), inputStream, length);
    }

    public void setBlob(String parameterName, Blob blob) throws SQLException {
        inputParameters().setBlob(nameToIndex(parameterName), blob);
    }

    public void setBlob(int parameterIndex, Blob blob) throws SQLException {
        inputParameters().setBlob(parameterIndex, blob);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        inputParameters().setBlob(parameterIndex, inputStream, length);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setBlob(parameterIndex, inputStream);
    }

    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        inputParameters().setBlob(nameToIndex(parameterName), inputStream);
    }


    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return outputParameters().getNClob(parameterIndex);
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return getNClob(nameToIndex(parameterName));
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        setSQLXML(nameToIndex(parameterName), xmlObject);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        inputParameters().setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return outputParameters().getSQLXML(parameterIndex);
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return getSQLXML(nameToIndex(parameterName));
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return outputParameters().getNString(parameterIndex);
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return getNString(nameToIndex(parameterName));
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return outputParameters().getNCharacterStream(parameterIndex);
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return getNCharacterStream(nameToIndex(parameterName));
    }

    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return outputParameters().getCharacterStream(parameterIndex);

    }

    public Reader getCharacterStream(String parameterName) throws SQLException {
        return getCharacterStream(nameToIndex(parameterName));
    }








    /**
     * Executes the SQL statement in this <code>PreparedStatement</code> object,
     * which may be any kind of SQL statement.
     * Some prepared statements return multiple results; the <code>execute</code>
     * method handles these complex statements as well as the simpler
     * form of statements handled by the methods <code>executeQuery</code>
     * and <code>executeUpdate</code>.
     * <P>
     * The <code>execute</code> method returns a <code>boolean</code> to
     * indicate the form of the first result.  You must call either the method
     * <code>getResultSet</code> or <code>getUpdateCount</code>
     * to retrieve the result; you must call <code>getInternalMoreResults</code> to
     * move to any subsequent result(s).
     *
     * @return <code>true</code> if the first result is a <code>ResultSet</code>
     *         object; <code>false</code> if the first result is an update
     *         count or there is no result
     * @exception SQLException if a database access error occurs;
     * this method is called on a closed <code>PreparedStatement</code>
     * or an argument is supplied to this method
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the {@code setQueryTimeout}
     * method has been exceeded and has at least attempted to cancel
     * the currently running {@code Statement}
     * @see Statement#execute
     * @see Statement#getResultSet
     * @see Statement#getUpdateCount
     * @see Statement#getMoreResults

     */
    public ResultSet executeQuery() throws SQLException {
        if (execute()) {
            return getResultSet();
        }
        throw new SQLException("CallableStatement.executeQuery() did not return a resultset", "HY000");
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return callStatement.executeQuery(sql);
    }

    /**
     *  Retrieves the current result as an update count;
     *  if the result is a <code>ResultSet</code> object or there are no more results, -1
     *  is returned. This method should be called only once per result.
     *
     * @return the current result as an update count; -1 if the current result is a
     * <code>ResultSet</code> object or there are no more results
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>Statement</code>
     * @see #execute
     */
    public int executeUpdate() throws SQLException {
        if (!execute()) {
            return getUpdateCount();
        }
        throw new SQLException("CallableStatement.executeUpdate() returned a resultset set", "HY000");
    }

    public int executeUpdate(String sql) throws SQLException {
        return callStatement.executeUpdate(sql, Statement.NO_GENERATED_KEYS);
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

    /**
     * Set unicode Stream.
     * @deprecated Use {@code setCharacterStream}
     */
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream stream, int length) throws SQLException {
        inputParameters().setBinaryStream(parameterIndex, stream, length);
    }

    /**
     * Clears the current parameter values immediately. <P>In general, parameter values remain in force for repeated use
     * of a statement. Setting a parameter value automatically clears its previous value.  However, in some cases it is
     * useful to immediately release the resources used by the current parameter values; this can be done by calling the
     * method <code>clearParameters</code>.
     */
    public void clearParameters() throws SQLException {
        if (parametersCount > 0) {
            MariaDbClientPreparedStatement ps = (MariaDbClientPreparedStatement) inputParameters();
            if (!ps.parametersCleared) {
                ps.clearParameters();
                for (int i = 1; i <= parametersCount; i++) {
                    ps.setNull(i, Types.NULL);
                }
                inputParameters().execute();
            }
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        return callStatement.getWarnings();
    }

    /**
     * Executes the SQL statement in this <code>PreparedStatement</code> object,
     * which may be any kind of SQL statement.
     * Some prepared statements return multiple results; the <code>execute</code>
     * method handles these complex statements as well as the simpler
     * form of statements handled by the methods <code>executeQuery</code>
     * and <code>executeUpdate</code>.
     * <P>
     * The <code>execute</code> method returns a <code>boolean</code> to
     * indicate the form of the first result.  You must call either the method
     * <code>getResultSet</code> or <code>getUpdateCount</code>
     * to retrieve the result; you must call <code>getInternalMoreResults</code> to
     * move to any subsequent result(s).
     *
     * @return <code>true</code> if the first result is a <code>ResultSet</code>
     *         object; <code>false</code> if the first result is an update
     *         count or there is no result
     * @exception SQLException if a database access error occurs;
     * this method is called on a closed <code>PreparedStatement</code>
     * or an argument is supplied to this method
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the {@code setQueryTimeout}
     * method has been exceeded and has at least attempted to cancel
     * the currently running {@code Statement}
     * @see Statement#execute
     * @see Statement#getResultSet
     * @see Statement#getUpdateCount
     * @see Statement#getMoreResults

     */
    public boolean execute() throws SQLException {
        con.lock.lock();
        try {
            if (rsOutputParameters != null) {
                rsOutputParameters.close();
                rsOutputParameters = null;
            }
            if (parametersCount > 0) {
                preparedStatement.execute();
            }
            boolean ret = callStatement.execute(callQuery);

            // Read off output parameters, if there are any
            // (but not if query is streaming)
            if (hasOutputParameters() && callStatement.getFetchSize() != Integer.MIN_VALUE) {
                readOutputParameters();
            }
            return ret;
        } finally {
            con.lock.unlock();
        }
    }

    public boolean execute(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement does not support execute(String sql)");
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

    /**
     * Adds a set of parameters to this <code>PreparedStatement</code>
     * object's batch of send.
     *
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>PreparedStatement</code>
     * @see Statement#addBatch
     */
    public void addBatch() throws SQLException {
        if (preparedStatement == null) {
            preparedStatement = con.prepareStatement(callQuery);
            preparedStatement.addBatch();
        } else if (parametersCount == 0) {
            preparedStatement.addBatch();
        } else {
            /* add statement to set input parameters, mark it as ignorable */
            preparedStatement.addBatch();
            batchIgnoreResult.set(batchCount);
            batchCount++;

            preparedStatement.addBatch(callQuery);
            batchCount++;
        }
    }

    /**
     * Adds the given SQL command to the current list of send for this
     * <code>Statement</code> object. The send in this list can be
     * executed as a batch by calling the method <code>executeBatch</code>.
     * <P>
     *<strong>Note:</strong>This method cannot be called on a
     * <code>PreparedStatement</code> or <code>CallableStatement</code>.
     * @param sql typically this is a SQL <code>INSERT</code> or
     * <code>UPDATE</code> statement
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed <code>Statement</code>, the
     * driver does not support batch updates, the method is called on a
     * <code>PreparedStatement</code> or <code>CallableStatement</code>
     * @see #executeBatch
     * @see DatabaseMetaData#supportsBatchUpdates
     */
    public void addBatch(String sql) throws SQLException {
        if (parametersCount == 0) {
            if (preparedStatement == null) {
                preparedStatement = con.prepareStatement(sql);
                preparedStatement.addBatch();
            }
        } else {
            preparedStatement.addBatch(sql);
            batchCount++;
        }
    }

    public void setRef(int parameterIndex, Ref ref) throws SQLException {
        inputParameters().setRef(parameterIndex, ref);
    }

    public void setArray(int parameterIndex, Array array) throws SQLException {
        inputParameters().setArray(parameterIndex, array);
    }


    public ParameterMetaData getParameterMetaData() throws SQLException {
        parameterMetadata.readMetadataFromDbIfRequired();
        return parameterMetadata;
    }

    /**
     * Retrieves the  number, types and properties of
     * this <code>ResultSet</code> object's columns.
     *
     * @return the description of this <code>ResultSet</code> object's columns
     * @exception SQLException if a database access error occurs or this method is
     *            called on a closed result set
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSet rs = callStatement.getResultSet();
        if (rs != null) {
            return rs.getMetaData();
        }
        return null;
    }


    /**
     * Releases this <code>Statement</code> object's database
     * and JDBC resources immediately instead of waiting for
     * this to happen when it is automatically closed.
     * It is generally good practice to release resources as soon as
     * you are finished with them to avoid tying up database
     * resources.
     * <P>
     * Calling the method <code>close</code> on a <code>Statement</code>
     * object that is already closed has no effect.
     * <P>
     * <B>Note:</B>When a <code>Statement</code> object is
     * closed, its current <code>ResultSet</code> object, if one exists, is
     * also closed.
     *
     * @exception SQLException if a database access error occurs
     */
    public void close() throws SQLException {

        if (preparedStatement != null) {
            preparedStatement.close();
            preparedStatement = null;
        }
        if (rsOutputParameters != null) {
            rsOutputParameters.close();
            rsOutputParameters = null;
        }

        if (callStatement != null) {
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

    public void clearWarnings() throws SQLException {
        callStatement.clearWarnings();
    }

    public void setCursorName(String name) throws SQLException {
        callStatement.setCursorName(name);
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

    public boolean getMoreResults(int current) throws SQLException {
        return callStatement.getMoreResults(current);
    }

    public int getFetchDirection() throws SQLException {
        return callStatement.getFetchDirection();
    }

    public void setFetchDirection(int direction) throws SQLException {
        callStatement.setFetchDirection(direction);
    }

    public int getFetchSize() throws SQLException {
        return callStatement.getFetchSize();
    }

    public void setFetchSize(int rows) throws SQLException {
        callStatement.setFetchSize(rows);
    }

    public int getResultSetConcurrency() throws SQLException {
        return callStatement.getResultSetConcurrency();
    }

    public int getResultSetType() throws SQLException {
        return callStatement.getResultSetType();
    }

    /**
     * Empties this <code>Statement</code> object's current list of
     * SQL send.<p>
     * 
     * @exception SQLException if a database access error occurs,
     *  this method is called on a closed <code>Statement</code> or the
     * driver does not support batch updates
     * @see #addBatch
     * @see DatabaseMetaData#supportsBatchUpdates
     */
    public void clearBatch() throws SQLException {
        if (preparedStatement != null) {
            preparedStatement.clearBatch();
            if (parametersCount == 0) {
                preparedStatement.close();
                preparedStatement = null;
            }
        }
        batchCount = 0;
        batchIgnoreResult.clear();
    }

    /**
     * Submits a batch of send to the database for execution and
     * if all send execute successfully, returns an array of update counts.
     * The <code>int</code> elements of the array that is returned are ordered
     * to correspond to the send in the batch, which are ordered
     * according to the order in which they were added to the batch.
     * The elements in the array returned by the method <code>executeBatch</code>
     * may be one of the following:
     * <OL>
     * <LI>A number greater than or equal to zero -- indicates that the
     * command was processed successfully and is an update count giving the
     * number of rows in the database that were affected by the command's
     * execution
     * <LI>A value of <code>SUCCESS_NO_INFO</code> -- indicates that the command was
     * processed successfully but that the number of rows affected is
     * unknown
     * <P>
     * If one of the send in a batch update fails to execute properly,
     * this method throws a <code>BatchUpdateException</code>, and a JDBC
     * driver may or may not continue to process the remaining send in
     * the batch.  However, the driver's behavior must be consistent with a
     * particular DBMS, either always continuing to process send or never
     * continuing to process send.  If the driver continues processing
     * after a failure, the array returned by the method
     * <code>BatchUpdateException.getUpdateCounts</code>
     * will contain as many elements as there are send in the batch, and
     * at least one of the elements will be the following:
     *
     * <LI>A value of <code>EXECUTE_FAILED</code> -- indicates that the command failed
     * to execute successfully and occurs only if a driver continues to
     * process send after a command fails
     * </OL>
     * <P>
     * The possible implementations and return values have been modified in
     * the Java 2 SDK, Standard Edition, version 1.3 to
     * accommodate the option of continuing to process send in a batch
     * update after a <code>BatchUpdateException</code> object has been thrown.
     *
     * @return an array of update counts containing one element for each
     * command in the batch.  The elements of the array are ordered according
     * to the order in which send were added to the batch.
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed <code>Statement</code> or the
     * driver does not support batch statements. Throws {@link BatchUpdateException}
     * (a subclass of <code>SQLException</code>) if one of the send sent to the
     * database fails to execute properly or attempts to return a result set.
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the {@code setQueryTimeout}
     * method has been exceeded and has at least attempted to cancel
     * the currently running {@code Statement}
     *
     * @see #addBatch
     * @see DatabaseMetaData#supportsBatchUpdates
     * @since 1.2
     */
    public int[] executeBatch() throws SQLException {
        if (preparedStatement != null) {
            int[] unfilteredResult = preparedStatement.executeBatch();
            if (batchIgnoreResult.cardinality() == 0) {
                return unfilteredResult;
            }
            int[] filteredResult = new int[unfilteredResult.length - batchIgnoreResult.cardinality()];
            int index = 0;
            for (int i = 0; i < unfilteredResult.length; i++) {
                if (!batchIgnoreResult.get(i)) {
                    filteredResult[index++] = unfilteredResult[i];
                }
            }
            return filteredResult;
        } else {
            return new int[0];
        }
    }

    public Connection getConnection() throws SQLException {
        return con;
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return callStatement.getGeneratedKeys();
    }

    public int getResultSetHoldability() throws SQLException {
        return callStatement.getResultSetHoldability();
    }

    public boolean isClosed() throws SQLException {
        return callStatement.isClosed();
    }

    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

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

}
