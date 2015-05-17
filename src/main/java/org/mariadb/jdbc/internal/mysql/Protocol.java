package org.mariadb.jdbc.internal.mysql;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.Query;
import org.mariadb.jdbc.internal.common.queryresults.QueryResult;
import org.mariadb.jdbc.internal.common.queryresults.StreamingSelectResult;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by diego_000 on 12/05/2015.
 */
public interface Protocol {
    MySQLProtocol.PrepareResult prepare(String sql) throws QueryException;

    void closePreparedStatement(int statementId) throws QueryException;

    boolean getAutocommit();

    boolean noBackslashEscapes();

    void connect() throws QueryException, SQLException;
    void initializeConnection() throws QueryException, SQLException;
    JDBCUrl getJdbcUrl();
    boolean inTransaction();

    Properties getInfo();

    boolean  hasMoreResults();

    void close();

    boolean isClosed();

    void selectDB(String database) throws QueryException;

    String getServerVersion();

    void setReadonly(boolean readOnly);

    boolean getReadonly();
    boolean isMasterConnection();
    boolean isHighAvailability();
    HostAddress getHostAddress();
    String getHost();

    int getPort();

    String getDatabase();

    String getUsername();

    String getPassword();

    boolean ping() throws QueryException;

    QueryResult executeQuery(Query dQuery)  throws QueryException, SQLException;

    QueryResult getResult(Query dQuery, boolean streaming) throws QueryException;

    QueryResult executeQuery(Query dQuery, boolean streaming) throws QueryException, SQLException;

    String getServerVariable(String variable) throws QueryException, SQLException;

    void cancelCurrentQuery() throws QueryException, IOException, SQLException;

    boolean createDB();

    QueryResult getMoreResults(boolean streaming) throws QueryException;

    boolean hasUnreadData();
    boolean checkIfMaster() throws SQLException ;
    boolean hasWarnings();
    int getDatatypeMappingFlags();
    StreamingSelectResult getActiveResult();

    void setMaxRows(int max) throws QueryException, SQLException;
    int getMaxRows();

    int getMajorServerVersion();

    int getMinorServerVersion();

    boolean versionGreaterOrEqual(int major, int minor, int patch);

    void setLocalInfileInputStream(InputStream inputStream);

    int getMaxAllowedPacket();

    void setMaxAllowedPacket(int maxAllowedPacket);

    void setTimeout(int timeout) throws SocketException;

    int getTimeout() throws SocketException;

    String getPinGlobalTxToPhysicalConnection();
}
