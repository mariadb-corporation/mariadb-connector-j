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
import java.util.List;
import java.util.Properties;

/**
 * Created by diego_000 on 12/05/2015.
 */
public interface Protocol {
    MySQLProtocol.PrepareResult prepare(String sql) throws QueryException;

    void closePreparedStatement(int statementId) throws QueryException;

    boolean getAutocommit();

    boolean noBackslashEscapes();

    void connect() throws QueryException;
    JDBCUrl getJdbcUrl();
    boolean inTransaction();
    void setProxy(FailoverProxy proxy);
    FailoverProxy getProxy();
    void setHostAddress(HostAddress hostAddress);

    Properties getInfo();

    boolean  hasMoreResults();

    void close();

    boolean isClosed();

    void selectDB(String database) throws QueryException;

    String getServerVersion();

    void setReadonly(boolean readOnly);
    boolean isConnected();
    boolean getReadonly();
    boolean isMasterConnection();
    boolean mustBeMasterConnection();
    boolean isHighAvailability();
    HostAddress getHostAddress();
    String getHost();

    int getPort();

    String getDatabase();

    String getUsername();

    String getPassword();

    boolean ping() throws QueryException;

    QueryResult executeQuery(Query dQuery)  throws QueryException;
    QueryResult executeQuery(final List<Query> dQueries, boolean streaming, boolean isRewritable, int rewriteOffset) throws QueryException;
    QueryResult getResult(List<Query> dQuery, boolean streaming) throws QueryException;

    QueryResult executeQuery(Query dQuery, boolean streaming) throws QueryException;

    String getServerVariable(String variable) throws QueryException;

    void cancelCurrentQuery() throws QueryException, IOException;

    boolean createDB();

    QueryResult getMoreResults(boolean streaming) throws QueryException;

    boolean hasUnreadData();
    boolean checkIfMaster() throws QueryException ;
    boolean hasWarnings();
    int getDatatypeMappingFlags();
    StreamingSelectResult getActiveResult();

    void setMaxRows(int max) throws QueryException;
    void setInternalMaxRows(int max);
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
    long getServerThreadId();
    void setTransactionIsolation(int level) throws QueryException;
    int getTransactionIsolationLevel();
}
