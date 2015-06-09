package org.mariadb.jdbc.multihost;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.mariadb.jdbc.BaseTest;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.mysql.FailoverProxy;
import org.mariadb.jdbc.internal.mysql.MultiHostListener;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.logging.*;

/**
 *  Base util class.
 *  For testing
 *  mvn test  -Pmultihost -DdbUrl=jdbc:mysql://localhost:3306,localhost:3307/test?user=root&favor=master-slave -DlogLevel=FINEST
 */
@Ignore
public class BaseMultiHostTest {
    protected static Logger log = Logger.getLogger("org.maria.jdbc");
    protected static String url;
    protected static final String defaultMultiHostUrl = "jdbc:mysql://host1,host2,host3:3306/test?user=root";
    protected static JDBCUrl jdbcUrl;
    //hosts
    protected static TcpProxy[] tcpProxies;

    @BeforeClass
    public static void beforeClass()  throws SQLException, IOException {
        //get the multi-host connection string
        String tmpUrl = System.getProperty("defaultMultiHostUrl", defaultMultiHostUrl);
        String logLevel = System.getProperty("logLevel");
        if (logLevel != null) {
            if (log.getHandlers().length == 0) {
                ConsoleHandler consoleHandler = new ConsoleHandler();
                consoleHandler.setFormatter(new CustomFormatter());
                consoleHandler.setLevel(Level.parse(logLevel));
                log.addHandler(consoleHandler);
                log.setLevel(Level.FINE);

                Logger.getLogger(MultiHostListener.class.getName()).setLevel(Level.ALL);
                Logger.getLogger(MultiHostListener.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(FailoverProxy.class.getName()).setLevel(Level.ALL);
                Logger.getLogger(FailoverProxy.class.getName()).addHandler(consoleHandler);
            }
        }
        JDBCUrl tmpJdbcUrl = JDBCUrl.parse(tmpUrl);
        tcpProxies = new TcpProxy[tmpJdbcUrl.getHostAddresses().length];
        int beginSocketPort = 52360;

        String sockethosts = "";
        for (int i=0;i<tmpJdbcUrl.getHostAddresses().length;i++) {
            log.info("creating socket "+tmpJdbcUrl.getHostAddresses()[i].host+":"+tmpJdbcUrl.getHostAddresses()[i].port+" -> localhost:"+beginSocketPort);
            tcpProxies[i] = new TcpProxy(tmpJdbcUrl.getHostAddresses()[i].host,
                    tmpJdbcUrl.getHostAddresses()[i].port,
                    beginSocketPort);
            sockethosts+=",localhost:"+beginSocketPort;
            beginSocketPort++;
        }

        url="jdbc:mysql://"+sockethosts.substring(1)+"/"+tmpUrl.split("/")[3];
        //parse the url
        jdbcUrl = JDBCUrl.parse(url);
    }

    protected Connection getNewConnection() throws SQLException {
        return getNewConnection(null);
    }

    protected Connection getNewConnection(String additionnalConnectionData) throws SQLException {
        if (additionnalConnectionData == null) {
            return DriverManager.getConnection(url);
        } else {
            return DriverManager.getConnection(url+additionnalConnectionData);
        }
    }

    @AfterClass
    public static void afterClass()  throws SQLException {
        for (TcpProxy tcpProxy : tcpProxies) {
            try {
                tcpProxy.stop();
            } catch (Exception e) {}
        }
    }

}
class CustomFormatter  extends Formatter {
    private static final String format = "[%1$tT] %4$s: %2$s - %5$s %6$s%n";
    private final java.util.Date dat = new java.util.Date();
    public synchronized String format(LogRecord record) {
        dat.setTime(record.getMillis());
        String source;
        if (record.getSourceClassName() != null) {
            source = record.getSourceClassName();
            if (record.getSourceMethodName() != null) {
                source += " " + record.getSourceMethodName();
            }
        } else {
            source = record.getLoggerName();
        }
        String message = formatMessage(record);
        String throwable = "";
        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println();
            record.getThrown().printStackTrace(pw);
            pw.close();
            throwable = sw.toString();
        }
        return String.format(format,
                dat,
                source,
                record.getLoggerName(),
                record.getLevel().getName(),
                message,
                throwable);
    }
}
