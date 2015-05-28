package org.mariadb.jdbc;


import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;

/**
 *  Base util class.
 *  For testing
 *  mvn test -DdbUrl=jdbc:mysql://localhost:3306/test?user=root -DlogLevel=FINEST
 */
@Ignore
public class BaseTest {

    protected static Logger log = Logger.getLogger("org.maria.jdbc");
    protected Connection connection;
    protected static String connU;
    protected static String connURI;
    protected static String hostname;
    protected static int port;
    protected static String database;
    protected static String username;
    protected static String password;
    protected static String parameters;
    protected static final String mDefUrl = "jdbc:mysql://localhost:3306/test?user=root";
    
    @BeforeClass
    public static void beforeClassBaseTest() {
    	String url = System.getProperty("dbUrl", mDefUrl);
    	JDBCUrl jdbcUrl = JDBCUrl.parse(url);

        String logLevel = System.getProperty("logLevel");
        if (logLevel != null) {
            if (log.getHandlers().length == 0) {
                ConsoleHandler consoleHandler = new ConsoleHandler();
                consoleHandler.setFormatter(new CustomFormatter());
                consoleHandler.setLevel(Level.parse(logLevel));
                log.addHandler(consoleHandler);
                log.setLevel(Level.ALL);
            }
        }

    	hostname = jdbcUrl.getHostname();
    	port = jdbcUrl.getPort();
    	database = jdbcUrl.getDatabase();
    	username = jdbcUrl.getUsername();
    	password = jdbcUrl.getPassword();
    	
    	log.fine("Properties parsed from JDBC URL - hostname: " + hostname + ", port: " + port + ", database: " + database + ", username: " + username + ", password: " + password);
    	
    	if (database != null && "".equals(username)) {
    		String[] tokens = database.contains("?") ? database.split("\\?") : null;
    		if (tokens != null) {
    			database = tokens[0];
    			String[] paramTokens = tokens[1].split("&");
    			username = paramTokens[0].startsWith("user=") ? paramTokens[0].substring(5) : null;
    			if (paramTokens.length > 1) {
    				password = paramTokens[0].startsWith("password=") ? paramTokens[1].substring(9) : null;
    			}
    		}
    	}
    	setURI();
    }
    
    private static void setURI() {
    	connU = "jdbc:mysql://" + hostname + ":" + port + "/" + database;
    	connURI = connU + "?user=" + username
    			+ (password != null && !"".equals(password) ? "&password=" + password : "")
    			+ (parameters != null ? parameters : "");
    }
    
    @Before
    public void before() throws SQLException{
        setConnection();
    }
    @After
    public void after() throws SQLException {
        try {
        	connection.close();
        } catch(Exception e) {
        }
    }
    
    protected void setHostname(String hostname) throws SQLException {
    	BaseTest.hostname = hostname;
    	setURI();
    	setConnection();
    }
    protected void setPort(int port) throws SQLException {
    	BaseTest.port = port;
    	setURI();
    	setConnection();
    }
    protected void setDatabase(String database) throws SQLException {
    	BaseTest.database = database;
    	setURI();
    	setConnection();
    }
    protected void setUsername(String username) throws SQLException {
    	BaseTest.username = username;
    	setURI();
    	setConnection();
    }
    protected void setPassword(String password) throws SQLException {
    	BaseTest.password = password;
    	setURI();
    	setConnection();
    }
    protected void setParameters(String parameters) throws SQLException {
    	BaseTest.parameters = parameters;
    	setURI();
    	setConnection();
    }
    
    protected void setConnection() throws SQLException {
    	openConnection(connURI, null);
    }   
    protected void setConnection(Map<String, String> props) throws SQLException {
    	Properties info = new Properties();
    	for (String key : props.keySet()) {
    		info.setProperty(key, props.get(key));
    	}
    	openConnection(connU, info);
    }
    protected void setConnection(Properties info) throws SQLException {
    	openConnection(connURI, info);
    }
    protected void setConnection(String parameters) throws SQLException {
    	openConnection(connURI + parameters, null);
    }
    
    private void openConnection(String URI, Properties info) throws SQLException {
    	try {
    		connection.close();
    	} catch (Exception ex) {
    	}
    	if (info == null) {
    		connection = DriverManager.getConnection(URI);
    	} else {
    		connection = DriverManager.getConnection(URI, info);
    	}
    }
    
    protected Connection openNewConnection() throws SQLException {
    	Properties info = connection.getClientInfo();
    	return openNewConnection(connURI, info);
    }
    protected Connection openNewConnection(String url) throws SQLException {
    	return DriverManager.getConnection(url);
    }
    protected Connection openNewConnection(String url, Properties info) throws SQLException {
    	return DriverManager.getConnection(url, info);
    }

    boolean checkMaxAllowedPacket(String testName) throws SQLException {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("select @@max_allowed_packet");
        rs.next();
        int max_allowed_packet = rs.getInt(1);

        rs = st.executeQuery("select @@innodb_log_file_size");
        rs.next();
        int innodb_log_file_size = rs.getInt(1);

        if(max_allowed_packet < 16*1024*1024) {
            log.info("test '" + testName + "' skipped  due to server variable max_allowed_packet < 16M");
            return false;
        }
        if(innodb_log_file_size < 16*1024*1024) {
            log.info("test '" + testName + "' skipped  due to server variable innodb_log_file_size < 16M");
            return false;
        }
        return true;
    }

    boolean checkMaxAllowedPacketMore40m(String testName) throws SQLException {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("select @@max_allowed_packet");
        rs.next();
        int max_allowed_packet = rs.getInt(1);

        rs = st.executeQuery("select @@innodb_log_file_size");
        rs.next();
        int innodb_log_file_size = rs.getInt(1);


        if(max_allowed_packet < 40*1024*1024) {
            log.info("test '" + testName + "' skipped  due to server variable max_allowed_packet < 40M");
            return false;
        }
        if(innodb_log_file_size < 160*1024*1024) {
            log.info("test '" + testName + "' skipped  due to server variable innodb_log_file_size < 160M");
            return false;
        }

        return true;
    }


    //does the user have super privileges or not?
    boolean hasSuperPrivilege(String testName) throws SQLException
    {
        boolean superPrivilege = false;
        Statement st = connection.createStatement();

        // first test for specific user and host combination
        ResultSet rs = st.executeQuery("SELECT Super_Priv FROM mysql.user WHERE user = '" + username + "' AND host = '" + hostname + "'");
        if (rs.next()) {
            superPrivilege = (rs.getString(1).equals("Y") ? true : false);
        } else
            {
                // then check for user on whatever (%) host
                rs = st.executeQuery("SELECT Super_Priv FROM mysql.user WHERE user = '" + username + "' AND host = '%'");
                if (rs.next())
                    superPrivilege = (rs.getString(1).equals("Y") ? true : false);
            }

        rs.close();

        if (!superPrivilege)
            log.info("test '" + testName + "' skipped because user '" + username + "' doesn't have SUPER privileges");

        return superPrivilege;
    }
    
    //is the connection local?
    boolean isLocalConnection(String testName)
    {
    	boolean isLocal = false;
    	
    	try {
			if (InetAddress.getByName(hostname).isAnyLocalAddress() || InetAddress.getByName(hostname).isLoopbackAddress())
				isLocal = true;
		} catch (UnknownHostException e) {
			// for some reason it wasn't possible to parse the hostname
			// do nothing
		}
    	
    	if (isLocal == false)
            log.info("test '" + testName + "' skipped because connection is not local");
    	
    	return isLocal;
    }

    boolean haveSSL(){
            try {
                ResultSet rs = connection.createStatement().executeQuery("select @@have_ssl");
                rs.next();
                String value = rs.getString(1);
                return value.equals("YES");
            } catch (Exception e)  {
                return false; /* maybe 4.x ? */
            }
        }

    void requireMinimumVersion(int major, int minor) throws SQLException {
        DatabaseMetaData md = connection.getMetaData();
        int dbMajor = md.getDatabaseMajorVersion();
        int dbMinor = md.getDatabaseMinorVersion();
        org.junit.Assume.assumeTrue(dbMajor > major ||
                (dbMajor == major && dbMinor >= minor));

    }
    
    // common function for logging information 
    static void logInfo(String message)
    {
        log.info(message);
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