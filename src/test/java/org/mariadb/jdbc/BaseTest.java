package org.mariadb.jdbc;


import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.sql.*;
import java.util.Map;
import java.util.Properties;

@Ignore
public class BaseTest {
    protected Connection connection;
    protected static String connU;
    protected static String connURI;
    protected static String mHostname;
    protected static int mPort;
    protected static String mDatabase;
    protected static String mUsername;
    protected static String mPassword;
    protected static String mParameters;
    protected static final String mDefUrl = "jdbc:mysql://localhost:3306/test?user=root";
    
    @BeforeClass
    public static void beforeClassBaseTest() {
    	String url = System.getProperty("dbUrl", mDefUrl);
    	JDBCUrl jdbcUrl = JDBCUrl.parse(url);
    	mHostname = jdbcUrl.getHostname();
    	mPort = jdbcUrl.getPort();
    	mDatabase = jdbcUrl.getDatabase();
    	mUsername = jdbcUrl.getUsername();
    	mPassword = jdbcUrl.getPassword();
    	if (mDatabase != null && "".equals(mUsername)) {
    		String[] tokens = mDatabase.contains("?") ? mDatabase.split("\\?") : null;
    		if (tokens != null) {
    			mDatabase = tokens[0];
    			String[] paramTokens = tokens[1].split("&");
    			mUsername = paramTokens[0].startsWith("user=") ? paramTokens[0].substring(5) : null;
    			if (paramTokens.length > 1) {
    				mPassword = paramTokens[0].startsWith("password=") ? paramTokens[1].substring(9) : null;
    			}
    		}
    	}
    	setURI();
    }
    
    private static void setURI() {
    	connU = "jdbc:mysql://" + mHostname + ":" + mPort + "/" + mDatabase;
    	connURI = connU + "?user=" + mUsername
    			+ (mPassword != null && !"".equals(mPassword) ? "&password=" + mPassword : "")
    			+ (mParameters != null ? mParameters : "");
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
    	mHostname = hostname;
    	setURI();
    	setConnection();
    }
    protected void setPort(int port) throws SQLException {
    	mPort = port;
    	setURI();
    	setConnection();
    }
    protected void setDatabase(String database) throws SQLException {
    	mDatabase = database;
    	setURI();
    	setConnection();
    }
    protected void setUsername(String username) throws SQLException {
    	mUsername = username;
    	setURI();
    	setConnection();
    }
    protected void setPassword(String password) throws SQLException {
    	mPassword = password;
    	setURI();
    	setConnection();
    }
    protected void setParameters(String parameters) throws SQLException {
    	mParameters = parameters;
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
    	openConnection(connU, info);
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

    boolean checkMaxAllowedPacket(String testName) throws SQLException
    {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("select @@max_allowed_packet");
        rs.next();
        int max_allowed_packet = rs.getInt(1);
        if(max_allowed_packet < 0xffffff)
        {
          System.out.println("test '" + testName + "' skipped  due to server variable max_allowed_packet < 16M");
          return false;
        }
        return true;
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
}
