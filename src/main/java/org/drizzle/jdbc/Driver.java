package org.drizzle.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.drizzle.jdbc.internal.drizzle.DrizzleProtocol;
import org.drizzle.jdbc.internal.drizzle.QueryException;
import org.drizzle.jdbc.internal.common.Protocol;
import org.drizzle.jdbc.internal.common.query.drizzle.DrizzleQueryFactory;
import org.drizzle.jdbc.internal.mysql.MySQLProtocol;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverPropertyInfo;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:46:09 AM
 */
public class Driver implements java.sql.Driver {
    private String hostname;
    private int port;
    private String username;
    private String password;
    private String database;
    private static final Logger log = LoggerFactory.getLogger(Driver.class);
    private String databaseType;

    static {
        try {
            System.out.println("yup");
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            throw new RuntimeException("Could not register driver",e);
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        // TODO: handle the properties!
        // TODO: define what props we support!
        log.debug("Connecting to: {} ",url);

        try {
            Protocol protocol = this.parseUrl(url);
            return new DrizzleConnection(protocol, new DrizzleQueryFactory());
        } catch (QueryException e) {
            throw new SQLException("Could not connect",e);
        }
    }

    /**
     * TODO: BUGGY
     * Syntax for connection url is:
     * jdbc:drizzle://username:password@host:port/database
     * @param url the url to parse
     * @throws SQLException if the connection string is bad
     */
    private Protocol parseUrl(String url) throws SQLException, QueryException {
        System.out.println("parsing url: "+url);
        Pattern p = Pattern.compile("^jdbc:(drizzle|mysqldriz)://((\\w+)(:(\\w+))?@)?([^/:]+)(:(\\d+))?(/(\\w+))?");
        Matcher m=p.matcher(url);        
        if(m.find()) {
            this.databaseType = m.group(1);
            this.username = m.group(3);
            log.debug("found username: {}",username);
            this.password = m.group(5);
            log.debug("found password: {}",password);
            this.hostname = m.group(6);
            log.debug("Found hostname: {}",hostname);

            if(m.group(8) != null) {
                this.port = Integer.parseInt(m.group(8));
                log.debug("Found port: {}",port);
            } else {

                if(this.databaseType.equals("drizzle"))
                    this.port=4427;
                else
                    this.port=3306;
            }
            this.database = m.group(10);
            log.debug("Found database: {}",database);
            if(this.databaseType.equals("drizzle")) {
                return new DrizzleProtocol(this.hostname,this.port,this.database,this.username,this.password);
            } else {
                System.out.println("yup");
                return new MySQLProtocol(this.hostname,this.port,this.database,this.username,this.password);
            }
        } else {
            log.debug("Could not parse connection string");
            throw new SQLException("Could not parse connection string...");
        }
    }

    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith("jdbc:drizzle://") || url.startsWith("jdbc:mysqldriz://");
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    public int getMajorVersion() {
        return 0;
    }

    public int getMinorVersion() {
        return 1;
    }

    public boolean jdbcCompliant() {
        return false;
    }
}
