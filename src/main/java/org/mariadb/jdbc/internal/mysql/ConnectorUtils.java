package org.mariadb.jdbc.internal.mysql;

import org.mariadb.jdbc.JDBCUrl;

import java.lang.reflect.Proxy;
import java.util.Properties;

/**
 * Created by diego_000 on 17/05/2015.
 * TODO: Add description/documentation
 */
public class ConnectorUtils {
    public static Protocol retrieveProxy(JDBCUrl jdbcUrl,
                                         final String username,
                                         final String password,
                                         Properties info) {
        Protocol proxyfiedProtocol;
        if (jdbcUrl.getHostAddresses().length == 1) {
            proxyfiedProtocol = (Protocol) Proxy.newProxyInstance(
                    MySQLProtocol.class.getClassLoader(),
                    new Class[]{Protocol.class},
                    new FailoverProxy(new MySQLProtocol(jdbcUrl, username, password, info), new SingleHostListener()));
        } else {
        	
        	// TODO: AURORA I would change this to be called flavor and it takes the value aurora or something else
        	//then it's not only restricted to aurora and can be used for other flavors of mysql as well
            String aurora = info.getProperty("aurora", "false");
            
            boolean auroraMultinode = "true".equals(aurora) || "1".equals(aurora);
            
            if (auroraMultinode) {
                proxyfiedProtocol = (Protocol) Proxy.newProxyInstance(
                        AuroraMultiNodesProtocol.class.getClassLoader(),
                        new Class[] {Protocol.class},
                        new FailoverProxy(new AuroraMultiNodesProtocol(jdbcUrl, username,  password,  info), new AuroraHostListener()));
            } else {
                proxyfiedProtocol = (Protocol) Proxy.newProxyInstance(
                        MultiNodesProtocol.class.getClassLoader(),
                        new Class[] {Protocol.class},
                        new FailoverProxy(new MultiNodesProtocol(jdbcUrl, username,  password,  info), new MultiHostListener()));

            }
        }
        return proxyfiedProtocol;
    }

}
