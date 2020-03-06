package org.mariadb.jdbc.util;

import javax.net.SocketFactory;

public abstract class ConfigurableSocketFactory extends SocketFactory {
    public abstract void setConfiguration(Options options, String host);
}
