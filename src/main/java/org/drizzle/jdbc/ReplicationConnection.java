package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.drizzle.packet.RawPacket;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Apr 18, 2009
 * Time: 9:57:17 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ReplicationConnection {
    public List<RawPacket> startBinlogDump(int startPos, String logfile) throws SQLException;
}
