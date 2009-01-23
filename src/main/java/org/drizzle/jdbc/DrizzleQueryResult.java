package org.drizzle.jdbc;

import org.drizzle.jdbc.packet.FieldPacket;

import java.util.List;
import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 23, 2009
 * Time: 8:15:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrizzleQueryResult {
    public static final DrizzleQueryResult OKRESULT = new DrizzleQueryResult();
    private final List<FieldPacket> fieldPackets;
    private List<List<String>> resultSet = new ArrayList<List<String>>();
    public DrizzleQueryResult(List<FieldPacket> fieldPackets) {
        this.fieldPackets=fieldPackets;
    }
    
    private DrizzleQueryResult() {
        fieldPackets = null;
    }

    public void addRow(List<String> row) {
        resultSet.add(row);
    }

    public List<FieldPacket> getFieldPackets() {
        return fieldPackets;
    }
}
