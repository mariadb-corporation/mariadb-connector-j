package org.skysql.jdbc.internal.common.queryresults;

import org.skysql.jdbc.internal.common.*;
import org.skysql.jdbc.internal.common.packet.*;
import org.skysql.jdbc.internal.common.packet.buffer.ReadUtil;
import org.skysql.jdbc.internal.drizzle.packet.DrizzleRowPacket;
import org.skysql.jdbc.internal.mysql.MySQLProtocol;
import org.skysql.jdbc.internal.mysql.packet.MySQLFieldPacket;
import org.skysql.jdbc.internal.mysql.packet.MySQLRowPacket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StreamingSelectResult extends SelectQueryResult {
    PacketFetcher packetFetcher;
    public List<ValueObject> values;
    MySQLProtocol protocol;
    boolean isEOF;
    boolean beforeFirst;


    private StreamingSelectResult(List<ColumnInformation> info, MySQLProtocol protocol, PacketFetcher fetcher) throws QueryException {
        this.columnInformation = info;
        this.protocol = protocol;
        this.packetFetcher = fetcher;
        this.beforeFirst = true;
        this.isEOF = false;
        if (protocol.activeResult != null) {
            throw new  QueryException("Result Set was not closed");
        }
        protocol.activeResult = this;
    }
     /**
     * create StreamingResultSet - precondition is that a result set packet has been read
     *
     * @param packet the result set packet from the server
     * @return a StreamingQueryResult
     * @throws java.io.IOException when something goes wrong while reading/writing from the server
     */
    public static StreamingSelectResult createStreamingSelectResult(
            ResultSetPacket packet, PacketFetcher packetFetcher, MySQLProtocol protocol)
            throws IOException, QueryException {

        final List<ColumnInformation> ci = new ArrayList<ColumnInformation>();
        for (int i = 0; i < packet.getFieldCount(); i++) {
            final RawPacket rawPacket = packetFetcher.getRawPacket();
            final ColumnInformation columnInfo = MySQLFieldPacket.columnInformationFactory(rawPacket);
            ci.add(columnInfo);
        }
        RawPacket fieldEOF = packetFetcher.getRawPacket();
        if (!ReadUtil.eofIsNext(fieldEOF)) {
            throw new IOException("unexpected packet");
        }
        return new StreamingSelectResult(ci, protocol, packetFetcher);

    }

    @Override
    public boolean next() throws IOException,QueryException{
       if (isEOF)
            return false;

            RawPacket rawPacket = packetFetcher.getRawPacket();

            if (ReadUtil.isErrorPacket(rawPacket)) {
                protocol.activeResult = null;
                protocol.hasMoreResults = false;
                ErrorPacket errorPacket = (ErrorPacket) ResultPacketFactory.createResultPacket(rawPacket);
                protocol.checkIfCancelled();
                throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
            }

            if (ReadUtil.eofIsNext(rawPacket)) {
                final EOFPacket eofPacket = (EOFPacket) ResultPacketFactory.createResultPacket(rawPacket);
                protocol.activeResult = null;
                protocol.hasMoreResults = eofPacket.getStatusFlags().contains(EOFPacket.ServerStatus.SERVER_MORE_RESULTS_EXISTS);
                warningCount = eofPacket.getWarningCount();
                //protocol.checkIfCancelled();
                isEOF = true;
                values = null;
                return false;
            }

            if (protocol.getDatabaseType() == SupportedDatabases.MYSQL) {
                final MySQLRowPacket rowPacket = new MySQLRowPacket(rawPacket, columnInformation);
                values = rowPacket.getRow(packetFetcher);
            } else {
                final DrizzleRowPacket rowPacket = new DrizzleRowPacket(rawPacket, columnInformation);
                values = rowPacket.getRow();
            }
           return true;

    }

    public void close() {
        if (protocol != null && protocol.activeResult == this)
        {
            for (;;) {
                try {
                    if(protocol.activeResult == null) {
                        return;
                    }
                    if (!next())
                        return;
                }
                catch (QueryException qe) {
                    return;
                }
                catch (IOException ioe) {
                    return;
                }
                finally   {
                   protocol.activeResult = null;
                   protocol = null;
                   packetFetcher = null;
                }
            }
        }
    }

    /**
     * gets the value at position i in the result set. i starts at zero!
     *
     * @param i index, starts at 0
     * @return
     */
    @Override
    public ValueObject getValueObject(int i) throws NoSuchColumnException {
        return values.get(i);
    }

    public int getRows() {
        return -1;
    }

    public boolean isBeforeFirst() {
        return beforeFirst;
    }
    public boolean  isAfterLast()  {
        return isEOF;
    }
}
