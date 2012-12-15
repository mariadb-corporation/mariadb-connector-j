package org.mariadb.jdbc.internal.common.queryresults;

import org.mariadb.jdbc.internal.common.ColumnInformation;
import org.mariadb.jdbc.internal.common.PacketFetcher;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.ValueObject;
import org.mariadb.jdbc.internal.common.packet.*;
import org.mariadb.jdbc.internal.common.packet.buffer.ReadUtil;
import org.mariadb.jdbc.internal.mysql.MySQLProtocol;
import org.mariadb.jdbc.internal.mysql.packet.MySQLFieldPacket;
import org.mariadb.jdbc.internal.mysql.packet.MySQLRowPacket;

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

        if (protocol.activeResult != null) {
            throw new  QueryException("There is an active result set on the current connection, "+
                    "which must be closed prior to opening a new one");
        }

        final List<ColumnInformation> ci = new ArrayList<ColumnInformation>();
        for (int i = 0; i < packet.getFieldCount(); i++) {
            final RawPacket rawPacket = packetFetcher.getRawPacket();

            // We do not expect an error packet, but check it just for safety
            if (ReadUtil.isErrorPacket(rawPacket)) {
                ErrorPacket errorPacket = new ErrorPacket(rawPacket);
                throw new QueryException("error when reading field packet " + errorPacket.getMessage(),
                        errorPacket.getErrorNumber(), errorPacket.getSqlState());
            }
            // We do not expect OK or EOF packets either
            byte b = rawPacket.getByteBuffer().get(0);
            if (b == 0 || b == (byte)0xfe) {
                // We do not expect OK or EOF packets here
                throw new QueryException("Packets out of order when trying to read field packet - " +
                    "got packet starting with byte " + b + "packet content (hex) = "
                        + MySQLProtocol.hexdump(rawPacket.getByteBuffer(), 0));
            }
            try {
                ColumnInformation columnInfo = MySQLFieldPacket.columnInformationFactory(rawPacket);
                ci.add(columnInfo);
            } catch (Exception e) {
                throw new QueryException("Error when trying to parse field packet : " + e + ",packet content (hex) = " +
                        MySQLProtocol.hexdump(rawPacket.getByteBuffer(), 0) , 0, "HY000", e);
            }
        }
        RawPacket fieldEOF = packetFetcher.getRawPacket();
        if (!ReadUtil.eofIsNext(fieldEOF)) {
            throw new QueryException("Packets out of order when reading field packets, expected was EOF packet. " +
                    "Packet contents (hex) = " + MySQLProtocol.hexdump(fieldEOF.getByteBuffer(),0));
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
                protocol.moreResults = false;
                ErrorPacket errorPacket = (ErrorPacket) ResultPacketFactory.createResultPacket(rawPacket);
                throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
            }

            if (ReadUtil.eofIsNext(rawPacket)) {
                final EOFPacket eofPacket = (EOFPacket) ResultPacketFactory.createResultPacket(rawPacket);
                protocol.activeResult = null;
                protocol.moreResults = eofPacket.getStatusFlags().contains(EOFPacket.ServerStatus.SERVER_MORE_RESULTS_EXISTS);
                warningCount = eofPacket.getWarningCount();
                isEOF = true;
                values = null;
                return false;
            }


           MySQLRowPacket rowPacket = new MySQLRowPacket(rawPacket, columnInformation);
           values = rowPacket.getRow(packetFetcher);

           return true;

    }

    public void close() {
        super.close();
        if (protocol != null && protocol.activeResult == this)
        {
            try {
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
                }
            }   finally   {
                   protocol.activeResult = null;
                   protocol = null;
                   packetFetcher = null;
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
