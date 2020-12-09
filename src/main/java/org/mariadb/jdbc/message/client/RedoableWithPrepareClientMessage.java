package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

public interface RedoableWithPrepareClientMessage extends RedoableClientMessage {
  String getCommand();

  ServerPreparedStatement prep();

  int encode(PacketWriter writer, Context context, PrepareResultPacket newPrepareResult)
      throws IOException, SQLException;

  @Override
  default int encodePacket(PacketWriter writer, Context context) throws IOException, SQLException {
    context.saveRedo(this);
    return encode(writer, context, null);
  }

  @Override
  default void reExecute(PacketWriter writer, Context context, PrepareResultPacket prepareResult)
      throws IOException, SQLException {
    encode(writer, context, prepareResult);
  }

  @Override
  default int reExecutePacket(
      PacketWriter writer, Context context, PrepareResultPacket newPrepareResult)
      throws IOException, SQLException {
    return encode(writer, context, newPrepareResult);
  }

  void setPrepareResult(PrepareResultPacket prepareResult);

  default void rePrepare(Client client) throws SQLException {
    PreparePacket preparePacket = new PreparePacket(getCommand());
    setPrepareResult(
        (PrepareResultPacket)
            client
                .execute(
                    preparePacket,
                    prep(),
                    0,
                    0L,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.TYPE_FORWARD_ONLY,
                    false)
                .get(0));
  }
}
