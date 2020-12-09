package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

public interface RedoableClientMessage extends ClientMessage {

  default int encodePacket(PacketWriter writer, Context context) throws IOException, SQLException {
    encode(writer, context);
    context.saveRedo(this);
    return 1;
  }

  default int encode(PacketWriter writer, Context context, PrepareResultPacket newPrepareResult)
      throws IOException, SQLException {
    encode(writer, context);
    return 1;
  }

  default void reExecute(PacketWriter writer, Context context, PrepareResultPacket prepareResult)
      throws IOException, SQLException {
    encode(writer, context, prepareResult);
  }

  default int reExecutePacket(
      PacketWriter writer, Context context, PrepareResultPacket newPrepareResult)
      throws IOException, SQLException {
    reExecute(writer, context, newPrepareResult);
    return 1;
  }
}
