package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

public interface RedoableClientMessage extends ClientMessage {

  default void saveParameters() {}
  ;

  default void ensureReplayable(Context context) throws IOException, SQLException {}

  default int encode(PacketWriter writer, Context context, PrepareResultPacket newPrepareResult)
      throws IOException, SQLException {
    return encode(writer, context);
  }

  default int reEncode(PacketWriter writer, Context context, PrepareResultPacket newPrepareResult)
      throws IOException, SQLException {
    return encode(writer, context, newPrepareResult);
  }
}
