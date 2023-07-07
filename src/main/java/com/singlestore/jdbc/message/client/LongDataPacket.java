// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.Parameter;
import com.singlestore.jdbc.message.ClientMessage;
import java.io.IOException;
import java.sql.SQLException;

/**
 * COM_STMT_SEND_LONG_DATA
 *
 * <p>Permit to send ONE value in a dedicated packet. The advantage is when length is unknown, to
 * stream easily data to socket
 */
public final class LongDataPacket implements ClientMessage {

  private final int statementId;
  private final Parameter parameter;
  private final int index;

  public LongDataPacket(int statementId, Parameter parameter, int index) {
    this.statementId = statementId;
    this.parameter = parameter;
    this.index = index;
  }

  @Override
  public int encode(Writer writer, Context context) throws IOException, SQLException {
    writer.initPacket();
    writer.writeByte(0x18);
    writer.writeInt(statementId);
    writer.writeShort((short) index);
    parameter.encodeLongData(writer);
    writer.flush();
    return 0;
  }
}
