// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.context.Context;

public class EofPacket implements Completion {

  public EofPacket(ReadableByteBuf buf, Context context) {
    buf.skip(1); // eof header
    context.setWarning(buf.readUnsignedShort());
    context.setServerStatus(buf.readUnsignedShort());
  }
}
