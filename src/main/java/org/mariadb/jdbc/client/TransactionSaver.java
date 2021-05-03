// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client;

import java.util.ArrayList;
import java.util.List;
import org.mariadb.jdbc.message.client.RedoableClientMessage;

public class TransactionSaver {
  private final List<RedoableClientMessage> buffers = new ArrayList<>();

  public void add(RedoableClientMessage clientMessage) {
    buffers.add(clientMessage);
  }

  public void clear() {
    buffers.clear();
  }

  public List<RedoableClientMessage> getBuffers() {
    return buffers;
  }
}
