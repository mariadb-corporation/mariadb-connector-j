// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.impl;

import com.singlestore.jdbc.message.client.RedoableClientMessage;
import java.util.ArrayList;
import java.util.List;

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
