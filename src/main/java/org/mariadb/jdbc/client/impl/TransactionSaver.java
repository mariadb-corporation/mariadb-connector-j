// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.impl;

import java.util.Arrays;
import org.mariadb.jdbc.message.client.RedoableClientMessage;

public class TransactionSaver {
  private final RedoableClientMessage[] buffers;
  private int idx = 0;
  private boolean dirty = false;

  public TransactionSaver(int transactionReplaySize) {
    buffers = new RedoableClientMessage[transactionReplaySize];
  }

  public void add(RedoableClientMessage clientMessage) {
    if (idx < buffers.length) {
      buffers[idx++] = clientMessage;
    } else {
      dirty = true;
    }
  }

  public void clear() {
    Arrays.fill(buffers, null);
    dirty = false;
    idx = 0;
  }

  public int getIdx() {
    return idx;
  }

  public boolean isDirty() {
    return dirty;
  }

  public RedoableClientMessage[] getBuffers() {
    return buffers;
  }
}
