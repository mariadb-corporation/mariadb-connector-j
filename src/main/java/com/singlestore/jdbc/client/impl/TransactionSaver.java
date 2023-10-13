// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.impl;

import com.singlestore.jdbc.message.client.RedoableClientMessage;
import java.util.Arrays;

/**
 * Transaction cache Huge command are not cached, cache is limited to configuration
 * transactionReplaySize commands
 */
public class TransactionSaver {

  private final RedoableClientMessage[] buffers;
  private int idx = 0;
  private boolean dirty = false;

  /**
   * Constructor
   *
   * @param transactionReplaySize maximum number of command cached
   */
  public TransactionSaver(int transactionReplaySize) {
    buffers = new RedoableClientMessage[transactionReplaySize];
  }

  /**
   * Add a command to cache.
   *
   * @param clientMessage client message
   */
  public void add(RedoableClientMessage clientMessage) {
    if (idx < buffers.length) {
      buffers[idx++] = clientMessage;
    } else {
      dirty = true;
    }
  }

  /** Transaction finished, clearing cache */
  public void clear() {
    Arrays.fill(buffers, null);
    dirty = false;
    idx = 0;
  }

  /**
   * Current transaction cache length
   *
   * @return cache length
   */
  public int getIdx() {
    return idx;
  }

  /**
   * Is cache not valid (some commands have not been cached)
   *
   * @return is dirty
   */
  public boolean isDirty() {
    return dirty;
  }

  /**
   * cache buffer
   *
   * @return cached messages
   */
  public RedoableClientMessage[] getBuffers() {
    return buffers;
  }
}
