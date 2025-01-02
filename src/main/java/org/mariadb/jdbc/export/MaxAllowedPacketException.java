// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.export;

import java.io.IOException;

/**
 * Dedicate exception when error is due to sending packet with size &gt; to server
 * max_allowed_packet, that would cause server to drop connection
 */
public class MaxAllowedPacketException extends IOException {

  private static final long serialVersionUID = 5669184960442818475L;

  /** is connection in wrong state */
  private final boolean mustReconnect;

  /**
   * Constructor
   *
   * @param message error message
   * @param mustReconnect is connection state unsure
   */
  public MaxAllowedPacketException(String message, boolean mustReconnect) {
    super(message);
    this.mustReconnect = mustReconnect;
  }

  /**
   * Indicate that connection state is unsure
   *
   * @return must driver reconnect connection
   */
  public boolean isMustReconnect() {
    return mustReconnect;
  }
}
