// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.util.exceptions;

import java.io.IOException;

public class MaxAllowedPacketException extends IOException {

  private static final long serialVersionUID = 5669184960442818475L;
  private final boolean mustReconnect;

  public MaxAllowedPacketException(String message, boolean mustReconnect) {
    super(message);
    this.mustReconnect = mustReconnect;
  }

  public boolean isMustReconnect() {
    return mustReconnect;
  }
}
