// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.util.constants;

public final class ConnectionState {

  public static final int STATE_NETWORK_TIMEOUT = 1;
  public static final int STATE_DATABASE = 2;
  public static final int STATE_READ_ONLY = 4;
  public static final int STATE_AUTOCOMMIT = 8;
  public static final int STATE_TRANSACTION_ISOLATION = 16;
}
