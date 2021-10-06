// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.util.constants;

@SuppressWarnings("unused")
public final class StateChange {

  public static final short SESSION_TRACK_SYSTEM_VARIABLES = 0;
  public static final short SESSION_TRACK_SCHEMA = 1;
  public static final short SESSION_TRACK_STATE_CHANGE = 2;
  public static final short SESSION_TRACK_GTIDS = 3;
  public static final short SESSION_TRACK_TRANSACTION_CHARACTERISTICS = 4;
  public static final short SESSION_TRACK_TRANSACTION_STATE = 5;
}
