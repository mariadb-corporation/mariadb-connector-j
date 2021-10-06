// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.socket;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;

@FunctionalInterface
public interface SocketHandlerFunction {

  Socket apply(Configuration conf, HostAddress hostAddress) throws IOException, SQLException;
}
