// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.socket.impl;

import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;

@FunctionalInterface
public interface SocketHandlerFunction {

  Socket apply(Configuration conf, HostAddress hostAddress) throws IOException, SQLException;
}
