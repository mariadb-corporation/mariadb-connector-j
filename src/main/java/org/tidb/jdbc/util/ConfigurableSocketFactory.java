// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.util;

import javax.net.SocketFactory;
import org.tidb.jdbc.Configuration;

public abstract class ConfigurableSocketFactory extends SocketFactory {
  public abstract void setConfiguration(Configuration conf, String host);
}
