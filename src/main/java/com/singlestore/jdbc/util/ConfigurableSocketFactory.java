// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.util;

import com.singlestore.jdbc.Configuration;
import javax.net.SocketFactory;

public abstract class ConfigurableSocketFactory extends SocketFactory {
  public abstract void setConfiguration(Configuration conf, String host);
}
