// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.util.options;

import java.util.HashMap;
import java.util.Map;

/** Option alias name */
public final class OptionAliases {

  /** list of aliases */
  public static final Map<String, String> OPTIONS_ALIASES;

  static {
    OPTIONS_ALIASES = new HashMap<>();
    OPTIONS_ALIASES.put("clientcertificatekeystoreurl", "keyStore");
    OPTIONS_ALIASES.put("clientcertificatekeystorepassword", "keyStorePassword");
    OPTIONS_ALIASES.put("clientcertificatekeystoretype", "keyStoreType");
  }
}
