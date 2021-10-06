// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.util.options;

import java.util.HashMap;
import java.util.Map;

public final class OptionAliases {

  public static final Map<String, String> OPTIONS_ALIASES;

  static {
    OPTIONS_ALIASES = new HashMap<>();
    OPTIONS_ALIASES.put("enabledSSLCipherSuites", "enabledSslCipherSuites");
    OPTIONS_ALIASES.put("serverRSAPublicKeyFile", "serverRsaPublicKeyFile");
  }
}
