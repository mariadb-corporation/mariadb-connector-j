// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.codec;

import java.util.*;
import org.mariadb.jdbc.Driver;

public class CodecLoader {

  /**
   * Get authentication plugin from type String. Customs authentication plugin can be added
   * implementing AuthenticationPlugin and registering new type in resources services.
   *
   * @return CodecList plugin corresponding to type
   */
  public static CodecList get() {

    ServiceLoader<CodecList> loader =
        ServiceLoader.load(CodecList.class, Driver.class.getClassLoader());
    return loader.iterator().next();
  }
}
