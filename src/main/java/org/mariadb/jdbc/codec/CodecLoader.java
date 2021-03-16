/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

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
