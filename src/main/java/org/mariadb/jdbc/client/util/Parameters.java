// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.util;

public interface Parameters {

  Parameter get(int index);

  boolean containsKey(int index);

  void set(int index, Parameter element);

  int size();

  Parameters clone();
}
