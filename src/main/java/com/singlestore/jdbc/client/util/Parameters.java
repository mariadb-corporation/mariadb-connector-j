// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2023 SingleStore, Inc.

package com.singlestore.jdbc.client.util;

/** Parameters list */
public interface Parameters {

  /**
   * get parameter at index
   *
   * @param index index
   * @return parameter
   */
  Parameter get(int index);

  /**
   * is there a parameter at requested index
   *
   * @param index index
   * @return indicate if there is a parameter at index
   */
  boolean containsKey(int index);

  /**
   * Set parameter at index
   *
   * @param index index
   * @param element parameter
   */
  void set(int index, Parameter element);

  /**
   * list size
   *
   * @return list size
   */
  int size();

  /**
   * Clone parameter list
   *
   * @return parameter list
   */
  Parameters clone();
}
