// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.util;

import java.util.Arrays;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.client.util.Parameters;

public class ParameterList implements Parameters, Cloneable {
  Parameter[] elementData;
  int length;

  public ParameterList(int defaultSize) {
    elementData = new Parameter[defaultSize];
    length = 0;
  }

  public ParameterList() {
    elementData = new Parameter[10];
    length = 0;
  }

  public Parameter get(int index) {
    if (index >= length)
      throw new ArrayIndexOutOfBoundsException("wrong index " + index + " length:" + length);
    return elementData[index];
  }

  public boolean containsKey(int index) {
    if (index >= 0 && length > index) {
      return elementData[index] != null;
    }
    return false;
  }

  public void set(int index, Parameter element) {
    if (elementData.length <= index) grow(index + 1);
    elementData[index] = element;
    if (index >= length) length = index + 1;
  }

  public int size() {
    return length;
  }

  private void grow(int minLength) {
    int currLength = elementData.length;
    int newLength = Math.max(currLength + (currLength >> 1), minLength);
    elementData = Arrays.copyOf(elementData, newLength);
  }

  @Override
  public ParameterList clone() {
    ParameterList param = new ParameterList(length);
    if (length > 0) System.arraycopy(elementData, 0, param.elementData, 0, length);
    param.length = length;
    return param;
  }
}
