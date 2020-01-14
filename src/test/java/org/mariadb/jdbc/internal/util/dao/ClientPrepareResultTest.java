/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.util.dao;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClientPrepareResultTest {

  /** SELECT query cannot be rewritable. */
  @Test
  public void selectQuery() {
    // SELECT query cannot be rewritable
    assertFalse(checkRewritable("SELECT * FROM MyTable"));
    assertFalse(checkRewritable("SELECT\n * FROM MyTable"));
    assertFalse(checkRewritable("SELECT(1)"));
    assertFalse(checkRewritable("INSERT MyTable (a) VALUES (1);SELECT(1)"));
  }

  /** INSERT FROM SELECT are not be rewritable. */
  @Test
  public void insertSelectQuery() {
    assertFalse(checkRewritable("INSERT INTO MyTable (a) SELECT * FROM seq_1_to_1000"));
    assertFalse(checkRewritable("INSERT INTO MyTable (a);SELECT * FROM seq_1_to_1000"));
    assertFalse(checkRewritable("INSERT INTO MyTable (a)SELECT * FROM seq_1_to_1000"));
    assertFalse(checkRewritable("INSERT INTO MyTable (a) (SELECT * FROM seq_1_to_1000)"));
    assertFalse(checkRewritable("INSERT INTO MyTable (a) SELECT\n * FROM seq_1_to_1000"));
  }

  /**
   * Insert query that contain table/column name with select keyword, or select in comment can be
   * rewritten.
   */
  @Test
  public void rewritableThatContainSelectQuery() {
    // but 'SELECT' keyword in column/table name can be rewritable
    assertTrue(checkRewritable("INSERT INTO TABLE_SELECT "));
    assertTrue(checkRewritable("INSERT INTO TABLE_SELECT"));
    assertTrue(checkRewritable("INSERT INTO SELECT_TABLE"));
    assertTrue(checkRewritable("INSERT INTO `TABLE SELECT `"));
    assertTrue(checkRewritable("INSERT INTO TABLE /* SELECT in comment */ "));
    assertTrue(checkRewritable("INSERT INTO TABLE //SELECT"));
  }

  private boolean checkRewritable(String query) {
    return ClientPrepareResult.rewritableParts(query, true).isQueryMultiValuesRewritable();
  }
}
