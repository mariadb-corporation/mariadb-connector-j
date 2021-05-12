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

package org.mariadb.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UpdateResultSetMethodsTest extends BaseTest {

  private ResultSet rs;

  /**
   * Initialisation.
   *
   * @throws SQLException exception
   */
  @Before
  public void beforeTest() throws SQLException {
    rs =
        sharedConnection
            .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("select 1");
  }

  @Test
  public void testInsertRow() throws SQLException {
    rs.insertRow();
  }

  @Test
  public void testDeleteRow() {
    Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> rs.deleteRow());
  }

  @Test
  public void testUpdateRow() throws SQLException {
    rs.updateRow();
  }

  @Test
  public void testRefreshRow() {
    try {
      rs.refreshRow();
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testCancelRowUpdates() throws SQLException {
    rs.cancelRowUpdates();
  }

  @Test
  public void testMoveToInsertRow() {
    Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> rs.moveToInsertRow());
  }

  @Test
  public void testMoveToCurrentRow() throws SQLException {
    rs.moveToCurrentRow();
  }

  @Test
  public void testupdateBinaryStream() {
    try {
      rs.updateBinaryStream(1, null, 0);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBinaryStream2() {
    try {
      rs.updateBinaryStream("", null, 0);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateObject() {
    try {
      rs.updateObject(1, null, 0);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateObject2() {
    try {
      rs.updateObject("", null, 0);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateCharStream() {
    try {
      rs.updateCharacterStream(1, null, 0);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateCharStream2() {
    try {
      rs.updateCharacterStream("", null, 0);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateAsciiStream() {
    try {
      rs.updateAsciiStream(1, null, 0);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateAsciiStream2() {
    try {
      rs.updateAsciiStream("a", null, 0);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNull0() {
    try {
      rs.updateNull(1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNull1() {
    try {
      rs.updateNull("a");
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBoolean2() {
    try {
      rs.updateBoolean(1, false);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBoolean3() {
    try {
      rs.updateBoolean("a", false);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateByte4() {
    try {
      rs.updateByte(1, (byte) 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateByte5() {
    try {
      rs.updateByte("a", (byte) 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateShort6() {
    try {
      rs.updateShort(1, (short) 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateShort7() {
    try {
      rs.updateShort("a", (short) 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateInt8() {
    try {
      rs.updateInt(1, 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateInt9() {
    try {
      rs.updateInt("a", 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateLong10() {
    try {
      rs.updateLong(1, 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateLong11() {
    try {
      rs.updateLong("a", 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateFloat12() {
    try {
      rs.updateFloat(1, (float) 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateFloat13() {
    try {
      rs.updateFloat("a", (float) 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateDouble14() {
    try {
      rs.updateDouble(1, 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateDouble15() {
    try {
      rs.updateDouble("a", 1);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBigDecimal16() {
    try {
      rs.updateBigDecimal(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBigDecimal17() {
    try {
      rs.updateBigDecimal("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateString18() {
    try {
      rs.updateString(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateString19() {
    try {
      rs.updateString("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBytes20() {
    try {
      rs.updateBytes(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBytes21() {
    try {
      rs.updateBytes("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateDate22() {
    try {
      rs.updateDate(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateDate23() {
    try {
      rs.updateDate("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateTime24() {
    try {
      rs.updateTime(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateTime25() {
    try {
      rs.updateTime("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateTimestamp26() {
    try {
      rs.updateTimestamp(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateTimestamp27() {
    try {
      rs.updateTimestamp("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateAsciiStream28() {
    try {
      rs.updateAsciiStream(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateAsciiStream29() {
    try {
      rs.updateAsciiStream("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateAsciiStream30() {
    try {
      rs.updateAsciiStream(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateAsciiStream31() {
    try {
      rs.updateAsciiStream("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateAsciiStream32() {
    try {
      rs.updateAsciiStream(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateAsciiStream33() {
    try {
      rs.updateAsciiStream("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBinaryStream34() {
    try {
      rs.updateBinaryStream(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBinaryStream35() {
    try {
      rs.updateBinaryStream("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBinaryStream36() {
    try {
      rs.updateBinaryStream(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBinaryStream37() {
    try {
      rs.updateBinaryStream("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBinaryStream38() {
    try {
      rs.updateBinaryStream(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBinaryStream39() {
    try {
      rs.updateBinaryStream("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateCharacterStream40() {
    try {
      rs.updateCharacterStream(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateCharacterStream41() {
    try {
      rs.updateCharacterStream("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateCharacterStream42() {
    try {
      rs.updateCharacterStream(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateCharacterStream43() {
    try {
      rs.updateCharacterStream("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateCharacterStream44() {
    try {
      rs.updateCharacterStream(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateCharacterStream45() {
    try {
      rs.updateCharacterStream("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateObject46() {
    try {
      rs.updateObject(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateObject47() {
    try {
      rs.updateObject(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateObject48() {
    try {
      rs.updateObject("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateObject49() {
    try {
      rs.updateObject("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBlob52() {
    try {
      rs.updateBlob(1, (java.sql.Blob) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBlob53() {
    try {
      rs.updateBlob("a", (java.sql.Blob) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBlob54() {
    try {
      rs.updateBlob(1, (java.io.InputStream) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBlob55() {
    try {
      rs.updateBlob("a", (java.io.InputStream) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBlob56() {
    try {
      rs.updateBlob(1, (java.io.InputStream) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateBlob57() {
    try {
      rs.updateBlob("a", (java.io.InputStream) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateClob58() {
    try {
      rs.updateClob(1, (java.sql.Clob) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateClob59() {
    try {
      rs.updateClob("a", (java.sql.Clob) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateClob60() {
    try {
      rs.updateClob(1, (java.io.Reader) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateClob61() {
    try {
      rs.updateClob("a", (java.io.Reader) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateClob62() {
    try {
      rs.updateClob(1, (java.io.Reader) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateClob63() {
    try {
      rs.updateClob("a", (java.io.Reader) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNString68() {
    try {
      rs.updateNString(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNString69() {
    try {
      rs.updateNString("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNClob70() {
    try {
      rs.updateNClob(1, (java.sql.NClob) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNClob71() {
    try {
      rs.updateNClob("a", (java.sql.NClob) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNClob72() {
    try {
      rs.updateNClob(1, (java.io.Reader) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNClob73() {
    try {
      rs.updateNClob("a", (java.io.Reader) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNClob74() {
    try {
      rs.updateNClob(1, (java.io.Reader) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNClob75() {
    try {
      rs.updateNClob("a", (java.io.Reader) null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNCharacterStream78() {
    try {
      rs.updateNCharacterStream(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNCharacterStream79() {
    try {
      rs.updateNCharacterStream("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNCharacterStream80() {
    try {
      rs.updateNCharacterStream(1, null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }

  @Test
  public void testupdateNCharacterStream81() {
    try {
      rs.updateNCharacterStream("a", null);
    } catch (SQLFeatureNotSupportedException sqle) {
      Assert.fail();
    } catch (SQLException sqlee) {
      // eat
    }
  }
}
