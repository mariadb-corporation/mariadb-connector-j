/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;


public class UpdateResultSetMethodsTest extends BaseTest {
    private ResultSet rs;

    @Before
    public void before() throws SQLException {
        rs = sharedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE).executeQuery("select 1");
    }

    @Test
    public void testInsertRow() throws SQLException {
        rs.insertRow();
    }

    @Test
    public void testDeleteRow() throws SQLException {
        try {
            rs.deleteRow();
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testUpdateRow() throws SQLException {
        rs.updateRow();
    }

    @Test
    public void testRefreshRow() throws SQLException {
        try {
            rs.refreshRow();
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testCancelRowUpdates() throws SQLException {
        rs.cancelRowUpdates();
    }

    @Test
    public void testMoveToInsertRow() throws SQLException {
        try {
            rs.moveToInsertRow();
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testMoveToCurrentRow() throws SQLException {
        rs.moveToCurrentRow();
    }

    @Test
    public void testupdateBinaryStream() throws SQLException {
        try {
            rs.updateBinaryStream(1, null, 0);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBinaryStream2() throws SQLException {
        try {
            rs.updateBinaryStream("", null, 0);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateObject() throws SQLException {
        try {
            rs.updateObject(1, null, 0);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateObject2() throws SQLException {
        try {
            rs.updateObject("", null, 0);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateCharStream() throws SQLException {
        try {
            rs.updateCharacterStream(1, null, 0);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateCharStream2() throws SQLException {
        try {
            rs.updateCharacterStream("", null, 0);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateAsciiStream() throws SQLException {
        try {
            rs.updateAsciiStream(1, null, 0);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateAsciiStream2() throws SQLException {
        try {
            rs.updateAsciiStream("a", null, 0);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNull0() throws SQLException {
        try {
            rs.updateNull(1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNull1() throws SQLException {
        try {
            rs.updateNull("a");
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBoolean2() throws SQLException {
        try {
            rs.updateBoolean(1, false);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBoolean3() throws SQLException {
        try {
            rs.updateBoolean("a", false);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateByte4() throws SQLException {
        try {
            rs.updateByte(1, (byte) 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateByte5() throws SQLException {
        try {
            rs.updateByte("a", (byte) 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateShort6() throws SQLException {
        try {
            rs.updateShort(1, (short) 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateShort7() throws SQLException {
        try {
            rs.updateShort("a", (short) 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateInt8() throws SQLException {
        try {
            rs.updateInt(1, 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateInt9() throws SQLException {
        try {
            rs.updateInt("a", 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateLong10() throws SQLException {
        try {
            rs.updateLong(1, (long) 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateLong11() throws SQLException {
        try {
            rs.updateLong("a", (long) 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateFloat12() throws SQLException {
        try {
            rs.updateFloat(1, (float) 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateFloat13() throws SQLException {
        try {
            rs.updateFloat("a", (float) 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateDouble14() throws SQLException {
        try {
            rs.updateDouble(1, (double) 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateDouble15() throws SQLException {
        try {
            rs.updateDouble("a", (double) 1);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBigDecimal16() throws SQLException {
        try {
            rs.updateBigDecimal(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBigDecimal17() throws SQLException {
        try {
            rs.updateBigDecimal("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateString18() throws SQLException {
        try {
            rs.updateString(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateString19() throws SQLException {
        try {
            rs.updateString("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBytes20() throws SQLException {
        try {
            rs.updateBytes(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBytes21() throws SQLException {
        try {
            rs.updateBytes("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateDate22() throws SQLException {
        try {
            rs.updateDate(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateDate23() throws SQLException {
        try {
            rs.updateDate("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateTime24() throws SQLException {
        try {
            rs.updateTime(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateTime25() throws SQLException {
        try {
            rs.updateTime("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateTimestamp26() throws SQLException {
        try {
            rs.updateTimestamp(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateTimestamp27() throws SQLException {
        try {
            rs.updateTimestamp("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateAsciiStream28() throws SQLException {
        try {
            rs.updateAsciiStream(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateAsciiStream29() throws SQLException {
        try {
            rs.updateAsciiStream("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateAsciiStream30() throws SQLException {
        try {
            rs.updateAsciiStream(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateAsciiStream31() throws SQLException {
        try {
            rs.updateAsciiStream("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateAsciiStream32() throws SQLException {
        try {
            rs.updateAsciiStream(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateAsciiStream33() throws SQLException {
        try {
            rs.updateAsciiStream("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBinaryStream34() throws SQLException {
        try {
            rs.updateBinaryStream(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBinaryStream35() throws SQLException {
        try {
            rs.updateBinaryStream("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBinaryStream36() throws SQLException {
        try {
            rs.updateBinaryStream(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBinaryStream37() throws SQLException {
        try {
            rs.updateBinaryStream("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBinaryStream38() throws SQLException {
        try {
            rs.updateBinaryStream(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBinaryStream39() throws SQLException {
        try {
            rs.updateBinaryStream("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateCharacterStream40() throws SQLException {
        try {
            rs.updateCharacterStream(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateCharacterStream41() throws SQLException {
        try {
            rs.updateCharacterStream("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateCharacterStream42() throws SQLException {
        try {
            rs.updateCharacterStream(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateCharacterStream43() throws SQLException {
        try {
            rs.updateCharacterStream("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateCharacterStream44() throws SQLException {
        try {
            rs.updateCharacterStream(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateCharacterStream45() throws SQLException {
        try {
            rs.updateCharacterStream("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateObject46() throws SQLException {
        try {
            rs.updateObject(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateObject47() throws SQLException {
        try {
            rs.updateObject(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateObject48() throws SQLException {
        try {
            rs.updateObject("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateObject49() throws SQLException {
        try {
            rs.updateObject("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBlob52() throws SQLException {
        try {
            rs.updateBlob(1, (java.sql.Blob) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBlob53() throws SQLException {
        try {
            rs.updateBlob("a", (java.sql.Blob) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBlob54() throws SQLException {
        try {
            rs.updateBlob(1, (java.io.InputStream) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBlob55() throws SQLException {
        try {
            rs.updateBlob("a", (java.io.InputStream) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBlob56() throws SQLException {
        try {
            rs.updateBlob(1, (java.io.InputStream) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateBlob57() throws SQLException {
        try {
            rs.updateBlob("a", (java.io.InputStream) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateClob58() throws SQLException {
        try {
            rs.updateClob(1, (java.sql.Clob) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateClob59() throws SQLException {
        try {
            rs.updateClob("a", (java.sql.Clob) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateClob60() throws SQLException {
        try {
            rs.updateClob(1, (java.io.Reader) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateClob61() throws SQLException {
        try {
            rs.updateClob("a", (java.io.Reader) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateClob62() throws SQLException {
        try {
            rs.updateClob(1, (java.io.Reader) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateClob63() throws SQLException {
        try {
            rs.updateClob("a", (java.io.Reader) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNString68() throws SQLException {
        try {
            rs.updateNString(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNString69() throws SQLException {
        try {
            rs.updateNString("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNClob70() throws SQLException {
        try {
            rs.updateNClob(1, (java.sql.NClob) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNClob71() throws SQLException {
        try {
            rs.updateNClob("a", (java.sql.NClob) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNClob72() throws SQLException {
        try {
            rs.updateNClob(1, (java.io.Reader) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNClob73() throws SQLException {
        try {
            rs.updateNClob("a", (java.io.Reader) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNClob74() throws SQLException {
        try {
            rs.updateNClob(1, (java.io.Reader) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNClob75() throws SQLException {
        try {
            rs.updateNClob("a", (java.io.Reader) null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNCharacterStream78() throws SQLException {
        try {
            rs.updateNCharacterStream(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNCharacterStream79() throws SQLException {
        try {
            rs.updateNCharacterStream("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNCharacterStream80() throws SQLException {
        try {
            rs.updateNCharacterStream(1, null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }

    @Test
    public void testupdateNCharacterStream81() throws SQLException {
        try {
            rs.updateNCharacterStream("a", null);
        } catch (SQLFeatureNotSupportedException sqle) {
            Assert.fail();
        } catch (SQLException sqlee) {
            //eat
        }
    }


}
