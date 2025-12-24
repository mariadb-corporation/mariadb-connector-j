// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.client;

/**
 * MySQL Protocol Command Constants
 * 
 * @see <a href="https://mariadb.com/kb/en/mariadb-protocol/">MariaDB Protocol</a>
 */
public final class CommandConstants {
  
  /** COM_QUIT - Close connection */
  public static final byte COM_QUIT = 0x01;
  
  /** COM_INIT_DB - Change database */
  public static final byte COM_INIT_DB = 0x02;
  
  /** COM_QUERY - Execute SQL query */
  public static final byte COM_QUERY = 0x03;
  
  /** COM_PING - Ping server */
  public static final byte COM_PING = 0x0e;
  
  /** COM_STMT_PREPARE - Prepare statement */
  public static final byte COM_STMT_PREPARE = 0x16;
  
  /** COM_STMT_EXECUTE - Execute prepared statement */
  public static final byte COM_STMT_EXECUTE = 0x17;
  
  /** COM_STMT_SEND_LONG_DATA - Send long data for prepared statement */
  public static final byte COM_STMT_SEND_LONG_DATA = 0x18;
  
  /** COM_STMT_CLOSE - Close prepared statement */
  public static final byte COM_STMT_CLOSE = 0x19;
  
  /** COM_RESET_CONNECTION - Reset connection state */
  public static final byte COM_RESET_CONNECTION = 0x1f;
  
  /** COM_STMT_BULK_EXECUTE - Bulk execute prepared statement (MariaDB specific) */
  public static final byte COM_STMT_BULK_EXECUTE = (byte) 0xfa;
  
  /** Cursor type: NO_CURSOR */
  public static final byte CURSOR_TYPE_NO_CURSOR = 0x00;
  
  /** Parameter type flag present */
  public static final byte PARAMETER_TYPE_FLAG = 0x01;
  
  /** Parameter value is null */
  public static final byte PARAMETER_NULL = 0x01;
  
  /** Parameter value follows */
  public static final byte PARAMETER_NOT_NULL = 0x00;
  
  private CommandConstants() {
    // Utility class, no instantiation
  }
}
