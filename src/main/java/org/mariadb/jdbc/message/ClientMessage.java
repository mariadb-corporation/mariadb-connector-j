// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.message;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.client.result.StreamingResult;
import org.mariadb.jdbc.client.result.UpdatableResult;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.ClosableLock;
import org.mariadb.jdbc.client.util.Parameters;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.message.server.OkPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public interface ClientMessage {

  /** LOAD DATA/XML LOCAL INFILE statement prefix, matched at a statement start only */
  Pattern LOAD_LOCAL_INFILE_PREFIX =
      Pattern.compile(
          "LOAD\\s+(?:DATA|XML)\\s+(?:(?:LOW_PRIORITY|CONCURRENT)\\s+)?LOCAL\\s+INFILE\\s*",
          Pattern.CASE_INSENSITIVE);

  /**
   * Check that the file requested by the server corresponds to a LOAD DATA LOCAL INFILE statement
   * of the current command.
   *
   * <p>The command is tokenized: string literals, quoted identifiers and comments are skipped, and
   * only a LOAD [DATA|XML] LOCAL INFILE found at the start of a statement is considered. Text that
   * merely appears inside a quoted identifier or a literal, e.g. a database or table name returned
   * by a malicious server and re-used by the driver or the application in a later query, can
   * therefore never satisfy a local file request.
   *
   * @param sql current command sql
   * @param parameters current command parameter
   * @param fileName file path request
   * @param context current connection context
   * @return true if file name correspond to demand and query is a load local infile
   */
  static boolean validateLocalFileName(
      String sql, Parameters parameters, String fileName, Context context) {
    if (sql == null || fileName == null || fileName.isEmpty()) return false;
    // file name as written in the SQL text: backslashes are escaped in a literal
    String expected = fileName.replace("\\", "\\\\");
    boolean backslashEscapes =
        context == null || (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) == 0;

    int len = sql.length();
    int pos = 0;
    int paramIndex = 0; // number of '?' placeholders before the current position
    boolean statementStart = true;
    Matcher loadMatcher = LOAD_LOCAL_INFILE_PREFIX.matcher(sql);

    while (pos < len) {
      char c = sql.charAt(pos);

      if (Character.isWhitespace(c)) {
        pos++;
        continue;
      }

      // comments: '# ...', '-- ...' (dashes followed by whitespace) and '/* ... */'
      if (c == '#'
          || (c == '-'
              && pos + 1 < len
              && sql.charAt(pos + 1) == '-'
              && (pos + 2 >= len || Character.isWhitespace(sql.charAt(pos + 2))))) {
        pos = skipToEndOfLine(sql, pos);
        continue;
      }
      if (c == '/' && pos + 1 < len && sql.charAt(pos + 1) == '*') {
        int endComment = sql.indexOf("*/", pos + 2);
        pos = endComment == -1 ? len : endComment + 2;
        continue;
      }

      if (c == ';') {
        statementStart = true;
        pos++;
        continue;
      }

      if (statementStart) {
        statementStart = false;
        loadMatcher.region(pos, len);
        if (loadMatcher.lookingAt()) {
          int after = loadMatcher.end();
          if (after < len && sql.charAt(after) == '\'') {
            int endLiteral = skipStringLiteral(sql, after, backslashEscapes);
            // endLiteral points after the closing quote (or sql end when unterminated)
            if (endLiteral <= len
                && sql.charAt(endLiteral - 1) == '\''
                && endLiteral - 1 > after
                && sql.substring(after + 1, endLiteral - 1).equals(expected)) {
              return true;
            }
            pos = endLiteral;
            continue;
          }
          if (after < len && sql.charAt(after) == '?') {
            if (parameters != null && paramIndex < parameters.size()) {
              String paramString = parameters.get(paramIndex).bestEffortStringValue(context);
              // null : parameter value can't be rendered to compare against the requested file
              if (paramString != null && paramString.equals("'" + expected + "'")) {
                return true;
              }
            }
            paramIndex++;
            pos = after + 1;
            continue;
          }
          pos = after;
          continue;
        }
      }

      if (c == '\'' || c == '"') {
        pos = skipStringLiteral(sql, pos, backslashEscapes);
        continue;
      }
      if (c == '`') {
        int endIdentifier = sql.indexOf('`', pos + 1);
        pos = endIdentifier == -1 ? len : endIdentifier + 1;
        continue;
      }
      if (c == '?') {
        paramIndex++;
      }
      pos++;
    }
    return false;
  }

  /**
   * Skip to the end of the current line.
   *
   * @param sql sql text
   * @param pos current position
   * @return position of the line terminator, or sql length when there is none
   */
  static int skipToEndOfLine(String sql, int pos) {
    int len = sql.length();
    while (pos < len) {
      char c = sql.charAt(pos);
      if (c == '\n' || c == '\r') return pos;
      pos++;
    }
    return len;
  }

  /**
   * Skip a string literal starting at pos (a quote character), handling backslash escapes when
   * enabled and doubled quotes.
   *
   * @param sql sql text
   * @param pos position of the opening quote
   * @param backslashEscapes whether backslash escapes the next character
   * @return position after the closing quote, or sql length when the literal is unterminated
   */
  static int skipStringLiteral(String sql, int pos, boolean backslashEscapes) {
    int len = sql.length();
    char quote = sql.charAt(pos);
    pos++;
    while (pos < len) {
      char c = sql.charAt(pos);
      if (backslashEscapes && c == '\\') {
        pos += 2;
        continue;
      }
      if (c == quote) {
        if (pos + 1 < len && sql.charAt(pos + 1) == quote) {
          pos += 2; // doubled quote inside the literal
          continue;
        }
        return pos + 1;
      }
      pos++;
    }
    return len;
  }

  /**
   * Encode client message to socket.
   *
   * @param writer socket writer
   * @param context connection context
   * @return number of client message written
   * @throws IOException if socket error occur
   * @throws SQLException if any issue occurs
   */
  int encode(Writer writer, Context context) throws IOException, SQLException;

  /**
   * Number of parameter rows, and so expected return length
   *
   * @return batch update length
   */
  default int batchUpdateLength() {
    return 0;
  }

  /**
   * Message description
   *
   * @return description
   */
  default String description() {
    return null;
  }

  /**
   * Are return value encoded in binary protocol
   *
   * @return use binary protocol
   */
  default boolean binaryProtocol() {
    return false;
  }

  /**
   * Can skip metadata
   *
   * @return can skip metadata
   */
  default boolean canSkipMeta() {
    return false;
  }

  /**
   * default packet resultset parser
   *
   * @param stmt caller
   * @param fetchSize fetch size
   * @param maxRows maximum number of rows
   * @param resultSetConcurrency resultset concurrency
   * @param resultSetType resultset type
   * @param closeOnCompletion must close caller on result parsing end
   * @param reader packet reader
   * @param writer packet writer
   * @param context connection context
   * @param exceptionFactory connection exception factory
   * @param lock thread safe locks
   * @param traceEnable is logging trace enable
   * @param message client message
   * @param redirectFct redirect consumer
   * @return results
   * @throws IOException if any socket error occurs
   * @throws SQLException for other kind of errors
   */
  default Completion readPacket(
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      Reader reader,
      Writer writer,
      Context context,
      ExceptionFactory exceptionFactory,
      ClosableLock lock,
      boolean traceEnable,
      ClientMessage message,
      Consumer<String> redirectFct)
      throws IOException, SQLException {

    ReadableByteBuf buf = reader.readReusablePacket(traceEnable);

    switch (buf.getByte()) {

        // *********************************************************************************************************
        // * OK response
        // *********************************************************************************************************
      case (byte) 0x00:
        OkPacket ok = OkPacket.parse(buf, context);
        if (context.getRedirectUrl() != null
            && (context.getServerStatus() & ServerStatus.IN_TRANSACTION) == 0
            && (context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) == 0) {
          redirectFct.accept(context.getRedirectUrl());
        }
        return ok;

        // *********************************************************************************************************
        // * ERROR response
        // *********************************************************************************************************
      case (byte) 0xff:
        // force current status to in transaction to ensure rollback/commit, since command may
        // have issue a transaction
        ErrorPacket errorPacket = new ErrorPacket(buf, context);
        throw exceptionFactory
            .withSql(this.description())
            .create(
                errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());
      case (byte) 0xfb:
        buf.skip(1); // skip header
        SQLException exception = null;
        reader.getSequence().set(writer.getSequence());
        InputStream is = getLocalInfileInputStream();
        if (is == null) {
          String fileName = buf.readStringNullEnd();
          if (!message.validateLocalFileName(fileName, context)) {
            exception =
                exceptionFactory
                    .withSql(this.description())
                    .create(
                        String.format(
                            "LOAD DATA LOCAL INFILE asked for file '%s' that doesn't correspond to"
                                + " initial query %s. Possible malicious proxy changing server"
                                + " answer ! Command interrupted",
                            fileName, this.description()),
                        "HY000");
          } else {

            try {
              is = new FileInputStream(fileName);
            } catch (FileNotFoundException f) {
              exception =
                  exceptionFactory
                      .withSql(this.description())
                      .create("Could not send file : " + f.getMessage(), "HY000", f);
            }
          }
        }

        // sending stream
        if (is != null) {
          try {
            byte[] fileBuf = new byte[8192];
            int len;
            while ((len = is.read(fileBuf)) > 0) {
              writer.writeBytes(fileBuf, 0, len);
              writer.flush();
            }
          } finally {
            is.close();
          }
        }

        // after file send / having an error, sending an empty packet to keep connection state ok
        writer.writeEmptyPacket();
        Completion completion =
            readPacket(
                stmt,
                fetchSize,
                maxRows,
                resultSetConcurrency,
                resultSetType,
                closeOnCompletion,
                reader,
                writer,
                context,
                exceptionFactory,
                lock,
                traceEnable,
                message,
                redirectFct);
        if (exception != null) {
          throw exception;
        }
        return completion;

        // *********************************************************************************************************
        // * ResultSet
        // *********************************************************************************************************
      default:
        int fieldCount = buf.readIntLengthEncodedNotNull();
        int maxAllowedColumns = context.getConf().maxAllowedColumns();
        if (fieldCount > maxAllowedColumns) {
          throw exceptionFactory
              .withSql(this.description())
              .create(
                  String.format(
                      "Server metadata announces %d columns, exceeding the maximum allowed number"
                          + " of columns (%d) permitted by the 'maxAllowedColumns' option. This may"
                          + " indicate a malicious proxy attempting to exhaust client memory."
                          + " Command interrupted.",
                      fieldCount, maxAllowedColumns),
                  "HY000");
        }

        ColumnDecoder[] ci;
        if (context.canSkipMeta() && this.canSkipMeta()) {
          if (buf.readByte() == 0) {
            // skip meta
            ci = ((BasePreparedStatement) stmt).getMeta();
          } else {
            // can skip meta, but meta might have changed
            ci = new ColumnDecoder[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
              ci[i] =
                  context
                      .getColumnDecoderFunction()
                      .apply(new StandardReadableByteBuf(reader.readPacket(traceEnable)));
            }
            ((BasePreparedStatement) stmt).updateMeta(ci);
          }
        } else {
          // always read meta
          ci = new ColumnDecoder[fieldCount];
          for (int i = 0; i < fieldCount; i++) {
            ci[i] =
                context
                    .getColumnDecoderFunction()
                    .apply(new StandardReadableByteBuf(reader.readPacket(traceEnable)));
          }
        }

        // intermediate EOF
        if (!context.isEofDeprecated()) {
          reader.skipPacket();
        }

        // read resultSet
        if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
          return new UpdatableResult(
              stmt,
              binaryProtocol(),
              maxRows,
              ci,
              reader,
              context,
              resultSetType,
              closeOnCompletion,
              traceEnable);
        }

        if (fetchSize != 0) {
          if ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
            context.setServerStatus(context.getServerStatus() - ServerStatus.MORE_RESULTS_EXISTS);
          }

          return new StreamingResult(
              stmt,
              binaryProtocol(),
              maxRows,
              ci,
              reader,
              context,
              fetchSize,
              lock,
              resultSetType,
              closeOnCompletion,
              traceEnable);
        } else {
          return new CompleteResult(
              stmt,
              binaryProtocol(),
              maxRows,
              ci,
              reader,
              context,
              resultSetType,
              closeOnCompletion,
              traceEnable,
              mightBeBulkResult());
        }
    }
  }

  /**
   * Get current local infile input stream.
   *
   * @return default to null
   */
  default InputStream getLocalInfileInputStream() {
    return null;
  }

  /**
   * Indicating if result might be a COM_STMT_BULK result
   *
   * @return true if so.
   */
  default boolean mightBeBulkResult() {
    return false;
  }

  /**
   * Request for local file to be validated from current query.
   *
   * @param fileName server file request path
   * @param context current connection context
   * @return true if file name correspond to demand and query is a load local infile
   */
  default boolean validateLocalFileName(String fileName, Context context) {
    return false;
  }
}
