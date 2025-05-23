// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.client;

import static org.mariadb.jdbc.util.constants.Capabilities.BULK_UNIT_RESULTS;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.client.util.Parameters;
import org.mariadb.jdbc.export.MaxAllowedPacketException;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

/**
 * batch execution. This relies on COM_STMT_BULK_EXECUTE
 *
 * @see <a href="https://mariadb.com/kb/en/com_stmt_bulk_execute/">documentation</a>
 */
public final class BulkExecutePacket implements RedoableWithPrepareClientMessage {
  private static final int FLAG_SEND_UNIT_RESULTS = 64;
  private static final int FLAG_SEND_TYPES_TO_SERVER = 128;
  private final String command;
  private final BasePreparedStatement prep;
  private List<Parameters> batchParameterList;
  private Prepare prepareResult;
  private boolean mightBeBulkResult;

  /**
   * Constructor
   *
   * @param prepareResult prepare result
   * @param batchParameterList batch parameter list
   * @param command sql command
   * @param prep object creator
   */
  public BulkExecutePacket(
      Prepare prepareResult,
      List<Parameters> batchParameterList,
      String command,
      BasePreparedStatement prep) {
    this.batchParameterList = batchParameterList;
    this.prepareResult = prepareResult;
    this.command = command;
    this.prep = prep;
  }

  public void saveParameters() {
    List<Parameters> savedList = new ArrayList<>(batchParameterList.size());
    for (Parameters parameterList : batchParameterList) {
      savedList.add(parameterList.clone());
    }
    this.batchParameterList = savedList;
  }

  public int encode(Writer writer, Context context, Prepare newPrepareResult)
      throws IOException, SQLException {

    int statementId =
        (newPrepareResult != null && newPrepareResult.getStatementId() != -1)
            ? newPrepareResult.getStatementId()
            : (this.prepareResult != null ? this.prepareResult.getStatementId() : -1);

    Iterator<Parameters> paramIterator = batchParameterList.iterator();
    Parameters parameters = paramIterator.next();
    int parameterCount = parameters.size();

    @SuppressWarnings("rawtypes")
    Parameter[] parameterHeaderType = new Parameter[parameterCount];
    // set header type
    for (int i = 0; i < parameterCount; i++) {
      parameterHeaderType[i] = parameters.get(i);
    }
    byte[] lastCmdData = null;
    int bulkPacketNo = 0;
    mightBeBulkResult = context.hasClientCapability(BULK_UNIT_RESULTS);
    // Implementation After writing a bunch of parameter to buffer is marked. then : - when writing
    // next bunch of parameter, if buffer grow more than max_allowed_packet, send buffer up to mark,
    // then create a new packet with current bunch of data - if a bunch of parameter data type
    // changes
    // send buffer up to mark, then create a new packet with new data type.
    // Problem remains if a bunch of parameter is bigger than max_allowed_packet
    main_loop:
    while (true) {
      bulkPacketNo++;

      writer.initPacket();
      writer.writeByte(0xfa); // COM_STMT_BULK_EXECUTE
      writer.writeInt(statementId);
      short calculatedFlags = FLAG_SEND_TYPES_TO_SERVER;
      if (mightBeBulkResult) {
        calculatedFlags |= FLAG_SEND_UNIT_RESULTS;
      }
      writer.writeShort(calculatedFlags);

      if ((calculatedFlags & FLAG_SEND_TYPES_TO_SERVER) == FLAG_SEND_TYPES_TO_SERVER) {
        for (int i = 0; i < parameterCount; i++) {
          writer.writeShort((short) parameterHeaderType[i].getBinaryEncodeType());
        }
      }

      if (lastCmdData != null) {
        if (writer.throwMaxAllowedLength(lastCmdData.length)) {
          throw new MaxAllowedPacketException(
              "query size is >= to max_allowed_packet", writer.getCmdLength() != 0);
        }
        writer.writeBytes(lastCmdData);
        writer.mark();
        lastCmdData = null;
        if (!paramIterator.hasNext()) {
          break;
        }
        parameters = paramIterator.next();
      }

      parameter_loop:
      while (true) {
        for (int i = 0; i < parameterCount; i++) {
          Parameter param = parameters.get(i);
          if (param.isNull()) {
            writer.writeByte(0x01); // value is null
          } else {
            writer.writeByte(0x00); // value follow
            param.encodeBinary(writer, context);
          }
        }

        if (!writer.bufIsDataAfterMark() && !writer.isMarked() && writer.hasFlushed()) {
          // parameter were too big to fit in a MySQL packet
          // need to finish the packet separately
          writer.flush();
          if (!paramIterator.hasNext()) {
            break main_loop;
          }
          parameters = paramIterator.next();
          // reset header type
          for (int j = 0; j < parameterCount; j++) {
            parameterHeaderType[j] = parameters.get(j);
          }
          break;
        }

        if (writer.isMarked() && writer.throwMaxAllowedLength(writer.pos())) {
          // for max_allowed_packet < 16Mb
          // packet length was ok at last mark, but won't with new data
          writer.flushBufferStopAtMark();
          writer.mark();
          lastCmdData = writer.resetMark();
          break;
        }

        writer.mark();

        if (writer.bufIsDataAfterMark()) {
          // flush has been done
          lastCmdData = writer.resetMark();
          break;
        }

        if (!paramIterator.hasNext()) {
          break main_loop;
        }

        parameters = paramIterator.next();

        // ensure type has not changed
        for (int i = 0; i < parameterCount; i++) {
          if (parameterHeaderType[i].getBinaryEncodeType()
                  != parameters.get(i).getBinaryEncodeType()
              && !parameters.get(i).isNull()) {
            writer.flush();
            // reset header type
            for (int j = 0; j < parameterCount; j++) {
              parameterHeaderType[j] = parameters.get(j);
            }
            break parameter_loop;
          }
        }
      }
    }

    writer.flush();

    return bulkPacketNo;
  }

  public boolean mightBeBulkResult() {
    return mightBeBulkResult;
  }

  public int batchUpdateLength() {
    return batchParameterList.size();
  }

  public String getCommand() {
    return command;
  }

  public BasePreparedStatement prep() {
    return prep;
  }

  public boolean binaryProtocol() {
    return true;
  }

  public String description() {
    return "BULK: " + command;
  }

  public void setPrepareResult(PrepareResultPacket prepareResult) {
    this.prepareResult = prepareResult;
  }
}
