/*
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

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Parameter;
import org.mariadb.jdbc.message.server.PrepareResultPacket;
import org.mariadb.jdbc.util.ParameterList;

public final class BulkExecutePacket implements RedoableWithPrepareClientMessage {
  private List<ParameterList> batchParameterList;
  private final String command;
  private final ServerPreparedStatement prep;
  private PrepareResultPacket prepareResult;

  public BulkExecutePacket(
      PrepareResultPacket prepareResult,
      List<ParameterList> batchParameterList,
      String command,
      ServerPreparedStatement prep) {
    this.batchParameterList = batchParameterList;
    this.prepareResult = prepareResult;
    this.command = command;
    this.prep = prep;
  }

  public void saveParameters() {
    List<ParameterList> savedList = new ArrayList<>(batchParameterList.size());
    for (int i = 0; i < batchParameterList.size(); i++) {
      savedList.add(batchParameterList.get(i).clone());
    }
    this.batchParameterList = savedList;
  }

  public int encode(PacketWriter writer, Context context, PrepareResultPacket newPrepareResult)
      throws IOException, SQLException {

    int statementId =
        (newPrepareResult != null && newPrepareResult.getStatementId() != -1)
            ? newPrepareResult.getStatementId()
            : (this.prepareResult != null ? this.prepareResult.getStatementId() : -1);

    Iterator<ParameterList> paramIterator = batchParameterList.iterator();
    ParameterList parameters = paramIterator.next();
    int parameterCount = parameters.size();

    @SuppressWarnings("rawtypes")
    Parameter<?>[] parameterHeaderType = new Parameter[parameterCount];
    // set header type
    for (int i = 0; i < parameterCount; i++) {
      parameterHeaderType[i] = parameters.get(i);
    }
    byte[] lastCmdData = null;
    int bulkPacketNo = 0;

    /**
     * Implementation After writing bunch of parameter to buffer is marked. then : - when writing
     * next bunch of parameter, if buffer grow more than max_allowed_packet, send buffer up to mark,
     * then create a new packet with current bunch of data - if bunch of parameter data type changes
     * send buffer up to mark, then create a new packet with new data type.
     *
     * <p>Problem remains if a bunch of parameter is bigger than max_allowed_packet
     */
    main_loop:
    while (true) {
      bulkPacketNo++;

      writer.initPacket();
      writer.writeByte(0xfa); // COM_STMT_BULK_EXECUTE
      writer.writeInt(statementId);
      writer.writeShort((short) 128); // always SEND_TYPES_TO_SERVER

      for (int i = 0; i < parameterCount; i++) {
        writer.writeShort((short) parameterHeaderType[i].getBinaryEncodeType());
      }

      if (lastCmdData != null) {
        writer.checkMaxAllowedLength(lastCmdData.length);
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
          Parameter<?> param = parameters.get(i);
          if (param.isNull()) {
            writer.writeByte(0x01); // value is null
          } else {
            writer.writeByte(0x00); // value follow
            param.encodeBinary(writer, context);
          }
        }

        if (!writer.isMarked() && writer.hasFlushed()) {
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
          break parameter_loop;
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
              != parameters.get(i).getBinaryEncodeType()) {
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

  public int batchUpdateLength() {
    return batchParameterList.size();
  }

  public String getCommand() {
    return command;
  }

  public ServerPreparedStatement prep() {
    return prep;
  }

  public boolean binaryProtocol() {
    return true;
  }

  public String description() {
    return command;
  }

  public void setPrepareResult(PrepareResultPacket prepareResult) {
    this.prepareResult = prepareResult;
  }
}
