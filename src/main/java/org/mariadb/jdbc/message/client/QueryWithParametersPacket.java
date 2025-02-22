// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.client.util.Parameters;
import org.mariadb.jdbc.message.ClientMessage;
import org.mariadb.jdbc.plugin.codec.ByteArrayCodec;
import org.mariadb.jdbc.util.ClientParser;

/**
 * Query client packet COM_QUERY see https://mariadb.com/kb/en/com_query/ same than QueryPacket, but
 * with parameters that will be escaped
 */
public final class QueryWithParametersPacket implements RedoableClientMessage {

  private final String preSqlCmd;
  private final ClientParser parser;
  private final InputStream localInfileInputStream;
  private List<Parameters> parametersList;
  
  /**
   * Constructor
   *
   * @param preSqlCmd additional pre command
   * @param parser command parser result
   * @param parameters parameters
   * @param localInfileInputStream local infile input stream
   */
  public QueryWithParametersPacket(
      String preSqlCmd,
      ClientParser parser,
      Parameters parameters,
      InputStream localInfileInputStream) {
    this.preSqlCmd = preSqlCmd;
    this.parser = parser;
    this.parametersList = List.of(parameters);
    this.localInfileInputStream = localInfileInputStream;
  }
  
  /**
   * Constructor for a packet containing multiple sets of parameters
   *
   * @param preSqlCmd additional pre command
   * @param parser command parser result
   * @param parametersList list of parameters
   */
  public QueryWithParametersPacket(
      String preSqlCmd,
      ClientParser parser,
      List<Parameters> parametersList) {
    this.preSqlCmd = preSqlCmd;
    this.parser = parser;
    this.parametersList = parametersList;
    this.localInfileInputStream = null;
  }

  @Override
  public void ensureReplayable(Context context) throws IOException, SQLException {
    for (int j = 0; j < parametersList.size(); j++) {
      Parameters parameters = parametersList.get(j);
      int parameterCount = parameters.size();
      for (int i = 0; i < parameterCount; i++) {
        Parameter p = parameters.get(i);
        if (!p.isNull() && p.canEncodeLongData()) {
          parameters.set(
              i, new org.mariadb.jdbc.codec.Parameter<>(ByteArrayCodec.INSTANCE, p.encodeData()));
        }
      }
    }
  }

  @Override
  public void saveParameters() {
    List<Parameters> clonedParameterList = new ArrayList<Parameters>(parametersList.size());
	for (int j = 0; j < parametersList.size(); j++) {
	  clonedParameterList.add(parametersList.get(j).clone());
	}
	this.parametersList = clonedParameterList;
  }

  @Override
  public int encode(Writer encoder, Context context) throws IOException, SQLException {
    encoder.initPacket();
    encoder.writeByte(0x03);
    if (preSqlCmd != null) encoder.writeAscii(preSqlCmd);
    if (parser.getParamPositions().size() == 0) {
      encoder.writeBytes(parser.getQuery());
      
    } else if (parser.getValuesBracketPositions() == null || parametersList.size() == 1) {
      Parameters parameters = parametersList.get(0);
      int pos = 0;
      int paramPos;
      for (int i = 0; i < parser.getParamPositions().size(); i++) {
        paramPos = parser.getParamPositions().get(i);
        encoder.writeBytes(parser.getQuery(), pos, paramPos - pos);
        pos = paramPos + 1;
        parameters.get(i).encodeText(encoder, context);
      }
      encoder.writeBytes(parser.getQuery(), pos, parser.getQuery().length - pos);
      
    } else {
      // do the rewriting here
      int startValuePos = parser.getValuesBracketPositions().get(0);
      int endValuePos = parser.getValuesBracketPositions().get(1);

      // all parameters must be inside the values block.
      int pos = 0;  // current byte position within parser.getQuery()
      int paramPos; // next placeholder byte position
      for (int j = 0; j < parametersList.size(); j++) {
    	Parameters parameters = parametersList.get(j);
        for (int i = 0; i < parser.getParamPositions().size(); i++) {
          paramPos = parser.getParamPositions().get(i);
          encoder.writeBytes(parser.getQuery(), pos, paramPos - pos);
          pos = paramPos + 1;
          parameters.get(i).encodeText(encoder, context);
        }
        if (j < parametersList.size() - 1) {
          encoder.writeBytes(parser.getQuery(), pos, endValuePos - pos + 1);
          encoder.writeByte(',');
          pos = startValuePos;
        }
      }
      encoder.writeBytes(parser.getQuery(), pos, parser.getQuery().length - pos);
    }
    
    encoder.flush();
    return 1;
  }
  
  @Override
  public int batchUpdateLength() {
    return 1;
  }

  @Override
  public boolean validateLocalFileName(String fileName, Context context) {
    return ClientMessage.validateLocalFileName(parser.getSql(), parametersList.get(0), fileName, context);
  }

  @Override
  public InputStream getLocalInfileInputStream() {
    return localInfileInputStream;
  }

  @Override
  public String description() {
    return parser.getSql();
  }

}
