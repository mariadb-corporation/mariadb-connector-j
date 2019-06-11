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

import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Assume;
import org.junit.Test;
import org.mariadb.jdbc.internal.protocol.Protocol;

public class AttributeTest extends BaseTest {

	  @Test
	  public void testServerHost() throws SQLException  {
		  //test for _server_host attribute

		  Statement statement = sharedConnection.createStatement();
		  try {
			  statement.execute("DROP USER 'test_serverhost'@'%'");
		  } catch (SQLException e) {
			  //eat exception
		  }
		  statement.execute("CREATE USER 'test_serverhost'@'%' IDENTIFIED BY 'test_serverhost'");
		  statement.execute("GRANT SELECT ON testj.* TO 'test_serverhost'@'%'");
		  statement.execute("GRANT SELECT ON performance_schema.session_connect_attrs TO 'test_serverhost'@'%'");
		  
		  Properties properties = new Properties();
		  properties.put("user", "test_serverhost");
		  properties.put("password", "test_serverhost");

		  try (Connection connection = openConnection(connU, properties)) {
			  Field protocolField =  MariaDbConnection.class.getDeclaredField("protocol");
			  protocolField.setAccessible(true);
			  Protocol protocolVal = (Protocol) protocolField.get(connection);

			  Statement attributeStatement = connection.createStatement();
			  ResultSet result = attributeStatement.executeQuery("select * from performance_schema.session_connect_attrs where ATTR_NAME='_server_host'");
			  while(result.next()) {
				  String str = result.getString("ATTR_NAME");
			      String strVal = result.getString("ATTR_VALUE");
			      System.out.println(str +"\t"+strVal);
			      
			      Assume.assumeTrue(protocolVal.getHost().matches(strVal));
			  }
  	
		  } catch (Exception e) {
			  e.printStackTrace();
			  fail();
		  }
		  statement.execute("DROP USER 'test_serverhost'@'%'");
	  }
}
