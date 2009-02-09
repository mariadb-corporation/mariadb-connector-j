package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.WriteBuffer;

/**
 4                            client_flags
 4                            max_packet_size
 1                            charset_number
 23                           (filler) always 0x00...
 n (Null-Terminated String)   user
 n (Length Coded Binary)      scramble_buff (1 + x bytes)
 1                            (filler) always 0x00
 n (Null-Terminated String)   databasename

 client_flags:            CLIENT_xxx options. The list of possible flag
                          values is in the description of the Handshake
                          Initialisation Packet, for server_capabilities.
                          For some of the bits, the server passed "what
                          it's capable of". The client leaves some of the
                          bits on, adds others, and passes back to the server.
                          One important flag is: whether compression is desired.

 max_packet_size:         the maximum number of bytes in a packet for the client

 charset_number:          in the same domain as the server_language field that
                          the server passes in the Handshake Initialization packet.

 user:                    identification

 scramble_buff:           the password, after encrypting using the scramble_buff
                          contents passed by the server (see "Password functions"
                          section elsewhere in this document)
                          if length is zero, no password was given

 databasename:            name of schema to use initially

 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 11:19:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class ClientAuthPacket implements DrizzlePacket {
    private WriteBuffer writeBuffer;
    private int serverCapabilities;
    private byte serverLanguage;
    private String username;
    private String password;
    private String database;

    public ClientAuthPacket(String username,String password,String database) {
        writeBuffer = new WriteBuffer();
        this.username=username;
        this.password=password;
        this.database=database;
    }
// TODO: fix passing the initial database...
    public byte [] toBytes(byte queryNumber) {
        writeBuffer.writeInt(serverCapabilities & ~((1<<5)|(1<<11)|(1<<3))).
                writeInt(4+4+1+23+username.length()+1+1+1+database.length()+1).
                    writeByte(serverLanguage). //1
                    writeBytes((byte)0,23).    //4
                    writeString(username).     //strlen username
                    writeByte((byte)0).        //1
                    //writeString(scramblePassword(password)) //strlen(scramb)
                    writeByte((byte)0).        //1
                    writeByte((byte)0).
                    writeString(database).     //strlen(database)
                    writeByte((byte)0);        //1
        return writeBuffer.toByteArrayWithLength(queryNumber);
    }

    public void setServerCapabilities(int serverCapabilities) {
        this.serverCapabilities = serverCapabilities;
    }

    public void setServerLanguage(byte serverLanguage) {
        this.serverLanguage = serverLanguage;
    }
}
