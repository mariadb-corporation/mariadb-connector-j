package org.drizzle.jdbc.packet;

import org.drizzle.jdbc.packet.buffer.WriteBuffer;

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
public class ClientAuthPacket {
    private WriteBuffer writeBuffer;

    public ClientAuthPacket(int serverCapabilities, byte serverLanguage) {
        writeBuffer = new WriteBuffer();  
        writeBuffer.writeLong(serverCapabilities & ~((1<<5)|(1<<11)|(1<<3))).
                    writeLong(4+4+1+23+2+1+1).
                    writeByte(serverLanguage).
                    writeBytes((byte)0,23).
                    writeString("aa").
                    writeByte((byte)0).
                    writeByte((byte)0);
        for(byte b : writeBuffer.toByteArrayWithLength((byte)1)) System.out.printf("%x ",b);
    }

    public byte [] toBytes() {
        int i=0;
        return writeBuffer.toByteArrayWithLength((byte)1);
    }
}
