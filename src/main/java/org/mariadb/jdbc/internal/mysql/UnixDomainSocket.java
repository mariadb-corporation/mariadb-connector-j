package org.mariadb.jdbc.internal.mysql;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Structure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;


public class UnixDomainSocket extends Socket {
    public static final int AF_UNIX = 1;
    public static final int SOCK_STREAM = 1;
    public static final int PROTOCOL = 0;
    public static final short AF_INET = 1;

    public static class SockAddr extends Structure {
        public short  sun_family;
        public byte[] sun_path;

        public SockAddr(String sunPath) {
            sun_family = AF_INET;
            byte[] arr = sunPath.getBytes();
            sun_path = new byte[arr.length +1];
            System.arraycopy(arr, 0, sun_path, 0, Math.min(sun_path.length - 1, arr.length));
            allocateMemory();
        }
    }

    public static native int socket(int domain, int type, int protocol) throws LastErrorException;
    public static native int connect(int sockfd, SockAddr sockaddr, int addrlen) throws LastErrorException;
    public static native int read(int fd, byte[] buffer, int count) throws LastErrorException;
    public static native int write(int fd, byte[] buffer, int count) throws LastErrorException;
    public static native int recv(int fd, byte[] buffer , int count, int flags) throws LastErrorException;
    public static native int send(int fd, byte[] buffer, int count, int flags) throws LastErrorException;
    public static native int close(int fd) throws LastErrorException;
    public static native String strerror(int errno);

    static {
        if (!Platform.isWindows() && !Platform.isWindowsCE()){
            Native.register("c");
        }
    }
    String path;
    InputStream is;
    OutputStream os;
    SockAddr sockaddr;
    int fd;

    class UnixSocketInputStream extends InputStream {
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                int size = recv(fd, b, len, 0);
                return size;
            } catch (LastErrorException lee) {
               throw new IOException("native read() failed : " + formatError(lee));
            }
        }
        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            int bytesRead =  read(b);
            if (bytesRead == 0) {
                return -1;
            }
            return b[0] & 0xff;
        }
        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }
    }

    class UnixSocketOutputStream extends OutputStream {
        @Override
        public void write(byte[]b, int off, int len) throws IOException{
            try  {
                int size = send(fd,  b, len ,0);
                if (size != len) {
                    throw new IOException("can't write " + len + "bytes");
                }
            }
            catch (LastErrorException lee) {
                throw new IOException("native write() failed : " + formatError(lee));
            }
          }
          @Override
          public void write(int b) throws IOException {
              write(new byte[]{(byte)b});
          }
          @Override
          public void write(byte[] b) throws IOException {
              write(b, 0, b.length);
          }
    }

    static String formatError(LastErrorException lee) {
        try {
            return strerror(lee.getErrorCode());
        } catch (Throwable t) {
            return lee.getMessage();
        }
    }


    public UnixDomainSocket(String path) throws IOException {
        if (Platform.isWindows() ||Platform.isWindowsCE()){
            throw new IOException("Unix domain sockets are not supported on Windows");
        }
        this.path = path;
        sockaddr = new SockAddr(path);
        try {
            fd = socket(AF_UNIX,SOCK_STREAM,PROTOCOL);
        } catch( LastErrorException lee) {
            throw new IOException("native socket() failed : " + formatError(lee));
        }
    }

    @Override
    public void  close() throws IOException {
        try {
            close(fd);
            fd = -1;
        } catch(LastErrorException lee) {
            throw new IOException("native close() failed : " + formatError(lee));
        }
    }

    @Override
    public void   connect(SocketAddress endpoint) throws IOException {
        connect(endpoint, 0);
    }

    public void   connect(SocketAddress endpoint, int timeout) throws IOException {
        try {
            int ret = connect(fd, sockaddr, sockaddr.size());
            if (ret != 0) {
                throw new IOException(strerror(Native.getLastError()));
            }
        } catch (LastErrorException lee) {
            throw new IOException("native connect() failed : " + formatError(lee));
        }
        is = new UnixSocketInputStream();
        os = new UnixSocketOutputStream();
    }
    public InputStream getInputStream() {
        return is;
    }
    public OutputStream getOutputStream() {
        return os;
    }
    public void setTcpNoDelay(boolean b) {
    }
    public void setKeepAlive(boolean b) {
    }
    public void setReceiveBufferSize(int size) {
    }
    public void setSendBufferSize(int size) {   
    }
    public void setSoLinger(boolean b, int i) {
    }
    public void setSoTimeout(int timeout) {
    }
    public void shutdownInput() {
    }
    public void shutdownOutput() {
    }
}
