package org.mariadb.jdbc.internal.mysql;

import com.sun.jna.*;
import com.sun.jna.platform.win32.BaseTSD.SIZE_T;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;
import com.sun.jna.win32.StdCallLibrary;
import com.sun.jna.win32.W32APIFunctionMapper;
import com.sun.jna.win32.W32APITypeMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SharedMemorySocket extends Socket {

    final static Map<String, Object> WIN32API_OPTIONS = new HashMap<String, Object>() {
        {
            put(Library.OPTION_FUNCTION_MAPPER, W32APIFunctionMapper.UNICODE);
            put(Library.OPTION_TYPE_MAPPER, W32APITypeMapper.UNICODE);
        }
    };
    public interface Kernel32 extends StdCallLibrary {
        public Kernel32 INSTANCE = (Kernel32) Native.loadLibrary("Kernel32", Kernel32.class, WIN32API_OPTIONS);
        public static final int FILE_MAP_WRITE = 0x0002;
        public static final int FILE_MAP_READ = 0x0004;
        public static final int EVENT_MODIFY_STATE = 0x0002;
        public static final int SYNCHRONIZE = 0x00100000;
        public static final int INFINITE = -1;

        public HANDLE OpenEvent(int dwDesiredAccess, boolean bInheritHandle, String name) throws LastErrorException;
        public HANDLE OpenFileMapping(int dwDesiredAccess, boolean bInheritHandle, String name) throws LastErrorException;
        public Pointer MapViewOfFile(HANDLE hFileMappingObject, int dwDesiredAccess, int dwFileOffsetHigh, int dwFileOffsetLow,
                                     SIZE_T dwNumberOfBytesToMap) throws LastErrorException;
        public boolean UnmapViewOfFile(Pointer view) throws LastErrorException;
        public boolean SetEvent(HANDLE handle) throws LastErrorException;
        public boolean CloseHandle(HANDLE handle) throws LastErrorException;
        public int WaitForSingleObject(HANDLE handle, int timeout) throws LastErrorException;
        public int WaitForMultipleObjects(int count, HANDLE[] handles, boolean waitAll, int millis) throws LastErrorException;
        public int GetLastError() throws LastErrorException;
        public HANDLE CreateMutex(Advapi32.SECURITY_ATTRIBUTES sa, boolean initialOwner, String name);
        public boolean ReleaseMutex(HANDLE hMutex);
        public Pointer LocalFree(Pointer p);


    }

    public interface Advapi32 extends StdCallLibrary {
        public Advapi32 INSTANCE = (Advapi32) Native.loadLibrary("advapi32", Advapi32.class,WIN32API_OPTIONS);
        public static class SECURITY_ATTRIBUTES extends Structure {
            public int nLength;
            public Pointer lpSecurityDescriptor;
            public boolean bInheritHandle;

            protected java.util.List getFieldOrder() {
                return Arrays.asList(new String[] {"nLength", "lpSecurityDescriptor", "bInheritHandle"});
            }
        }
        boolean ConvertStringSecurityDescriptorToSecurityDescriptor(String sddl, int sddlVersion, PointerByReference psd,
                IntByReference length);

    }
    //SDDL string for mutex security flags (Everyone group has SYNCHRONIZE right)
    public static final String EVERYONE_SYNCHRONIZE_SDDL = "D:(A;;0x100000;;;WD)";

    // Size of memory mapped region
    static int BUFFERLEN = 16004;

    private String memoryName;
    private HANDLE serverRead;
    private HANDLE serverWrote;
    private HANDLE clientRead;
    private HANDLE clientWrote;
    private HANDLE connectionClosed;
    private Pointer view;
    private int bytesLeft;
    private int position;
    private int connectNumber;
    private int timeout = Kernel32.INFINITE;

    InputStream is;
    OutputStream os;

    class SharedMemoryInputStream extends InputStream {
        @Override
        public int read(byte[] b, int off, int count) throws IOException {
            HANDLE handles[] = {serverWrote, connectionClosed};
            if (bytesLeft == 0) {
                int index = Kernel32.INSTANCE.WaitForMultipleObjects(2, handles, false, timeout);
                if (index == -1) {
                    throw new IOException("wait failed, timeout");
                } else if (index == 1) {
                    // Connection closed
                    throw new IOException("Server closed connection");
                } else if (index != 0) {
                    throw new IOException("Unexpected return result from WaitForMultipleObjects : " + index);
                }
                bytesLeft = view.getInt(0);
                position = 4;
            }
            int len = Math.min(count, bytesLeft);
            view.read(position, b, off, len);
            position += len;
            bytesLeft -= len;
            if (bytesLeft == 0)
                Kernel32.INSTANCE.SetEvent(clientRead);
            return len;
        }

        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            int bytesRead = read(b);
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

    class SharedMemoryOutputStream extends OutputStream {
        @Override
        public void write(byte[] b, int off, int count) throws IOException {
            int bytesToWrite = count;
            int buffPos = off;
            HANDLE handles[] = {serverRead, connectionClosed};
            while (bytesToWrite > 0) {
                int index = Kernel32.INSTANCE.WaitForMultipleObjects(2, handles, false, timeout);
                if (index == -1) {
                    // wait failed,  probably timeout
                    throw new IOException("WaitForMultipleObjects() failed, timeout");
                } else if (index == 1) {
                    // Connection closed
                    throw new IOException("Server closed connection");
                } else if (index != 0) {
                    throw new IOException("Unexpected return result from WaitForMultipleObjects : " + index);
                }

                int chunk = Math.min(bytesToWrite, BUFFERLEN);
                view.setInt(0, chunk);
                view.write(4, b, buffPos, chunk);
                buffPos += chunk;
                bytesToWrite -= chunk;
                if (!Kernel32.INSTANCE.SetEvent(clientWrote))
                    throw new IOException("SetEvent failed");
            }
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[] {(byte) b});
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }
    }

    public SharedMemorySocket(String name) throws IOException {
        if (!Platform.isWindows()) {
            throw new IOException("shared memory connections are only supported on Windows");
        }
        memoryName = name;
    }


    @Override
    public void connect(SocketAddress endpoint) throws IOException {
        connect(endpoint, 0);
    }

    public static HANDLE openEvent(String name) {
        return Kernel32.INSTANCE.OpenEvent(Kernel32.EVENT_MODIFY_STATE | Kernel32.SYNCHRONIZE, false, name);
    }

    public static Pointer mapMemory(String mapName, int mode, int size) {
        HANDLE mapping = Kernel32.INSTANCE.OpenFileMapping(mode, false, mapName);
        Pointer v = Kernel32.INSTANCE.MapViewOfFile(mapping, mode, 0, 0, new SIZE_T(size));
        Kernel32.INSTANCE.CloseHandle(mapping);
        return v;
    }

    /*
    Create a mutex to synchronize login. Without mutex, different connections that are created at about the same
    time, could get the same connection number. Note, that this mutex, or any synchronization does not exist in
    in either C or .NET connectors (i.e they are racy)
    */
    private HANDLE lockMutex() throws IOException {
        PointerByReference securityDescriptor = new PointerByReference();
        Advapi32.INSTANCE.ConvertStringSecurityDescriptorToSecurityDescriptor(EVERYONE_SYNCHRONIZE_SDDL, 1, securityDescriptor,null);
        Advapi32.SECURITY_ATTRIBUTES sa = new Advapi32.SECURITY_ATTRIBUTES();
        sa.nLength = sa.size();
        sa.lpSecurityDescriptor = securityDescriptor.getValue();
        sa.bInheritHandle = false;
        HANDLE mutex = Kernel32.INSTANCE.CreateMutex(sa, false, memoryName + "_CONNECT_MUTEX");
        Kernel32.INSTANCE.LocalFree(securityDescriptor.getValue());
        if (Kernel32.INSTANCE.WaitForSingleObject(mutex, timeout) == -1) {
            Kernel32.INSTANCE.CloseHandle(mutex);
            throw new IOException("wait failed (timeout, last error =  " + Kernel32.INSTANCE.GetLastError());
        }
        return mutex;
    }

    public int getConnectNumber() throws IOException {
        HANDLE connectRequest;
        try {
            connectRequest = openEvent(memoryName + "_CONNECT_REQUEST");
        } catch (LastErrorException lee) {
            try {
                connectRequest = openEvent("Global\\" + memoryName + "_CONNECT_REQUEST");
                memoryName = "Global\\" + memoryName;
            } catch (LastErrorException lee2) {
                throw new IOException("getConnectNumber() fails : " + lee2.getMessage() + " " + memoryName);
            }
        }

        HANDLE connectAnswer = openEvent(memoryName + "_CONNECT_ANSWER");


        HANDLE mutex = lockMutex();
        Pointer connectData = null;
        try {
            Kernel32.INSTANCE.SetEvent(connectRequest);
            connectData = mapMemory(memoryName + "_CONNECT_DATA", Kernel32.FILE_MAP_READ, 4);
            int ret = Kernel32.INSTANCE.WaitForSingleObject(connectAnswer, timeout);
            if (ret != 0) {
                throw new IOException("WaitForSingleObject returned " + ret + ", last error " + Kernel32.INSTANCE.GetLastError());
            }
            int nr = connectData.getInt(0);
            return nr;
        } finally {
            Kernel32.INSTANCE.ReleaseMutex(mutex);
            Kernel32.INSTANCE.CloseHandle(mutex);
            if (connectData != null)  {
                Kernel32.INSTANCE.UnmapViewOfFile(connectData);
            }
            Kernel32.INSTANCE.CloseHandle(connectRequest);
            Kernel32.INSTANCE.CloseHandle(connectAnswer);
        }
    }


    public void connect(SocketAddress endpoint, int timeout) throws IOException {
        try {
            is = new SharedMemoryInputStream();
            os = new SharedMemoryOutputStream();
            connectNumber = getConnectNumber();
            String prefix = memoryName + "_" + connectNumber;
            clientRead = openEvent(prefix + "_CLIENT_READ");
            serverRead = openEvent(prefix + "_SERVER_READ");
            serverWrote = openEvent(prefix + "_SERVER_WROTE");
            clientWrote = openEvent(prefix + "_CLIENT_WROTE");
            connectionClosed = openEvent(prefix + "_CONNECTION_CLOSED");
            view = mapMemory(prefix + "_DATA", Kernel32.FILE_MAP_WRITE, BUFFERLEN);
            Kernel32.INSTANCE.SetEvent(serverRead);
        } catch (LastErrorException lee) {
            throw new IOException(lee.getMessage(), lee.getCause());
        }
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

    public void setSoTimeout(int t) {
        if (t == 0) {
            timeout = Kernel32.INFINITE;
        } else {
            timeout = t;
        }
    }

    public void shutdownInput() {
    }

    public void shutdownOutput() {
    }

    @Override
    public void close() {

        if (connectionClosed != null) {
            /* Set close event if not yet set */
            if (Kernel32.INSTANCE.WaitForSingleObject(connectionClosed, 0) != 0) {
                Kernel32.INSTANCE.SetEvent(connectionClosed);
            }
        }
        HANDLE[] handles = {serverRead, serverWrote, clientRead, clientWrote, connectionClosed};
        for (HANDLE h : handles) {
            if (h != null) {
                Kernel32.INSTANCE.CloseHandle(h);
            }
        }
        if (view != null) {
            Kernel32.INSTANCE.UnmapViewOfFile(view);
        }
        serverRead = null;
        serverWrote = null;
        clientRead = null;
        clientWrote = null;
        connectionClosed = null;
        view = null;
    }
}
