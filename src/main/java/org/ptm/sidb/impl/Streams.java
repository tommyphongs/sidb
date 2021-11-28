package org.ptm.sidb.impl;

import org.apache.commons.pool.ObjectPool;
import org.ptm.sidb.SiDBUtils;
import org.ptm.sidb.utils.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Streams {
    public abstract static class AbstractIoSiDBStream implements SiDBStream {

        protected InputStream in;

        protected OutputStream out;

        public synchronized SiDBResponse request(Runner cmd, byte[]... vals) {
            beforeExec();
            try {
                SiDBUtils.sendCmd(out, cmd, vals);
                SiDBResponse resp = SiDBUtils.readResp(in);
                beforeReturn(resp);
                return resp;
            } catch (Throwable e) {
                return whenError(e);
            }
        }

        protected void beforeExec() {
        }

        protected void beforeReturn(SiDBResponse resp) {
        }

        protected SiDBResponse whenError(Throwable e) {
            throw new SiDBException(e);
        }

        public void callback(SiDBStreamCallback callback) {
            beforeExec();
            try {
                callback.involve(in, out);
            } catch (Throwable e) {
                whenError(e);
            }
        }

    }

    public static class SocketSiDBStream extends AbstractIoSiDBStream {

        private Socket socket;
        protected String host;
        protected int port;
        protected int timeout;
        protected byte[] auth;

        public SocketSiDBStream(String host, int port, int timeout) {
            this(host, port, timeout, null);
        }

        public SocketSiDBStream(String host, int port, int timeout, byte[] auth) {
            this.socket = new Socket();
            this.host = host;
            this.port = port;
            this.timeout = timeout;
            this.auth = auth;
        }

        protected void beforeExec() {
            if (!socket.isConnected()) {
                try {
                    socket.connect(new InetSocketAddress(host, port), timeout);
                    socket.setSoTimeout(timeout);
                    this.in = new BufferedInputStream(socket.getInputStream());
                    this.out = new BufferedOutputStream(socket.getOutputStream());
                    if (auth != null) {
                        SiDBUtils.sendCmd(out, Runner.auth, auth);
                        if (!SiDBUtils.readResp(in).ok()) {
                            throw new IOException("auth fail");
                        }
                    }
                } catch (IOException e) {
                    throw new SiDBException(e);
                }
            }
        }

        @Override
        protected SiDBResponse whenError(Throwable e) {
            if (!socket.isClosed())
                try {
                    socket.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            return super.whenError(e);
        }

        public void close() throws IOException {
            socket.close();
        }
    }

    public static class PoolSiDBStream implements SiDBStream {

        protected ObjectPool<SiDBStream> pool;

        public PoolSiDBStream(ObjectPool<SiDBStream> pool) {
            this.pool = pool;
        }

        public SiDBResponse request(Runner cmd, byte[]... vals) {
            SiDBStream steam = null;
            try {
                steam = pool.borrowObject();
                SiDBResponse resp = steam.request(cmd, vals);
                pool.returnObject(steam);
                return resp;
            } catch (Exception e) {
                if (steam != null)
                    try {
                        pool.invalidateObject(steam);
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                throw new SiDBException(e);
            }
        }

        public void callback(SiDBStreamCallback callback) {
            try {
                SiDBStream steam = pool.borrowObject();
                try {
                    steam.callback(callback);
                } finally {
                    pool.returnObject(steam);
                }
            } catch (Exception e) {
                throw new SiDBException(e);
            }
        }

        public void close() throws IOException {
            try {
                pool.close();
            } catch (Exception e) {
                if (e instanceof IOException)
                    throw (IOException)e;
                throw new IOException(e);
            }
        }
    }

    public static interface SiDBStream extends Closeable {

        SiDBResponse request(Runner cmd, byte[]... vals);

        void callback(SiDBStreamCallback callback);
    }
}
