package org.ptm.sidb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.ptm.sidb.impl.Clients;
import org.ptm.sidb.impl.Streams;
import org.ptm.sidb.utils.ConnectionPools;
import org.ptm.sidb.utils.*;

public class SiDBUtils {
	
	public static String DEFAULT_HOST = "127.0.0.1";
	public static int DEFAULT_PORT = 5001;
	public static int DEFAULT_TIMEOUT = 5000;

	private SiDBUtils() {

	}
	
	public static Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
	
	public static final byte[] EMPTY_ARG = new byte[0];
	
	public static SiDBResponse.ResponseFactory respFactory = new SiDBResponse.ResponseFactory();

	public static final SiDB simple(String host, int port, int timeout) {
		return new Clients.SiDBClient(host, port, timeout);
	}

	public static final SiDB pool(String host, int port, int timeout, Object config) {
		return pool(host, port, timeout, config, null);
	}
	

    public static final SiDB pool(String host, int port, int timeout, Object config, byte[] auth) {
        return new Clients.SiDBClient(_pool(host, port, timeout, config, auth));
    }
	
	protected static final Streams.PoolSiDBStream _pool(String host, int port, int timeout, Object config, byte[] auth) {
		return ConnectionPools.pool(host, port, timeout, config, auth);
	}

	public static final SiDB replicate(String masterHost, int masterPort, String slaveHost, int slavePort, int timeout, Object config) {
		return replicate(masterHost, masterPort, slaveHost, slavePort, timeout, config, null, null);
	}
	

	public static final SiDB replicate(String masterHost, int masterPort, String slaveHost, int slavePort, int timeout, Object config, byte[] masterAuth, byte[] slaveAuth) {
        Streams.PoolSiDBStream master = _pool(masterHost, masterPort, timeout, config, masterAuth);
        Streams.PoolSiDBStream slave = _pool(slaveHost, slavePort, timeout, config, slaveAuth);
        return new Clients.SiDBClient(new ReplicationSSDMStream(master, slave));
    }

	public static byte[] read(InputStream in) throws IOException {
		int len = 0;
		int d = in.read();
		if (d == '\n')
			return null;
		else if (d >= '0' && d <= '9')
			len = len * 10 + (d - '0');
		else
			throw new SiDBException("protocol error. unexpect byte=" + d);
		while (true) {
			d = in.read();
			if (d >= '0' && d <= '9')
				len = len * 10 + (d - '0');
			else if (d == '\n')
				break;
			else
				throw new SiDBException("protocol error. unexpect byte=" + d);
		}
		byte[] data = new byte[len];
		if (len > 0) {
			int count = 0;
			int r = 0;
			while (count < len) {
				r = in.read(data, count, len - count > 8192 ? 8192 : len - count);
				if (r > 0) {
					count += r;
				} else if (r == -1)
					throw new SiDBException("protocol error. unexpect stream end!");
			}
		}
		d = in.read();
		if (d != '\n')
			throw new SiDBException("protocol error. unexpect byte=" + d);
		return data;
	}
	

	public static void writeBlock(OutputStream out, byte[] data) throws IOException {
		if (data == null)
			data = EMPTY_ARG;
		out.write(Integer.toString(data.length).getBytes());
		out.write('\n');
		out.write(data);
		out.write('\n');
	}

	public static void sendCmd(OutputStream out, Runner cmd, byte[] ... vals) throws IOException {
		SiDBUtils.writeBlock(out, cmd.bytes());
		for (byte[] bs : vals) {
			SiDBUtils.writeBlock(out, bs);
		}
		out.write('\n');
		out.flush();
	}

	public static SiDBResponse readResp(InputStream in) throws IOException {
		SiDBResponse resp = respFactory.create();
		byte[] data = SiDBUtils.read(in);
		if (data == null)
			throw new SiDBException("protocol error. unexpect \\n");
		resp.stat = new String(data);
		while (true) {
			data = SiDBUtils.read(in);
			if (data == null)
				break;
			resp.datas.add(data);
		}
		return resp;
	}

    public static class ReplicationSSDMStream implements Streams.SiDBStream {

        protected Streams.SiDBStream master;

        protected Streams.SiDBStream slave;

        public ReplicationSSDMStream(Streams.SiDBStream master, Streams.SiDBStream slave) {
            this.master = master;
            this.slave = slave;
        }

        public SiDBResponse request(Runner cmd, byte[]... vals) {
            if (cmd.isSlave())
                return slave.request(cmd, vals);
            return master.request(cmd, vals);
        }

        public void callback(SiDBStreamCallback callback) {
            master.callback(callback);
        }

        public void close() throws IOException {
            try {
                master.close();
            } finally {
                slave.close();
            }
        }
    }
}
