package org.ptm.sidb.utils;

import org.ptm.sidb.impl.Streams;

import java.io.IOException;

public class JniSiDB implements Streams.SiDBStream {
	
	static {
		System.loadLibrary("ssdb");
		System.loadLibrary("ssdbjni");
	}
	
	public JniSiDB(String path) {
		int re = _init(path);
		if (re != 0) {
			throw new IllegalArgumentException("re="+re);
		}
	}

	public SiDBResponse request(Runner cmd, byte[]... vals) {
		byte[][] re = _req(cmd.bytes(), vals);
		SiDBResponse resp = new SiDBResponse();
		if (re == null || re.length == 0) {
			resp.stat = "error";
			return resp;
		} else {
			resp.stat = new String(re[0]);
			if (re.length > 1) {
				for (int i = 1; i < re.length; i++) {
					resp.datas.add(re[i]);
				}
			}
		}
		return resp;
	}

	public void callback(SiDBStreamCallback callback) {
		throw new RuntimeException("JNI Not impl callback");
	}

	public void close() throws IOException {
		int re = _close();
		if (re != 0) {
			throw new IOException("re="+re);
		}
	}

	protected native int _init(String cnf);
	protected native byte[][] _req(byte[] cmd, byte[]... vals);
	protected native int _close();
}
