package org.ptm.sidb.utils;

import java.io.InputStream;
import java.io.OutputStream;

public interface SiDBStreamCallback {

	void involve(InputStream in, OutputStream out);
}
