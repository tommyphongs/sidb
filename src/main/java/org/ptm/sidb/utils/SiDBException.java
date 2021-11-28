package org.ptm.sidb.utils;

@SuppressWarnings("serial")
public class SiDBException extends RuntimeException {

	public SiDBException(String message) {
		super(message);
	}

	public SiDBException(Throwable cause) {
		super(cause);
	}

}
