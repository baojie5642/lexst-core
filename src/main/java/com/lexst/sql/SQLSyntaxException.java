/**
 *
 */
package com.lexst.sql;


public class SQLSyntaxException extends RuntimeException {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	/**
	 *
	 */
	public SQLSyntaxException() {
		super();
	}

	/**
	 * @param message
	 */
	public SQLSyntaxException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public SQLSyntaxException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public SQLSyntaxException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public SQLSyntaxException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

}
