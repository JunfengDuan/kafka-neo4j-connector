/**
  * @author marinapopova
  * Mar 01, 2016
 */
package org.kafka.neo4j.exception;

public class KafkaClientRecoverableException extends Exception {

	/**
	 * 
	 */
	public KafkaClientRecoverableException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public KafkaClientRecoverableException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public KafkaClientRecoverableException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public KafkaClientRecoverableException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public KafkaClientRecoverableException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
