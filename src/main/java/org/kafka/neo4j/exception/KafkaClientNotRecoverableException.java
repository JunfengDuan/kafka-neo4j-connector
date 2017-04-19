/**
  * @author marinapopova
  * Mar 01, 2016
 */
package org.kafka.neo4j.exception;

public class KafkaClientNotRecoverableException extends Exception {

	/**
	 * 
	 */
	public KafkaClientNotRecoverableException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public KafkaClientNotRecoverableException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public KafkaClientNotRecoverableException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public KafkaClientNotRecoverableException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public KafkaClientNotRecoverableException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
