package org.apache.axis2.transport.amqp.common;

public class AMQPException extends RuntimeException
{
    /**
	 * 
	 */
	private static final long serialVersionUID = 3905947857016574289L;

	public AMQPException(String s, Exception e){
        super(s,e);
    }

    public AMQPException(String s){
        super(s);
    }
}
