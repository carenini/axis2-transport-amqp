package org.apache.axis2.transport.amqp.common;

public class AMQPException extends RuntimeException
{
    public AMQPException(String s, Exception e){
        super(s,e);
    }

    public AMQPException(String s){
        super(s);
    }
}
