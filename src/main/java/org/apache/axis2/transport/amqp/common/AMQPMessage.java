package org.apache.axis2.transport.amqp.common;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;
public class AMQPMessage {
	private String consumerTag=null;
	private Envelope envelope=null;
	private BasicProperties properties=null;
	private byte[] body=null;
	
	
	public AMQPMessage(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) {
		this.consumerTag = consumerTag;
		this.envelope = envelope;
		this.properties = properties;
		this.body = body;
	}
	
	public AMQPMessage() {
		AMQP.BasicProperties.Builder bob = new AMQP.BasicProperties.Builder();
		properties = bob.build();
	}

	public String getConsumerTag() {
		return consumerTag;
	}
	public void setConsumerTag(String consumerTag) {
		this.consumerTag = consumerTag;
	}
	public Envelope getEnvelope() {
		return envelope;
	}
	public void setEnvelope(Envelope envelope) {
		this.envelope = envelope;
	}
	public BasicProperties getProperties() {
		return properties;
	}
	public void setProperties(BasicProperties properties) {
		this.properties = properties;
	}
	public byte[] getBody() {
		return body;
	}
	public void setBody(byte[] body) {
		this.body = body;
	}
}
