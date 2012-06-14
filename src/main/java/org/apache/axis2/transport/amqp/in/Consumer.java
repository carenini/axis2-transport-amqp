package org.apache.axis2.transport.amqp.in;

import java.io.IOException;

import org.apache.axis2.transport.amqp.common.AMQPMessage;
import org.apache.axis2.transport.base.threads.WorkerPool;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Consumer extends DefaultConsumer {
	private WorkerPool wp=null;
	
	public Consumer(Channel channel, WorkerPool wp) {
		super(channel);
		this.wp=wp;
	}

	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
		IncomingMessageHandler handler=null;
		AMQPMessage msg=null;
		long deliveryTag = envelope.getDeliveryTag();
		msg=new AMQPMessage(consumerTag, envelope, properties, body);
		
		handler=new IncomingMessageHandler(msg);
		wp.execute(handler);
		getChannel().basicAck(deliveryTag, false);
	}
}
