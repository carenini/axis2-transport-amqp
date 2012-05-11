package org.apache.axis2.transport.amqp.common;

import java.io.IOException;

import org.apache.axis2.transport.base.threads.WorkerPool;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Consumer extends DefaultConsumer {
	private WorkerPool wp=null;
	private AMQPEndpoint ep=null;
	
	public Consumer(AMQPEndpoint ep,Channel channel, WorkerPool wp) {
		super(channel);
		this.wp=wp;
		this.ep=ep;
	}

	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
		IncomingMessageHandler handler=null;
		AMQPMessage msg=null;
		long deliveryTag = envelope.getDeliveryTag();
		msg=new AMQPMessage(consumerTag, envelope, properties, body);
		
		handler=new IncomingMessageHandler(ep, msg);
		wp.execute(handler);
		getChannel().basicAck(deliveryTag, false);
	}
}
