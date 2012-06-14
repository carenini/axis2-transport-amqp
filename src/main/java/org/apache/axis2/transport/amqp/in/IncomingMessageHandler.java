package org.apache.axis2.transport.amqp.in;

import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.amqp.common.AMQPConnectionFactory;
import org.apache.axis2.transport.amqp.common.AMQPConstants;
import org.apache.axis2.transport.amqp.common.AMQPMessage;
import org.apache.axis2.transport.amqp.common.AMQPTransportInfo;
import org.apache.axis2.transport.amqp.common.AMQPUtils;
import org.apache.axis2.transport.amqp.common.Destination;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.MetricsCollector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.rabbitmq.client.AMQP.BasicProperties;

public class IncomingMessageHandler implements Runnable {
    private static final Log log = LogFactory.getLog(IncomingMessageHandler.class);

	private AMQPEndpoint endpoint=null;
	private BasicProperties properties=null; 
    private MetricsCollector metrics = null;
    private AMQPMessage message=null;
    
	public IncomingMessageHandler(AMQPMessage msg)  {
		this.properties=msg.getProperties();
		this.message=msg;
	}

	@Override
	public void run() {
		AMQPConnectionFactory cf=null;
    	AMQPListener listener=null;
		String msg_id=null; 
		String reply_to=null;
		MessageContext msgContext =null;
		Destination d=null;
		
		try {

			cf=endpoint.getConnectionFactory();
			listener= endpoint.getAmqpListener();
			metrics = listener.getMetricsCollector();
			msg_id=properties.getMessageId();
			reply_to=properties.getReplyTo();

			// update transport level metrics
			metrics.incrementBytesReceived(properties.getBodySize());

			msgContext = endpoint.createMessageContext();
			// set the Message ID as the Message ID of the MessageContext
			msgContext.setMessageID(msg_id);
			msgContext.setProperty(AMQPConstants.AMQP_CORRELATION_ID, msg_id);

			String soapAction = AMQPUtils.getProperty(message, BaseConstants.SOAPACTION);

			String contentTypeInfo =message.getProperties().getContentType();
			if (contentTypeInfo == null) {
				throw new AxisFault("Unable to determine content type for message " + msgContext.getMessageID());
			}

			// set the message property OUT_TRANSPORT_INFO
			// the reply is assumed to be over the JMSReplyTo destination, using
			// the same incoming connection factory, if a JMSReplyTo is available

			if (reply_to == null) {
				log.debug("Messsage");
				// does the service specify a default reply destination ?
				d = endpoint.getSource();
				if (d != null)
					reply_to = d.toAddress();
			}
			
			if (reply_to != null) {
				msgContext.setProperty(Constants.OUT_TRANSPORT_INFO, new AMQPTransportInfo(cf, new Destination(reply_to), contentTypeInfo));
			}

			AMQPUtils.setSOAPEnvelope(message, msgContext, contentTypeInfo);
			
			listener.handleIncomingMessage(msgContext, AMQPUtils.getTransportHeaders(message), soapAction, contentTypeInfo);

		} catch (AxisFault e) {
			e.printStackTrace();
		} 
	}
}
