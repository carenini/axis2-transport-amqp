package org.apache.axis2.transport.amqp.common;

import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.amqp.in.AMQPListener;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.MetricsCollector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;

public class IncomingMessageHandler implements Runnable {
    private static final Log log = LogFactory.getLog(IncomingMessageHandler.class);

	private AMQPEndpoint endpoint=null;
	private String consumerTag=null; 
	private Envelope envelope=null;
	private BasicProperties properties=null; 
	private byte[] body=null;
    private MetricsCollector metrics = null;
    private AMQPMessage message=null;
    
	public IncomingMessageHandler(AMQPEndpoint ep, AMQPMessage msg)  {
		// TODO Auto-generated constructor stub
		this.body = body;
		this.consumerTag=msg.getConsumerTag();
		this.envelope=msg.getEnvelope();
		this.endpoint=ep;
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
				String replyDestinationAddress = endpoint.getReplyDestinationAddress();
				if (replyDestinationAddress != null) {
					reply_to = replyDestinationAddress;
				}

			}
			if (reply_to != null) {
				msgContext.setProperty(Constants.OUT_TRANSPORT_INFO, new AMQPTransportInfo(cf, new Destination(reply_to), contentTypeInfo));
			}

			AMQPUtils.setSOAPEnvelope(message, msgContext, contentTypeInfo);
			// FIXME add transactions!
			/*
        if (ut != null) {
            msgContext.setProperty(BaseConstants.USER_TRANSACTION, ut);
        }
			 */

			listener.handleIncomingMessage(msgContext, AMQPUtils.getTransportHeaders(message), soapAction, contentTypeInfo);

		} catch (AxisFault e) {
			e.printStackTrace();
		} finally {

			Object o = msgContext.getProperty(BaseConstants.SET_ROLLBACK_ONLY);
			if (o != null) {
				if ((o instanceof Boolean && ((Boolean) o)) ||
						(o instanceof String && Boolean.valueOf((String) o))) {
					throw new RollbackRequestException();
				}
			}
		}
	}
}
