/*
 * Copyright 2004,2005 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.axis2.transport.amqp.out;

import org.apache.axiom.om.OMOutputFormat;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMText;
import org.apache.axiom.om.OMNode;
import org.apache.axiom.om.util.UUIDGenerator;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.transport.TransportUtils;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.OutTransportInfo;
import org.apache.axis2.transport.amqp.common.AMQPConnectionFactory;
import org.apache.axis2.transport.amqp.common.AMQPConnectionFactoryManager;
import org.apache.axis2.transport.amqp.common.AMQPConstants;
import org.apache.axis2.transport.amqp.common.AMQPException;
import org.apache.axis2.transport.amqp.common.AMQPMessage;
import org.apache.axis2.transport.amqp.common.AMQPTransportInfo;
import org.apache.axis2.transport.amqp.common.AMQPUtils;
import org.apache.axis2.transport.base.*;
import org.apache.axis2.transport.base.streams.WriterOutputStream;
import org.apache.axis2.transport.http.HTTPConstants;


import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;

import javax.activation.DataHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;

/**
 * The TransportSender for JMS
 */
public class AMQPSender extends AbstractTransportSender implements ManagementSupport {

	public static final String TRANSPORT_NAME = AMQPConstants.TRANSPORT_AMQP;

	/** The JMS connection factory manager to be used when sending messages out */
	private AMQPConnectionFactoryManager connFacManager;

	/**
	 * Initialize the transport sender by reading pre-defined connection
	 * factories for outgoing messages.
	 * 
	 * @param cfgCtx
	 *            the configuration context
	 * @param transportOut
	 *            the transport sender definition from axis2.xml
	 * @throws AxisFault
	 *             on error
	 */
	@Override
	public void init(ConfigurationContext cfgCtx, TransportOutDescription transportOut) throws AxisFault {
		super.init(cfgCtx, transportOut);
		connFacManager = new AMQPConnectionFactoryManager(transportOut);
		log.info("AMQP Transport Sender initialized...");
	}

	/**
	 * Get corresponding connection factory defined within the transport
	 * sender for the transport-out information - usually constructed from a
	 * targetEPR
	 * 
	 * @param trpInfo
	 *            the transport-out information
	 * @return the corresponding JMS connection factory, if any
	 */
	private AMQPConnectionFactory getAMQPConnectionFactory(AMQPTransportInfo trpInfo) {
		Map<String, String> props = trpInfo.getProperties();
		if (trpInfo.getProperties() != null) {
			String amqpConnectionFactoryName = props.get(AMQPConstants.PARAM_AMQP_CONFAC);
			if (amqpConnectionFactoryName != null) {
				return connFacManager.getAMQPConnectionFactory(amqpConnectionFactoryName);
			} else {
				return connFacManager.getAMQPConnectionFactory(props);
			}
		} else {
			return null;
		}
	}

	/**
	 * Needs a more robust strategy to cache connections and sessions For
	 * efficiency I assume that the reply to exchange,queue and the binding
	 * already exists.
	 * 
	 * For synchrouns request/reponse a temp queue will be create and bound to
	 * the direct exchange.
	 */
	@Override
	public void sendMessage(MessageContext msgCtx, String targetEPR, OutTransportInfo outTransportInfo) throws AxisFault {

		AMQPTransportInfo amqpTransportInfo = null;
		
		// If targetEPR is not null, determine the addressing info from it
		if (targetEPR != null) {
			amqpTransportInfo = new AMQPTransportInfo(targetEPR);
		}
		// If not try to get the addressing info from the transport description
		else if (outTransportInfo != null && outTransportInfo instanceof AMQPTransportInfo) {
			amqpTransportInfo = (AMQPTransportInfo) outTransportInfo;
		}

		if (_connectionDetails.containsKey(amqpTransportInfo.getConnectionURL())) {
			conDetails = _connectionDetails.get(amqpTransportInfo.getConnectionURL());
		} else {
			// else create a new connection
			Connection con = Client.createConnection();
			try {
				con.connect(amqpTransportInfo.getConnectionURL());
			} catch (Exception e) {
				throw new AMQPException("Error creating a connection to the broker", e);
			}
			_connectionDetails.put(amqpTransportInfo.getConnectionURL(), new ConnectionDetails(con));
		}

		if (conDetails != null) {
			session = conDetails.getSession();
		}

		byte[] message = null;
		try {
			message = createMessageData(msgCtx);
		} catch (AMQPException e) {
			handleException("Error creating a message from the axis message context", e);
		}

		// should we wait for a synchronous response on this same thread?
		boolean waitForResponse = waitForSynchronousResponse(msgCtx);
		AMQP.BasicProperties.Builder prop_builder = new AMQP.BasicProperties.Builder();
		AMQP.BasicProperties msgProps = bob.build();
		DeliveryProperties deliveryProps = new DeliveryProperties();
		fillMessageHeaders(msgCtx, amqpTransportInfo, session, waitForResponse, deliveryProps, msgProps);

		synchronized (session) {
			session.header(msgProps, deliveryProps);
			session.data(message);
			session.endData();
		}

		// if we are expecting a synchronous response back for the message sent
		// out
		if (waitForResponse) {
			waitForResponseAndProcess(session, msgProps, msgCtx);
		}
	}

	private void fillMessageHeaders(MessageContext msgCtx, AMQPTransportInfo amqpTransportInfo, boolean waitForResponse, AMQP.BasicProperties msgProps) {
		// Routing info
		deliveryProps.setExchange(amqpTransportInfo.getDestination().getExchangeName());
		deliveryProps.setRoutingKey(amqpTransportInfo.getDestination().getRoutingKey());

		// Content type
		OMOutputFormat format = BaseUtils.getOMOutputFormat(msgCtx);
		MessageFormatter messageFormatter = null;
		try {
			messageFormatter = TransportUtils.getMessageFormatter(msgCtx);
		} catch (AxisFault axisFault) {
			throw new AMQPException("Unable to get the message formatter to use");
		}

		String contentType = messageFormatter.getContentType(msgCtx, format, msgCtx.getSoapAction());
		msgProps=msgProps.builder().contentType(contentType).build();

		// Custom properties - SOAP ACTION
		Map<String, Object> props = new HashMap<String, Object>();

		if (msgCtx.isServerSide()) {
			// set SOAP Action as a property on the message
			props.put(BaseConstants.SOAPACTION, (String) msgCtx.getProperty(BaseConstants.SOAPACTION));

		} else {
			String action = msgCtx.getOptions().getAction();
			if (action != null) {
				props.put(BaseConstants.SOAPACTION, action);
			}
		}

		msgProps=msgProps.builder().headers(props).build();

		// transport headers
		Map headerMap = (Map) msgCtx.getProperty(MessageContext.TRANSPORT_HEADERS);

		if (headerMap != null) {
			Iterator iter = headerMap.keySet().iterator();
			while (iter.hasNext()) {

				String name = (String) iter.next();

				if (AMQPConstants.AMQP_CORRELATION_ID.equals(name)) {
					msgProps=msgProps.builder().correlationId((String) headerMap.get(AMQPConstants.AMQP_CORRELATION_ID)).build();
					// If it's request/response, then we need to fill in
					// correlation id and reply to properties
				} else if (AMQPConstants.AMQP_DELIVERY_MODE.equals(name)) {
					Object o = headerMap.get(AMQPConstants.AMQP_DELIVERY_MODE);
					String val=""+o;
					msgProps=msgProps.builder().deliveryMode(Integer.parseInt(val)).build();
				} else if (AMQPConstants.AMQP_EXPIRATION.equals(name)) {
					msgProps=msgProps.builder().expiration((String) headerMap.get(AMQPConstants.AMQP_EXPIRATION)).build();
				} else if (AMQPConstants.AMQP_MESSAGE_ID.equals(name)) {
					msgProps=msgProps.builder().messageId((String) headerMap.get(AMQPConstants.AMQP_MESSAGE_ID)).build();
				} else if (AMQPConstants.AMQP_PRIORITY.equals(name)) {
					msgProps=msgProps.builder().priority(Integer.parseInt(((String) headerMap.get(AMQPConstants.AMQP_PRIORITY)))).build();
				} else if (AMQPConstants.AMQP_TIMESTAMP.equals(name)) {
					long timestamp=Long.parseLong((String) headerMap.get(AMQPConstants.AMQP_TIMESTAMP));
					msgProps=msgProps.builder().timestamp(new Date(timestamp)).build();
				} else {
					// custom app props
					Object value = headerMap.get(name);
					props.put(name, value);
				}
			}
		}

		/*
		 * For efficiency I assume that the reply to exchange and destination is
		 * already created If the reply is for the same service, then this
		 * should be the queue that the service is listening to. Blindly
		 * creating these exchanges,queues and bindings is sub optimal and can
		 * be avoid if the administrator creates the nessacery exchanges,queues
		 * and bindings before hand.
		 * 
		 * If the service hasn't specify and it's a request/reply MEP then a
		 * temporary queue (which is auto-deleted) is created and bound to the
		 * amq.direct exchange.
		 */
		if (msgCtx.getProperty(AMQPConstants.AMQP_REPLY_TO_EXCHANGE_NAME) != null) {
			String replyExchangeName = (String) msgCtx.getProperty(AMQPConstants.AMQP_REPLY_TO_EXCHANGE_NAME);
			String replyRoutingKey = msgCtx.getProperty(AMQPConstants.AMQP_REPLY_TO_ROUTING_KEY) != null ? (String) msgCtx.getProperty(AMQPConstants.AMQP_REPLY_TO_ROUTING_KEY) : null;

			// for fannout exchange or some other custom exchange, the routing
			// key maybe null
			msgProps=msgProps.builder().replyTo(new ReplyTo(replyExchangeName, replyRoutingKey)).build();
		}

		// If it's request/response, then we need to fill in reply to properties
		// and correlation_id
		if (waitForResponse) {

			if (waitForResponse && msgProps.getCorrelationId() == null) {
				if (msgCtx.getProperty(AMQPConstants.AMQP_CORELATION_ID) != null) {
					msgProps=msgProps.builder().correlationId((String) msgCtx.getProperty(AMQPConstants.AMQP_CORELATION_ID)).build();
				} else {
					msgProps=msgProps.builder().correlationId(UUIDGenerator.getUUID()).build();
				}

			}

			if (msgProps.getReplyTo() == null) {
				// We need to use a temp queue here.
				String tempQueueName = "Queue_" + msgProps.getCorrelationId();
				synchronized (session) {
					session.queueDeclare(tempQueueName, null, null, Option.AUTO_DELETE, Option.EXCLUSIVE);
					session.queueBind(tempQueueName, "amq.direct", tempQueueName, null);
					session.sync();
				}
				msgProps.replyTo(new ReplyTo("amq.direct", tempQueueName));
			}
		}
	}

	private byte[] createMessageData(MessageContext msgContext) {
		OMOutputFormat format = BaseUtils.getOMOutputFormat(msgContext);
		MessageFormatter messageFormatter = null;
		try {
			messageFormatter = TransportUtils.getMessageFormatter(msgContext);
		} catch (AxisFault axisFault) {
			throw new AMQPException("Unable to get the message formatter to use", axisFault);
		}

		String contentType = messageFormatter.getContentType(msgContext, format, msgContext.getSoapAction());

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			messageFormatter.writeTo(msgContext, format, baos, true);
			baos.flush();
			return baos.toByteArray();
		} catch (IOException e) {
			throw new AMQPException("IO Error while creating message", e);
		}
	}

	private void waitForResponseAndProcess(Channel chan, AMQPMessage msg, MessageContext msgCtx) throws AxisFault {
		long timeout = AMQPConstants.DEFAULT_AMQP_TIMEOUT;
		String waitReply = (String) msgCtx.getProperty(AMQPConstants.AMQP_WAIT_REPLY);
		AMQP.BasicProperties msg_props=null;
		Envelope msg_env=null;
		
		msg_props=msg.getProperties();
		msg_env=msg.getEnvelope();
				
		if (waitReply != null) {
			timeout = Long.valueOf(waitReply).longValue();
		}
		// We are using the routing key (which is the queue name) as the
		// destination
		String destination = msg_env.getRoutingKey();
		
		MessageManager listener = new MessageManager(chan, destination, msg_props.getCorrelationId());
		chan.messageSubscribe(msg_env.getRoutingKey(), destination, Session.TRANSFER_CONFIRM_MODE_REQUIRED, Session.TRANSFER_ACQUIRE_MODE_PRE_ACQUIRE, new MessagePartListenerAdapter(listener), null, Option.NO_OPTION);

		AMQPMessage reply = listener.receive(timeout);

		if (reply != null) {
			processSyncResponse(msgCtx, reply);

		} else {
			log.warn("Did not receive a response within " + timeout + " ms to destination : " + msg_props.getReplyTo().getRoutingKey() + " with correlation ID : " + msg_props.getCorrelationId());
		}
	}

	private void processSyncResponse(MessageContext outMsgCtx, AMQPMessage message) throws AxisFault {

		MessageContext responseMsgCtx = createResponseMessageContext(outMsgCtx);

		// load any transport headers from received message
		Map map = AMQPUtils.getTransportHeaders(message);
		responseMsgCtx.setProperty(MessageContext.TRANSPORT_HEADERS, map);

		// workaround for Axis2 TransportUtils.createSOAPMessage() issue, where
		// a response
		// of content type "text/xml" is thought to be REST if
		// !MC.isServerSide(). This
		// question is still under debate and due to the timelines, I am
		// commiting this
		// workaround as Axis2 1.2 is about to be released and  1.0
		responseMsgCtx.setServerSide(false);

		String contentType = AMQPUtils.getProperty(message, BaseConstants.CONTENT_TYPE);

		AMQPUtils.setSOAPEnvelope(message, responseMsgCtx, contentType);
		responseMsgCtx.setServerSide(true);

		handleIncomingMessage(responseMsgCtx, map, (String) map.get(BaseConstants.SOAPACTION), contentType);
	}

}
