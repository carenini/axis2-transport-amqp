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
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.transport.TransportUtils;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.OutTransportInfo;
import org.apache.axis2.transport.base.*;
import org.apache.axis2.transport.base.streams.WriterOutputStream;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.axis2.transport.amqp.common.AMQPConnectionFactory;
import org.apache.axis2.transport.amqp.common.AMQPConnectionFactoryManager;
import org.apache.axis2.transport.amqp.common.AMQPConstants;
import org.apache.axis2.transport.amqp.common.AMQPException;
import org.apache.axis2.transport.amqp.common.AMQPMessage;
import org.apache.axis2.transport.amqp.common.AMQPTransportInfo;
import org.apache.axis2.transport.amqp.common.AMQPUtils;
import org.apache.axis2.transport.amqp.common.Destination;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import javax.activation.DataHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;

/**
 * The TransportSender for AMQP
 */
public class AMQPSender extends AbstractTransportSender implements ManagementSupport {

	public static final String TRANSPORT_NAME = AMQPConstants.TRANSPORT_AMQP;

	/**
	 * The AMQP connection factory manager to be used when sending messages out
	 * 
	 * @uml.property name="connFacManager"
	 * @uml.associationEnd readOnly="true"
	 */
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
	 * Get corresponding AMQP connection factory defined within the transport
	 * sender for the transport-out information - usually constructed from a
	 * targetEPR
	 * 
	 * @param trpInfo
	 *            the transport-out information
	 * @return the corresponding AMQP connection factory, if any
	 */
	private AMQPConnectionFactory getAMQPConnectionFactory(AMQPTransportInfo trpInfo) {
		Map<String, String> props = trpInfo.getProperties();
		if (trpInfo.getProperties() != null) {
			String jmsConnectionFactoryName = props.get(AMQPConstants.PARAM_AMQP_CONFAC);
			if (jmsConnectionFactoryName != null) {
				return connFacManager.getAMQPConnectionFactory(jmsConnectionFactoryName);
			} else {
				return connFacManager.getAMQPConnectionFactory(props);
			}
		} else {
			return null;
		}
	}

	/**
	 * Performs the actual sending of the AMQP message
	 */
	@Override
	public void sendMessage(MessageContext msgCtx, String targetAddress, OutTransportInfo outTransportInfo) throws AxisFault {

		AMQPConnectionFactory conFac = null;
		AMQPTransportInfo amqpOut = null;
		AMQPMessageSender messageSender = null;

		if (targetAddress != null) {
			amqpOut = new AMQPTransportInfo(targetAddress);
			// do we have a definition for a connection factory to use for this
			// address?
			conFac = getAMQPConnectionFactory(amqpOut);

			if (conFac != null) {
				messageSender = new AMQPMessageSender(conFac, targetAddress);

			} else {
					messageSender = amqpOut.createAMQPSender();
			}

		} else if (outTransportInfo != null && outTransportInfo instanceof AMQPTransportInfo) {

			amqpOut = (AMQPTransportInfo) outTransportInfo;
				messageSender = amqpOut.createAMQPSender();
		}

		// The message property to be used to send the content type is
		// determined by
		// the out transport info, i.e. either from the EPR if we are sending a
		// request,
		// or, if we are sending a response, from the configuration of the
		// service that
		// received the request). The property name can be overridden by a
		// message
		// context property.
		String contentTypeProperty = (String) msgCtx.getProperty(AMQPConstants.CONTENT_TYPE_PROPERTY_PARAM);
		if (contentTypeProperty == null) {
			contentTypeProperty = amqpOut.getContentTypeProperty();
		}

		// need to synchronize as Sessions are not thread safe
		synchronized (messageSender.getSession()) {
			try {
				sendOverAMQP(msgCtx, messageSender, contentTypeProperty, jmsConnectionFactory, amqpOut);
			} finally {
				messageSender.close();
			}
		}
	}

	/**
	 * Perform actual sending of the AMQP message
	 */
	private void sendOverAMQP(MessageContext msgCtx, AMQPMessageSender messageSender, String contentTypeProperty, AMQPConnectionFactory amqpConnectionFactory, AMQPTransportInfo amqpOut) throws AxisFault {

		// convert the axis message context into a AMQP Message that we can send
		// over AMQP
		AMQPMessage message = null;
		AMQP.BasicProperties msg_prop=null;
		Envelope msg_env=null;
		String correlationId = null;
		// should we wait for a synchronous response on this same thread?
		boolean waitForResponse = waitForSynchronousResponse(msgCtx);

		message = createMessage(msgCtx, contentTypeProperty);
		msg_env=message.getEnvelope();
		msg_prop=message.getProperties();
		Destination replyDestination = amqpOut.getReplyDestination();

		// if this is a synchronous out-in, prepare to listen on the response
		// destination
		if (waitForResponse) {

			String replyDestName = (String) msgCtx.getProperty(AMQPConstants.AMQP_REPLY_TO);
			if (replyDestName == null && amqpConnectionFactory != null) {
				replyDestName = amqpConnectionFactory.getReplyToDestination();
			}

			String replyDestType = (String) msgCtx.getProperty(AMQPConstants.AMQP_REPLY_TO_TYPE);
			if (replyDestType == null && amqpConnectionFactory != null) {
				replyDestType = amqpConnectionFactory.getReplyDestinationType();
			}

			if (replyDestName != null) {
				if (amqpConnectionFactory != null) {
					replyDestination = amqpConnectionFactory.getDestination(replyDestName, replyDestType);
				} else {
					replyDestination = amqpOut.getReplyDestination(replyDestName);
				}
			}
			replyDestination = AMQPUtils.setReplyDestination(replyDestination, messageSender.getSession(), message);
		}

		messageSender.send(message, msgCtx);
		metrics.incrementMessagesSent(msgCtx);

		metrics.incrementBytesSent(msgCtx, message.getProperties().getBodySize());

		// if we are expecting a synchronous response back for the message sent
		// out
		if (waitForResponse) {
			// TODO
			// ********************************************************************************
			// TODO **** replace with asynchronous polling via a poller task to
			// process this *******
			// information would be given. Then it should poll (until timeout)
			// the
			// requested destination for the response message and inject it from
			// a
			// asynchronous worker thread
			
			messageSender.getConnection().start(); // multiple calls are safely ignored
			correlationId = msg_prop.getMessageId();

			// We assume here that the response uses the same message property to specify the content type of the message.
			waitForResponseAndProcess(messageSender.getSession(), replyDestination, msgCtx, correlationId, contentTypeProperty);
			// TODO
			// ********************************************************************************
		}
	}

	/**
	 * Create a Consumer for the reply destination and wait for the response AMQP
	 * message synchronously. If a message arrives within the specified time
	 * interval, process it through Axis2
	 * 
	 * @param chan
	 *            the session to use to listen for the response
	 * @param replyDestination
	 *            the AMQP reply Destination
	 * @param msgCtx
	 *            the outgoing message for which we are expecting the response
	 * @param contentTypeProperty
	 *            the message property used to determine the content type of the
	 *            response message
	 * @throws AxisFault
	 *             on error
	 */
	private void waitForResponseAndProcess(Channel chan, Destination replyDestination, MessageContext msgCtx, String correlationId, String contentTypeProperty) throws AxisFault {

		//try {
			MessageConsumer consumer;
			consumer = AMQPUtils.createConsumer(chan, replyDestination, "AMQPCorrelationID = '" + correlationId + "'");

			// how long are we willing to wait for the sync response
			long timeout = AMQPConstants.DEFAULT_AMQP_TIMEOUT;
			String waitReply = (String) msgCtx.getProperty(AMQPConstants.AMQP_WAIT_REPLY);
			if (waitReply != null) {
				timeout = Long.valueOf(waitReply).longValue();
			}

			if (log.isDebugEnabled()) {
				log.debug("Waiting for a maximum of " + timeout + "ms for a response message to destination : " + replyDestination + " with AMQP correlation ID : " + correlationId);
			}

			AMQPMessage reply = consumer.receive(timeout);

			if (reply != null) {
				// update transport level metrics
				metrics.incrementMessagesReceived();
				metrics.incrementBytesReceived(reply.getProperties().getBodySize());

				try {
					processSyncResponse(msgCtx, reply, contentTypeProperty);
					metrics.incrementMessagesReceived();
				} catch (AxisFault e) {
					metrics.incrementFaultsReceiving();
					throw e;
				}

			} else {
				log.warn("Did not receive a AMQP response within " + timeout + " ms to destination : " + replyDestination + " with AMQP correlation ID : " + correlationId);
				metrics.incrementTimeoutsReceiving();
			}

		/*} catch (AMQPException e) {
			metrics.incrementFaultsReceiving();
			handleException("Error creating a consumer, or receiving a synchronous reply " + "for outgoing MessageContext ID : " + msgCtx.getMessageID() + " and reply Destination : " + replyDestination, e);
		}*/
	}

	/**
	 * Create a AMQP Message from the given MessageContext and using the given
	 * session
	 * 
	 * @param msgContext
	 *            the MessageContext
	 * @param chan
	 *            the AMQP session
	 * @param contentTypeProperty
	 *            the message property to be used to store the content type
	 * @return a AMQP message from the context and session
	 * @throws AMQPException
	 *             on exception
	 * @throws AxisFault
	 *             on exception
	 */
	private AMQPMessage createMessage(MessageContext msgContext, String contentTypeProperty) throws AMQPException, AxisFault {

		AMQPMessage message = null;
		Envelope msg_env=null;
		AMQP.BasicProperties msg_prop=null;
		ByteArrayOutputStream bos=null;
		OutputStream out=null;
		byte[] msg_payload=null;
		String contentType = null;
		String payloadType = null;
		Map<String, Object> headers = null;

		StringWriter sw=null;
		String msgType = getProperty(msgContext, AMQPConstants.AMQP_MESSAGE_TYPE);
		
		message=new AMQPMessage();
		// check the first element of the SOAP body, do we have content wrapped
		// using the
		// default wrapper elements for binary
		// (BaseConstants.DEFAULT_BINARY_WRAPPER) or
		// text (BaseConstants.DEFAULT_TEXT_WRAPPER) ? If so, do not create SOAP
		// messages
		// for AMQP but just get the payload in its native format
		 
		payloadType=guessMessageType(msgContext);
		bos=new ByteArrayOutputStream();
		
		if (payloadType == null) {

			OMOutputFormat format = BaseUtils.getOMOutputFormat(msgContext);
			MessageFormatter messageFormatter = null;
			try {
				messageFormatter = TransportUtils.getMessageFormatter(msgContext);
			} catch (AxisFault axisFault) {
				throw new AMQPException("Unable to get the message formatter to use");
			}

			contentType=messageFormatter.getContentType(msgContext, format, msgContext.getSoapAction());

			boolean useBytesMessage = msgType != null && AMQPConstants.AMQP_BYTE_MESSAGE.equals(msgType) || contentType.indexOf(HTTPConstants.HEADER_ACCEPT_MULTIPART_RELATED) > -1;

			if (useBytesMessage) {
				sw = null;
				out = bos;
			} else {
				sw = new StringWriter();
				try {
					out = new WriterOutputStream(sw, format.getCharSetEncoding());
				} catch (UnsupportedCharsetException ex) {
					handleException("Unsupported encoding " + format.getCharSetEncoding(), ex);
					return null;
				}
			}

			try {
				messageFormatter.writeTo(msgContext, format, out, true);
				out.close();
			} catch (IOException e) {
				handleException("IO Error while creating BytesMessage", e);
			}

			if (!useBytesMessage) message.setBody(sw.toString().getBytes());
			if (contentTypeProperty != null) {
				headers=msg_prop.getHeaders();
				if (headers==null) headers=new HashMap<String, Object>();
				headers.put(contentTypeProperty, contentType);
				msg_prop=msg_prop.builder().headers(headers).build();
			}
			

		} else if (AMQPConstants.AMQP_BYTE_MESSAGE.equals(payloadType)) {
			OMElement wrapper = msgContext.getEnvelope().getBody().getFirstChildWithName(BaseConstants.DEFAULT_BINARY_WRAPPER);
			OMNode omNode = wrapper.getFirstOMChild();
			if (omNode != null && omNode instanceof OMText) {
				Object dh = ((OMText) omNode).getDataHandler();
				if (dh != null && dh instanceof DataHandler) {
					try {
						((DataHandler) dh).writeTo(bos);
						message.setBody(bos.toByteArray());
					} catch (IOException e) {
						handleException("Error serializing binary content of element : " + BaseConstants.DEFAULT_BINARY_WRAPPER, e);
					}
				}
			}
		} else if (AMQPConstants.AMQP_TEXT_MESSAGE.equals(payloadType)) {
			message.setBody(msgContext.getEnvelope().getBody().getFirstChildWithName(BaseConstants.DEFAULT_TEXT_WRAPPER).getText().getBytes());
		}

		// set the AMQP correlation ID if specified
		String correlationId = getProperty(msgContext, AMQPConstants.AMQP_CORRELATION_ID);
		if (correlationId == null && msgContext.getRelatesTo() != null) {
			correlationId = msgContext.getRelatesTo().getValue();
		}

		msg_prop=message.getProperties();
		if (correlationId != null) {
			msg_prop=msg_prop.builder().correlationId(correlationId).build();
		}

		if (msgContext.isServerSide()) {
			// set SOAP Action as a property on the AMQP message
			setProperty(message, msgContext, BaseConstants.SOAPACTION);
		} else {
			String action = msgContext.getOptions().getAction();
			if (action != null) {
				headers=msg_prop.getHeaders();
				if (headers==null) headers=new HashMap<String, Object>();
				headers.put(BaseConstants.SOAPACTION, action);
				msg_prop=msg_prop.builder().headers(headers).build();
			}
		}

		AMQPUtils.setTransportHeaders(msgContext, message);
		return message;
	}

	/**
	 * Guess the message type to use for AMQP looking at the message contexts'
	 * envelope
	 * 
	 * @param msgContext
	 *            the message context
	 * @return AMQPConstants.AMQP_BYTE_MESSAGE or AMQPConstants.AMQP_TEXT_MESSAGE
	 *         or null
	 */
	private String guessMessageType(MessageContext msgContext) {
		OMElement firstChild = msgContext.getEnvelope().getBody().getFirstElement();
		if (firstChild != null) {
			if (BaseConstants.DEFAULT_BINARY_WRAPPER.equals(firstChild.getQName())) {
				return AMQPConstants.AMQP_BYTE_MESSAGE;
			} else if (BaseConstants.DEFAULT_TEXT_WRAPPER.equals(firstChild.getQName())) {
				return AMQPConstants.AMQP_TEXT_MESSAGE;
			}
		}
		return null;
	}

	/**
	 * Creates an Axis MessageContext for the received AMQP message and sets up
	 * the transports and various properties
	 * 
	 * @param outMsgCtx
	 *            the outgoing message for which we are expecting the response
	 * @param message
	 *            the AMQP response message received
	 * @param contentTypeProperty
	 *            the message property used to determine the content type of the
	 *            response message
	 * @throws AxisFault
	 *             on error
	 */
	private void processSyncResponse(MessageContext outMsgCtx, AMQPMessage message, String contentTypeProperty) throws AxisFault {

		MessageContext responseMsgCtx = createResponseMessageContext(outMsgCtx);

		// load any transport headers from received message
		AMQPUtils.loadTransportHeaders(message, responseMsgCtx);

		String contentType = contentTypeProperty == null ? null : AMQPUtils.getProperty(message, contentTypeProperty);

		AMQPUtils.setSOAPEnvelope(message, responseMsgCtx, contentType);
		handleIncomingMessage(responseMsgCtx, AMQPUtils.getTransportHeaders(message), AMQPUtils.getProperty(message, BaseConstants.SOAPACTION), contentType);
	}

	private void setProperty(AMQPMessage message, MessageContext msgCtx, String key) {
		AMQP.BasicProperties msg_props=null;
		Map<String, Object> headers = null;
		String value = getProperty(msgCtx, key);
		
		msg_props=message.getProperties();		 
		headers=msg_props.getHeaders();
		if (value != null) {
			headers.put(key, value);
			msg_props=msg_props.builder().headers(headers).build();
		}
	}

	private String getProperty(MessageContext mc, String key) {
		return (String) mc.getProperty(key);
	}
}
