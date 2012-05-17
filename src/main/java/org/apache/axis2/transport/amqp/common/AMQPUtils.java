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
package org.apache.axis2.transport.amqp.common;

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.format.DataSourceMessageBuilder;
import org.apache.axis2.format.TextMessageBuilder;
import org.apache.axis2.format.TextMessageBuilderAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.axis2.transport.TransportUtils;
import org.apache.axis2.transport.amqp.common.iowrappers.BytesMessageDataSource;
import org.apache.axis2.transport.amqp.common.iowrappers.BytesMessageInputStream;
import org.apache.axis2.transport.amqp.in.AMQPListener;
import org.apache.axis2.transport.base.BaseUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Envelope;

import javax.mail.internet.ContentType;
import javax.mail.internet.ParseException;

import java.lang.reflect.Method;
import java.util.*;

/**
 * Miscallaneous methods used for the AMQP transport
 */
public class AMQPUtils extends BaseUtils {

	private static final Log log = LogFactory.getLog(AMQPUtils.class);
	private static final Class<?>[] NOARGS = new Class<?>[] {};
	private static final Object[] NOPARMS = new Object[] {};

	/**
	 * Should this service be enabled over the AMQP transport?
	 * 
	 * @param service
	 *            the Axis service
	 * @return true if AMQP should be enabled
	 */
	public static boolean isAMQPService(AxisService service) {
		if (service.isEnableAllTransports()) {
			return true;

		} else {
			for (String transport : service.getExposedTransports()) {
				if (AMQPListener.TRANSPORT_NAME.equals(transport)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Get a String property from the AMQP message
	 * 
	 * @param message
	 *            AMQP message
	 * @param property
	 *            property name
	 * @return property value
	 */
	public static String getProperty(AMQPMessage message, String property) {
		try {
			return (String)(message.getProperties().getHeaders().get(property));
		} catch (AMQPException e) {
			return null;
		}
	}

	/**
	 * Return the destination name from the given URL
	 * 
	 * @param url
	 *            the URL
	 * @return the destination name
	 */
	public static String getDestination(String url) {
		String tempUrl = url.substring(AMQPConstants.AMQP_PREFIX.length());
		int propPos = tempUrl.indexOf("?");

		if (propPos == -1) {
			return tempUrl;
		} else {
			return tempUrl.substring(0, propPos);
		}
	}

	/**
	 * Set the SOAPEnvelope to the Axis2 MessageContext, from the AMQP Message
	 * passed in
	 * 
	 * @param message
	 *            the AMQP message read
	 * @param msgContext
	 *            the Axis2 MessageContext to be populated
	 * @param contentType
	 *            content type for the message
	 * @throws AxisFault
	 * @throws AMQPException
	 */
	public static void setSOAPEnvelope(AMQPMessage message, MessageContext msgContext, String contentType) throws AxisFault, AMQPException {
		BasicProperties msg_prop=message.getProperties();
		Envelope env=message.getEnvelope();
		
		if (contentType == null) {
			contentType=msg_prop.getContentType();
			log.debug("No content type specified; assuming " + contentType);

		}

		int index = contentType.indexOf(';');
		String type = index > 0 ? contentType.substring(0, index) : contentType;
		Builder builder = BuilderUtil.getBuilderFromSelector(type, msgContext);
		if (builder == null) {
			if (log.isDebugEnabled()) {
				log.debug("No message builder found for type '" + type + "'. Falling back to SOAP.");
			}
			builder = new SOAPBuilder();
		}

		OMElement documentElement;
		if (contentType.equals("application/octet-stream")) {
			// Extract the charset encoding from the content type and
			// set the CHARACTER_SET_ENCODING property as e.g. SOAPBuilder
			// relies on this.
			String charSetEnc = null;
			try {
				if (contentType != null) {
					charSetEnc = new ContentType(contentType).getParameter("charset");
				}
			} catch (ParseException ex) {
				// ignore
			}
			msgContext.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, charSetEnc);

			if (builder instanceof DataSourceMessageBuilder) {
				documentElement = ((DataSourceMessageBuilder) builder).processDocument(new BytesMessageDataSource(message), contentType, msgContext);
			} else {
				documentElement = builder.processDocument(new BytesMessageInputStream(message), contentType, msgContext);
			}
		} else if (contentType.equals("text/plain")) {
			TextMessageBuilder textMessageBuilder;
			if (builder instanceof TextMessageBuilder) {
				textMessageBuilder = (TextMessageBuilder) builder;
			} else {
				textMessageBuilder = new TextMessageBuilderAdapter(builder);
			}
			String content = new String(message.getBody());
			documentElement = textMessageBuilder.processDocument(content, contentType, msgContext);
		} else {
			handleException("Unsupported AMQP message type " + message.getClass().getName());
			return; // Make compiler happy
		}
		msgContext.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));
	}

	/**
	 * Set transport headers from the axis message context, into the AMQP message
	 * 
	 * @param msgContext
	 *            the axis message context
	 * @param message
	 *            the AMQP Message
	 * @throws AMQPException
	 *             on exception
	 */
	public static void setTransportHeaders(MessageContext msgContext, AMQPMessage message) throws AMQPException {
		String correlation_id=null;
		Integer delivery_mode=null;
		long expiration=0;
		String message_id=null;
		int priority=0;
		long timestamp=0;
		String msg_type=null;
		AMQP.BasicProperties.Builder header_builder=null;
		AMQP.BasicProperties msg_prop=null;
		Map<String,Object> headers=new HashMap<String,Object>();
		
		msg_prop=message.getProperties();
		if (msg_prop==null) {
			msg_prop=new AMQP.BasicProperties.Builder().build();
		}
		Envelope msg_env=message.getEnvelope();
		
		Map<?, ?> headerMap = (Map<?, ?>) msgContext.getProperty(MessageContext.TRANSPORT_HEADERS);

		if (headerMap == null) {
			return;
		}

		for (Object headerName : headerMap.keySet()) {
			String name = (String) headerName;

			if (name.startsWith(AMQPConstants.JMSX_PREFIX) && !(name.equals(AMQPConstants.JMSX_GROUP_ID) || name.equals(AMQPConstants.JMSX_GROUP_SEQ))) {
				continue;
			}
			if (AMQPConstants.AMQP_CORRELATION_ID.equals(name)) {
				correlation_id=(String) headerMap.get(AMQPConstants.AMQP_CORRELATION_ID);
				msg_prop=msg_prop.builder().correlationId(correlation_id).build();
			} else if (AMQPConstants.AMQP_DELIVERY_MODE.equals(name)) {
				Object o = headerMap.get(AMQPConstants.AMQP_DELIVERY_MODE);
				if (o instanceof Integer) {
					delivery_mode=Integer.parseInt((String) o);
					msg_prop=msg_prop.builder().deliveryMode(delivery_mode).build();
				} else {
					log.warn("Invalid delivery mode ignored : " + o);
				}
			} else if (AMQPConstants.AMQP_EXPIRATION.equals(name)) {
				expiration=Long.parseLong((String) headerMap.get(AMQPConstants.AMQP_EXPIRATION));
				msg_prop=msg_prop.builder().deliveryMode(delivery_mode).build();
			} else if (AMQPConstants.AMQP_MESSAGE_ID.equals(name)) {
				message_id=(String) headerMap.get(AMQPConstants.AMQP_MESSAGE_ID);
				msg_prop=msg_prop.builder().messageId(message_id).build();
			} else if (AMQPConstants.AMQP_PRIORITY.equals(name)) {
				priority=Integer.parseInt((String) headerMap.get(AMQPConstants.AMQP_PRIORITY));
				msg_prop=msg_prop.builder().priority(priority).build();
			} else if (AMQPConstants.AMQP_TIMESTAMP.equals(name)) {
				timestamp=Long.parseLong((String) headerMap.get(AMQPConstants.AMQP_TIMESTAMP));
				//FIXME
				msg_prop=msg_prop.builder().timestamp(new Date(timestamp)).build();
			} else if (AMQPConstants.AMQP_MESSAGE_TYPE.equals(name)) {
				msg_type=(String) headerMap.get(AMQPConstants.AMQP_MESSAGE_TYPE);
				msg_prop=msg_prop.builder().type(msg_type).build();

			} else {
				Object value = headerMap.get(name);
				headers.put(name, value);
			}
			msg_prop=msg_prop.builder().headers(headers).build();
		}
	}

	/**
	 * Read the transport headers from the AMQP Message and set them to the axis2
	 * message context
	 * 
	 * @param message
	 *            the AMQP Message received
	 * @param responseMsgCtx
	 *            the axis message context
	 * @throws AxisFault
	 *             on error
	 */
	public static void loadTransportHeaders(AMQPMessage message, MessageContext responseMsgCtx) throws AxisFault {
		responseMsgCtx.setProperty(MessageContext.TRANSPORT_HEADERS, getTransportHeaders(message));
	}

	/**
	 * Extract transport level headers for AMQP from the given message into a Map
	 * 
	 * @param message
	 *            the AMQP message
	 * @return a Map of the transport headers
	 */
	public static Map<String, Object> getTransportHeaders(AMQPMessage message) {
		// create a Map to hold transport headers
		Map<String, Object> map = new HashMap<String, Object>();
		Map<String, Object> headers = message.getProperties().getHeaders();
		BasicProperties msg_prop=message.getProperties();
		Envelope msg_env=message.getEnvelope();

        // correlation ID
        if (msg_prop.getCorrelationId() != null) {
            map.put(AMQPConstants.AMQP_CORRELATION_ID, msg_prop.getCorrelationId());
        }
        // set the delivery mode as persistent or not
        map.put(AMQPConstants.AMQP_DELIVERY_MODE, Integer.toString(msg_prop.getDeliveryMode()));
        // FIXME ? Extract destination from... where?
       /*// destination name
        if (message.getAMQPDestination() != null) {
            Destination dest = message.getAMQPDestination();
            map.put(AMQPConstants.AMQP_DESTINATION, dest instanceof Queue ? ((Queue) dest).getQueueName() : ((Topic) dest).getTopicName());
        }*/
        // expiration
        map.put(AMQPConstants.AMQP_EXPIRATION, msg_prop.getExpiration());
        // if a AMQP message ID is found
        if (msg_prop.getMessageId() != null) {
            map.put(AMQPConstants.AMQP_MESSAGE_ID, msg_prop.getMessageId());
        }
        // priority
        map.put(AMQPConstants.AMQP_PRIORITY, Long.toString(msg_prop.getPriority()));
        // redelivered
        map.put(AMQPConstants.AMQP_REDELIVERED, Boolean.toString(msg_env.isRedeliver()));
        // replyto destination name
        if (msg_prop.getReplyTo() != null) {
            Destination dest = new Destination (msg_prop.getReplyTo());
            map.put(AMQPConstants.AMQP_REPLY_TO, dest);
        }
        // priority
        map.put(AMQPConstants.AMQP_TIMESTAMP, Long.toString(msg_prop.getTimestamp().getTime()));
        // message type
        if (msg_prop.getType() != null) {
            map.put(AMQPConstants.AMQP_TYPE, msg_prop.getType());
        }
        // any other transport properties / headers
        Set<String> e = null;
        e = msg_prop.getHeaders().keySet();
        for (String headerName:e){
        	Object o=headers.get(e);
        	if (o instanceof String) map.put(headerName, (String)o);
        	if (o instanceof Boolean) map.put(headerName, (Boolean)o);
        	if (o instanceof Integer) map.put(headerName, (Integer)o);
        	if (o instanceof Long) map.put(headerName, (Long)o);
        	if (o instanceof Double) map.put(headerName, (Double)o);
        	if (o instanceof Float) map.put(headerName, (Float)o);
        }
		return map;
	}




	/**
	 * Return a String representation of the destination type
	 * 
	 * @param destType
	 *            the destination type indicator int
	 * @return a descriptive String
	 */
	public static String getDestinationTypeAsString(int destType) {
		if (destType == AMQPConstants.QUEUE) {
			return "Queue";
		} else if (destType == AMQPConstants.EXCHANGE) {
			return "Exchange";
		} else {
			return "Generic";
		}
	}


}
