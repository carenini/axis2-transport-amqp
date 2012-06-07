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

import org.apache.axis2.transport.OutTransportInfo;
import org.apache.axis2.transport.amqp.out.AMQPMessageSender;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Hashtable;

/**
 * The AMQP OutTransportInfo is a holder of information to send an outgoing
 * message (e.g. a Response) to a AMQP destination. Thus at a minimum a
 * reference to a ConnectionFactory and a Destination are held
 */
public class AMQPTransportInfo implements OutTransportInfo {

	private static final Log log = LogFactory.getLog(AMQPTransportInfo.class);

	/**
	 * this is a reference to the underlying AMQP ConnectionFactory when sending
	 * messages through connection factories not defined at the TransportSender
	 * level
	 */
	private ConnectionFactory connectionFactory = null;
	
	private AMQPConnectionFactoryManager conn_manager=null;
	/**
	 * this is a reference to a AMQP Connection Factory instance, which has a
	 * reference to the underlying actual connection factory, an open connection
	 * to the AMQP provider and optionally a session already available for use
	 */
	private AMQPConnectionFactory amqpConnectionFactory = null;
	/** the Destination queue or topic for the outgoing message */
	private Destination destination = null;

	/** the Reply Destination queue or topic for the outgoing message */
	private Destination replyDestination = null;

	/**
	 * the EPR properties when the out-transport info is generated from a target
	 * EPR
	 */
	private Hashtable<String, String> properties = null;
	/** the target EPR string where applicable */
	private String targetEPR = null;
	/**
	 * the message property name that stores the content type of the outgoing
	 * message
	 */
	private String contentType;

	/*Connection details */
	private String pass;
	private String user;

	/**
	 * Creates an instance using the given AMQP connection factory and
	 * destination
	 * 
	 * @param amqpConnectionFactory
	 *            the AMQP connection factory
	 * @param dest
	 *            the destination
	 * @param contentType
	 *            the content type
	 */
	public AMQPTransportInfo(AMQPConnectionFactory amqpConnectionFactory, Destination dest, String contentType) {
		this.amqpConnectionFactory = amqpConnectionFactory;
		this.destination = dest;
		this.contentType = contentType;
	}

	/**
	 * Creates and instance using the given URL
	 * 
	 * @param targetEPR
	 *            the target EPR
	 */
	public AMQPTransportInfo(String targetEPR) {
		String replyDestinationName=null;
		String destinationName=null;
		int replyDestinationType = -1;
		int destinationType = -1;
		
		this.targetEPR = targetEPR;
		if (!targetEPR.startsWith(AMQPConstants.AMQP_PREFIX)) {
			handleException("Invalid prefix for a AMQP EPR : " + targetEPR);
		} else {
			properties = BaseUtils.getEPRProperties(targetEPR);
			destinationName = properties.get(AMQPConstants.PARAM_REPLY_DESTINATION);
			destinationType = Integer.parseInt(properties.get(AMQPConstants.PARAM_DEST_TYPE));
			
			replyDestinationType=Integer.parseInt(properties.get(AMQPConstants.PARAM_REPLY_DEST_TYPE));
			replyDestinationName = properties.get(AMQPConstants.PARAM_REPLY_DESTINATION);
		
			contentType = properties.get(AMQPConstants.CONTENT_TYPE_PROPERTY_PARAM);

			// FIXME extract routing keys
			if(destinationType==AMQPConstants.QUEUE) destination=DestinationFactory.queueDestination(destinationName);
			else destination=DestinationFactory.exchangeDestination(destinationName, destinationType, null);
			
			if(replyDestinationType==AMQPConstants.QUEUE) replyDestination=DestinationFactory.queueDestination(replyDestinationName);
			else replyDestination=DestinationFactory.exchangeDestination(replyDestinationName, replyDestinationType, null);
			
			// create a one time connection and session to be used
			user = properties != null ? properties.get(AMQPConstants.PARAM_AMQP_USERNAME) : null;
			pass = properties != null ? properties.get(AMQPConstants.PARAM_AMQP_PASSWORD) : null;
		}
	}

	/**
	 * Provides a lazy load when created with a target EPR. This method performs
	 * actual lookup for the connection factory and destination
	 */
/*	public void loadConnectionFactoryFromProperies() {
		if (properties != null) {
			connectionFactory = getConnectionFactory(properties);
		}
	}
*/
	/**
	 * Get the referenced ConnectionFactory using the properties from the
	 * context
	 * 
	 * @param context
	 *            the context to use for lookup
	 * @param props
	 *            the properties which contains the JNDI name of the factory
	 * @return the connection factory
	 */
/*	private ConnectionFactory getConnectionFactory(Hashtable<String, String> props) {
		String conFacId = props.get(AMQPConstants.PARAM_CONFAC_ID);
		if (conFacId != null) {
			return AMQPUtils.lookup(ConnectionFactory.class, conFacId);
		} else {
			handleException("Connection Factory JNDI name cannot be determined");
		}
		return null;
	}
*/
	/**
	 * Get the AMQP destination specified by the given URL from the context
	 * 
	 * @param context
	 *            the Context to lookup
	 * @param url
	 *            URL
	 * @return the AMQP destination, or null if it does not exist
	 */
/*	private Destination getDestination(String url) {
		Destination d = AMQPUtils.getDestination(url);
		log.debug("Lookup the AMQP destination " + d.getName() + " of type " + d.getType() + " extracted from the URL " + url);
		return d;
	}
*/
	private void handleException(String s) {
		log.error(s);
		throw new AxisAMQPException(s);
	}

	/*
	private void handleException(String s, Exception e) {
		log.error(s, e);
		throw new AxisAMQPException(s, e);
	}
	*/

	public Destination getDestination() {
		return destination;
	}

	public ConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	public AMQPConnectionFactory getAmqpConnectionFactory() {
		return amqpConnectionFactory;
	}

	public void setContentType(String contentType) {
		// this is a useless Axis2 method imposed by the OutTransportInfo
		// interface :(
	}

	public Hashtable<String, String> getProperties() {
		return properties;
	}

	public String getTargetEPR() {
		return targetEPR;
	}

	public Destination getReplyDestination() {
		return replyDestination;
	}

	public void setReplyDestination(Destination replyDestination) {
		this.replyDestination = replyDestination;
	}

	public String getContentTypeProperty() {
		return contentType;
	}

	public void setContentTypeProperty(String contentTypeProperty) {
		this.contentType = contentTypeProperty;
	}

	/**
	 * Create a one time MessageProducer for this AMQP OutTransport information.
	 * @return a AMQPSender based on one-time use resources
	 * @throws IOException 
	 * @throws AMQPException
	 *             on errors, to be handled and logged by the caller
	 */
	public AMQPMessageSender createAMQPSender() throws IOException {
		Channel chan = null;
		Connection connection = null;
		int dest_type=-1;
		


		if (connection == null) {
			connection = amqpConnectionFactory != null ? amqpConnectionFactory.getConnection() : null;
		}

		dest_type=destination.getType();
		chan=connection.createChannel();
		if (connection != null) {
			if (dest_type == AMQPConstants.QUEUE) chan.queueDeclare(destination.getName(), false, false, true, null);
			// TODO Exchange type
			else chan.exchangeDeclare(destination.getName(), "direct", true);
		}
		return new AMQPMessageSender(chan, destination);
	}

	public Object getConnectionURL() {
		// TODO Auto-generated method stub
		return null;
	}

	public AMQPConnectionFactoryManager getConnectionManager() {
		return conn_manager;
	}

	public void setConnectionManager(AMQPConnectionFactoryManager conn_manager) {
		this.conn_manager = conn_manager;
	}
}
