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

import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.MetricsCollector;
import org.apache.axis2.transport.amqp.ctype.ContentTypeInfo;
import org.apache.axis2.transport.amqp.in.AMQPListener;
import org.apache.axis2.transport.amqp.out.AMQPOutTransportInfo;
import org.apache.axis2.context.MessageContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;

import javax.transaction.UserTransaction;

/**
 * This is the JMS message receiver which is invoked when a message is received. This processes
 * the message through the engine
 */
public class AMQPMessageReceiver {

    private static final Log log = LogFactory.getLog(AMQPMessageReceiver.class);

    /** The JMSListener */
    private AMQPListener amqpListener = null;
    /** A reference to the JMS Connection Factory */
    private AMQPConnectionFactory amqpConnectionFactory = null;
    /** The JMS metrics collector */
    private MetricsCollector metrics = null;
    /** The endpoint this message receiver is bound to */
    final AMQPEndpoint endpoint;

    /**
     * Create a new JMSMessage receiver
     *
     * @param jmsListener the JMS transport Listener
     * @param jmsConFac   the JMS connection factory we are associated with
     * @param workerPool  the worker thread pool to be used
     * @param cfgCtx      the axis ConfigurationContext
     * @param serviceName the name of the Axis service
     * @param endpoint    the JMSEndpoint definition to be used
     */
    AMQPMessageReceiver(AMQPListener listener, AMQPConnectionFactory conFac, AMQPEndpoint endpoint) {
        this.amqpListener = listener;
        this.amqpConnectionFactory = conFac;
        this.endpoint = endpoint;
        this.metrics = listener.getMetricsCollector();
    }

    /**
     * Process a new message received
     *
     * @param message the JMS message received
     * @param ut      UserTransaction which was used to receive the message
     * @return true if caller should commit
     */
    public boolean onMessage(Message message, UserTransaction ut) {
    	MessageProperties msg_prop=message.getMessageProperties();
        
    	if (log.isDebugEnabled()) {
    		StringBuffer sb = new StringBuffer();
    		sb.append("Received new JMS message for service :").append(endpoint.getServiceName());
    		sb.append("\nDestination    : ").append(msg_prop.getReceivedExchange());
    		sb.append("\nMessage ID     : ").append(msg_prop.getMessageId());
    		sb.append("\nCorrelation ID : ").append(msg_prop.getCorrelationId());
    		sb.append("\nReplyTo        : ").append(msg_prop.getReplyTo());
    		sb.append("\nRedelivery ?   : ").append(msg_prop.isRedelivered());
    		sb.append("\nPriority       : ").append(msg_prop.getPriority());
    		sb.append("\nExpiration     : ").append(msg_prop.getExpiration());
    		sb.append("\nTimestamp      : ").append(msg_prop.getTimestamp());
    		sb.append("\nMessage Type   : ").append(msg_prop.getType());
    		sb.append("\nPersistent ?   : ").append(MessageDeliveryMode.PERSISTENT == msg_prop.getDeliveryMode());

    		log.debug(sb.toString());
    		if (log.isTraceEnabled()) {
    			log.trace("\nMessage : " + message.getBody());
    		}
    	}
        // update transport level metrics
        metrics.incrementBytesReceived(msg_prop.getContentLength());
        

        // has this message already expired? expiration time == 0 means never expires
        // TODO: explain why this is necessary; normally it is the responsibility of the provider to handle message expiration
        long expiryTime = Long.parseLong(msg_prop.getExpiration());
        if (expiryTime > 0 && System.currentTimeMillis() > expiryTime) {
        	if (log.isDebugEnabled()) {
        		log.debug("Discard expired message with ID : " + msg_prop.getMessageId());
        	}
        	return true;
        }


        boolean successful = false;
        try {
            successful = processThroughEngine(message, ut);

        } catch (AxisFault e) {
            log.error("Axis fault processing message", e);
        } catch (Exception e) {
            log.error("Unknown error processing message", e);

        } finally {
            if (successful) {
                metrics.incrementMessagesReceived();
            } else {
                metrics.incrementFaultsReceiving();
            }
        }

        return successful;
    }

    /**
     * Process the new message through Axis2
     *
     * @param message the JMS message
     * @param ut      the UserTransaction used for receipt
     * @return true if the caller should commit
     * @throws JMSException, on JMS exceptions
     * @throws AxisFault     on Axis2 errors
     */
    private boolean processThroughEngine(Message message, UserTransaction ut) throws AxisFault {
    	MessageProperties msg_prop=message.getMessageProperties();
        MessageContext msgContext = endpoint.createMessageContext();

        // set the JMS Message ID as the Message ID of the MessageContext
        msgContext.setMessageID(msg_prop.getMessageId());
        msgContext.setProperty(AMQPConstants.AMQP_CORRELATION_ID, msg_prop.getMessageId());

        String soapAction = AMQPUtils.getProperty(message, BaseConstants.SOAPACTION);

        ContentTypeInfo contentTypeInfo =endpoint.getContentTypeRuleSet().getContentTypeInfo(message);
        if (contentTypeInfo == null) {
            throw new AxisFault("Unable to determine content type for message " + msgContext.getMessageID());
        }

        // set the message property OUT_TRANSPORT_INFO
        // the reply is assumed to be over the JMSReplyTo destination, using
        // the same incoming connection factory, if a JMSReplyTo is available
        String replyTo = msg_prop.getReplyTo();
        if (replyTo == null) {
            // does the service specify a default reply destination ?
            String replyDestinationAddress = endpoint.getReplyDestinationAddress();
            if (replyDestinationAddress != null) {
                replyTo = replyDestinationAddress;
            }

        }
        if (replyTo != null) {
            msgContext.setProperty(Constants.OUT_TRANSPORT_INFO, new AMQPOutTransportInfo(amqpConnectionFactory, replyTo, contentTypeInfo.getPropertyName()));
        }

        AMQPUtils.setSOAPEnvelope(message, msgContext, contentTypeInfo.getContentType());
        if (ut != null) {
            msgContext.setProperty(BaseConstants.USER_TRANSACTION, ut);
        }

        try {
            amqpListener.handleIncomingMessage(msgContext, AMQPUtils.getTransportHeaders(message), soapAction, contentTypeInfo.getContentType());

        } finally {

            Object o = msgContext.getProperty(BaseConstants.SET_ROLLBACK_ONLY);
            if (o != null) {
                if ((o instanceof Boolean && ((Boolean) o)) ||
                    (o instanceof String && Boolean.valueOf((String) o))) {
                    return false;
                }
            }
            return true;
        }
    }
}
