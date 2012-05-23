/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.axis2.transport.amqp.out;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.amqp.common.AMQPConstants;
import org.apache.axis2.transport.amqp.common.AMQPMessage;
import org.apache.axis2.transport.amqp.common.AxisAMQPException;
import org.apache.axis2.transport.amqp.common.Destination;
import org.apache.axis2.transport.base.BaseConstants;


import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

import javax.transaction.*;

/**
 * Performs the actual sending of a JMS message, and the subsequent committing of a JTA transaction
 * (if requested) or the local session transaction, if used. An instance of this class is unique
 * to a single message send out operation and will not be shared.
 */
public class AMQPMessageSender {

    private static final Log log = LogFactory.getLog(AMQPMessageSender.class);
	private Channel chan;
	private Destination destination;



    /**
     * This is a low-end method to support the one-time sends using JMS 1.0.2b
     * @param connection the JMS Connection
     * @param session JMS Channel
     * @param producer the MessageProducer
     * @param destination the JMS Destination
     * @param cacheLevel cacheLevel - None | Connection | Channel | Producer
     * @param jmsSpec11 true if the JMS 1.1 API should be used
     * @param isQueue posting to a Queue?
     */
    public AMQPMessageSender(Channel session, Destination destination) {
        this.chan = session;
        this.destination = destination;
    }

    /**
     * Perform actual send of JMS message to the Destination selected
     *
     * @param message the JMS message
     * @param msgCtx the Axis2 MessageContext
     * @throws IOException 
     */
    public void send(AMQPMessage message, MessageContext msgCtx) throws IOException {

    	Boolean jtaCommit    = getBooleanProperty(msgCtx, BaseConstants.JTA_COMMIT_AFTER_SEND);
    	Boolean rollbackOnly = getBooleanProperty(msgCtx, BaseConstants.SET_ROLLBACK_ONLY);
    	Boolean persistent   = getBooleanProperty(msgCtx, AMQPConstants.DELIVERY_MODE);
    	Integer priority     = getIntegerProperty(msgCtx, AMQPConstants.PRIORITY);
    	Integer timeToLive   = getIntegerProperty(msgCtx, AMQPConstants.TIME_TO_LIVE);
    	BasicProperties msg_prop = null;

    	// Do not commit, if message is marked for rollback
    	if (rollbackOnly != null && rollbackOnly) {
    		jtaCommit = Boolean.FALSE;
    	}

    	msg_prop=message.getProperties();
    	if (persistent != null) {
    		msg_prop=msg_prop.builder().deliveryMode(2).build();

    	}
    	if (priority != null) {
    		msg_prop=msg_prop.builder().priority(1).build();
    	}
    	if (timeToLive != null) {
    		msg_prop=msg_prop.builder().expiration(timeToLive.toString()).build();
    	}

    	boolean sendingSuccessful = false;
    	// perform actual message sending

    	if (destination.getType()==AMQPConstants.QUEUE){
    		chan.basicPublish("", destination.getName(), message.getProperties(), message.getBody());	
    	}
    	else {
    		chan.basicPublish(destination.getName(),destination.getRoutingKey(),message.getProperties(), message.getBody()); 
    	}

    	// set the actual MessageID to the message context for use by any others down the line
    	String msgId = null;
    	msgId = msg_prop.getMessageId();
    	if (msgId != null) {
    		msgCtx.setProperty(AMQPConstants.AMQP_MESSAGE_ID, msgId);
    	}


    	if (log.isDebugEnabled()) {
    		log.debug("Sent Message Context ID : " + msgCtx.getMessageID() +" with Message ID : " + msgId +" to destination : " + destination);
    	}

  /*  	if (jtaCommit != null) {
    		UserTransaction ut = (UserTransaction) msgCtx.getProperty(BaseConstants.USER_TRANSACTION);
    		if (ut != null) {
    			try {
    				if (sendingSuccessful && jtaCommit) {
    					ut.commit();
    				} else {
    					ut.rollback();
    				}
    				msgCtx.removeProperty(BaseConstants.USER_TRANSACTION);

    				if (log.isDebugEnabled()) {
    					log.debug((sendingSuccessful ? "Committed" : "Rolled back") +" JTA Transaction");
    				}

    			} catch (Exception e) {
    				handleException("Error committing/rolling back JTA transaction after " +"sending of message with MessageContext ID : " + msgCtx.getMessageID() + " to destination : " + destination, e);
    			}
    		}

    	} else {
    		if (chan.getTransacted()) {
    			if (sendingSuccessful && (rollbackOnly == null || !rollbackOnly)) {
    				chan.commit();
    			} else {
    				chan.rollback();
    			}
    		}
    		if (log.isDebugEnabled()) {
    			log.debug((sendingSuccessful ? "Committed" : "Rolled back") +" local (JMS Channel) Transaction");
    		}
    	}*/
    }

    /**
     * Close non-shared producer, session and connection if any
     */
    public void close() {
    	try {
			chan.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    private void handleException(String message, Exception e) {
        log.error(message, e);
        throw new AxisAMQPException(message, e);
    }

    private Boolean getBooleanProperty(MessageContext msgCtx, String name) {
        Object o = msgCtx.getProperty(name);
        if (o != null) {
            if (o instanceof Boolean) {
                return (Boolean) o;
            } else if (o instanceof String) {
                return Boolean.valueOf((String) o);
            }
        }
        return null;
    }

    private Integer getIntegerProperty(MessageContext msgCtx, String name) {
        Object o = msgCtx.getProperty(name);
        if (o != null) {
            if (o instanceof Integer) {
                return (Integer) o;
            } else if (o instanceof String) {
                return Integer.parseInt((String) o);
            }
        }
        return null;
    }


}
