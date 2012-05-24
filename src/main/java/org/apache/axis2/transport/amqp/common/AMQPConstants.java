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

import org.apache.axis2.client.Options;

public class AMQPConstants {

    /**
     * The prefix indicating an Axis AMQP URL
     */
    public static final String AMQP_PREFIX = "amqp:";

    public static final String TRANSPORT_AMQP = "amqp";

    //------------------------------------ defaults / constants ------------------------------------
    /**
     * The local (Axis2) JMS connection factory name of the default connection
     * factory to be used, if a service does not explicitly state the connection
     * factory it should be using by a Parameter named JMSConstants.CONFAC_PARAM
     */
    public static final String DEFAULT_CONFAC_NAME = "default";
    
    /**
     * Value indicating a Queue used for {@link DEST_PARAM_TYPE}, {@link REPLY_PARAM_TYPE}
     */
    public static final String DESTINATION_TYPE_QUEUE = "queue";
    /**
     * Value indicating a Topic used for {@link DEST_PARAM_TYPE}, {@link REPLY_PARAM_TYPE}
     */
    public static final String DESTINATION_TYPE_EXCHANGE = "exchange";

    /**
     * Value indicating a JMS 1.1 Generic Destination used by {@link DEST_PARAM_TYPE}, {@link REPLY_PARAM_TYPE}
     */
    public static final String DESTINATION_TYPE_GENERIC = "generic";

    /** Do not cache any JMS resources between tasks (when sending) or JMS CF's (when sending) */
	public static final int CACHE_NONE = 0;
	/** Cache only the JMS connection between tasks (when receiving), or JMS CF's (when sending)*/
	public static final int CACHE_CONNECTION = 1;
	/** Cache only the JMS connection and Session between tasks (receiving), or JMS CF's (sending) */
	public static final int CACHE_SESSION = 2;
	/** Cache the JMS connection, Session and Consumer between tasks when receiving*/
	public static final int CACHE_CONSUMER = 3;
	/** Cache the JMS connection, Session and Producer within a JMSConnectionFactory when sending */
	public static final int CACHE_PRODUCER = 4;
    /** automatic choice of an appropriate caching level (depending on the transaction strategy) */
	public static final int CACHE_AUTO = 5;

    /** A JMS 1.1 Generic Destination type or ConnectionFactory */
    public static final int GENERIC = 0;
    /** A Queue Destination type or ConnectionFactory */
    public static final int QUEUE = 1;
    /** A Topic Destination type or ConnectionFactory */
    public static final int EXCHANGE = 2;

    /**
     * The EPR parameter name indicating the name of the message level property that indicated the content type.
     */
    public static final String CONTENT_TYPE_PROPERTY_PARAM = "transport.amqp.ContentTypeProperty";

    //---------------------------------- services.xml parameters -----------------------------------
    /**
     * The Service level Parameter name indicating the JMS destination for requests of a service
     */
    public static final String PARAM_DESTINATION = "transport.amqp.Destination";
    /**
     * The Service level Parameter name indicating the destination type for requests.
     * also see {@link DESTINATION_TYPE_QUEUE}, {@link DESTINATION_TYPE_TOPIC}
     */
    public static final String PARAM_DEST_TYPE = "transport.amqp.DestinationType";
    /**
     * The Service level Parameter name indicating the [default] response destination of a service
     */
    public static final String PARAM_REPLY_DESTINATION = "transport.amqp.ReplyDestination";
    /**
     * The Service level Parameter name indicating the response destination type
     * also see {@link DESTINATION_TYPE_QUEUE}, {@link DESTINATION_TYPE_TOPIC}
     */
    public static final String PARAM_REPLY_DEST_TYPE = "transport.amqp.ReplyDestinationType";
    /**
     * The Parameter name of an Axis2 service, indicating the JMS connection
     * factory which should be used to listen for messages for it. This is
     * the local (Axis2) name of the connection factory and not the JNDI name
     */
    public static final String PARAM_AMQP_CONFAC = "transport.amqp.ConnectionFactory";
    /**
     * Connection factory type if using JMS 1.0, either DESTINATION_TYPE_QUEUE or DESTINATION_TYPE_TOPIC
     */
    public static final String PARAM_CONFAC_TYPE = "transport.amqp.ConnectionFactoryType";
    /**
     * The Parameter name indicating the JMS connection factory JNDI name
     */
    public static final String PARAM_CONFAC_ID = "transport.amqp.ConnectionFactoryJNDIName";
    /**
     * The Parameter indicating the expected content type for messages received by the service.
     */
    public static final String CONTENT_TYPE_PARAM = "transport.amqp.ContentType";
    /**
     * The Parameter indicating a final EPR as a String, to be published on the WSDL of a service
     * Could occur more than once, and could provide additional connection properties or a subset
     * of the properties auto computed. Also could replace IP addresses with hostnames, and expose
     * public credentials clients. If a user specified this parameter, the auto generated EPR will
     * not be exposed - unless an instance of this parameter is added with the string "legacy"
     * This parameter could be used to expose EPR's conforming to the proposed SOAP/JMS spec
     * until such time full support is implemented for it.
     */
    public static final String PARAM_PUBLISH_EPR = "transport.amqp.PublishEPR";
    /** The parameter indicating the JMS API specification to be used - if this is "1.1" the JMS
     * 1.1 API would be used, else the JMS 1.0.2B
     */
    public static final String PARAM_JMS_SPEC_VER = "transport.amqp.JMSSpecVersion";

    /**
     * The Parameter indicating whether the JMS Session should be transacted for the service
     * Specified as a "true" or "false"
     */
    public static final String PARAM_SESSION_TRANSACTED = "transport.amqp.SessionTransacted";
    /**
     * The Parameter indicating the Session acknowledgement for the service. Must be one of the
     * following Strings, or the appropriate Integer used by the JMS API
     * "AUTO_ACKNOWLEDGE", "CLIENT_ACKNOWLEDGE", "DUPS_OK_ACKNOWLEDGE" or "SESSION_TRANSACTED"
     */
    public static final String PARAM_SESSION_ACK = "transport.amqp.SessionAcknowledgement";
    /** A message selector to be used when messages are sought for this service */
    public static final String PARAM_MSG_SELECTOR = "transport.amqp.MessageSelector";
    /** Is the Subscription durable ? - "true" or "false" See {@link PARAM_DURABLE_SUB_NAME} */
    public static final String PARAM_SUB_DURABLE = "transport.amqp.SubscriptionDurable";
    /** The name for the durable subscription See {@link PARAM_SUB_DURABLE}*/
    public static final String PARAM_DURABLE_SUB_NAME = "transport.amqp.DurableSubscriberName";
    /**
     * JMS Resource cachable level to be used for the service One of the following:
     * {@link CACHE_NONE}, {@link CACHE_CONNECTION}, {@link CACHE_SESSION}, {@link CACHE_PRODUCER},
     * {@link CACHE_CONSUMER}, or {@link CACHE_AUTO} - to let the transport decide
     */
    public static final String PARAM_CACHE_LEVEL = "transport.amqp.CacheLevel";
    /** Should a pub-sub connection receive messages published by itself? */
    public static final String PARAM_PUBSUB_NO_LOCAL = "transport.amqp.PubSubNoLocal";
    /**
     * The number of milliseconds to wait for a message on a consumer.receive() call
     * negative number - wait forever
     * 0 - do not wait at all
     * positive number - indicates the number of milliseconds to wait
     */
    public static final String PARAM_RCV_TIMEOUT = "transport.amqp.ReceiveTimeout";
    /**
     *The number of concurrent consumers to be created to poll for messages for this service
     * For Topics, this should be ONE, to prevent receipt of multiple copies of the same message
     */
    public static final String PARAM_CONCURRENT_CONSUMERS = "transport.amqp.ConcurrentConsumers";
    /**
     * The maximum number of concurrent consumers for the service - See {@link PARAM_CONCURRENT_CONSUMERS}
     */
    public static final String PARAM_MAX_CONSUMERS = "transport.amqp.MaxConcurrentConsumers";
    /**
     * The number of idle (i.e. message-less) polling attempts before a worker task commits suicide,
     * to scale down resources, as load decreases
     */
    public static final String PARAM_IDLE_TASK_LIMIT = "transport.amqp.IdleTaskLimit";
    /**
     * The maximum number of messages a polling worker task should process, before suicide - to
     * prevent many longer running threads - default is unlimited (i.e. a worker task will live forever)
     */
    public static final String PARAM_MAX_MSGS_PER_TASK = "transport.amqp.MaxMessagesPerTask";
    /**
     * Number of milliseconds before the first reconnection attempt is tried, on detection of an
     * error. Subsequent retries follow a geometric series, where the
     * duration = previous duration * factor
     * This is further limited by the {@link PARAM_RECON_MAX_DURATION} to be meaningful
     */
    public static final String PARAM_RECON_INIT_DURATION = "transport.amqp.InitialReconnectDuration";
    /** @see PARAM_RECON_INIT_DURATION */
    public static final String PARAM_RECON_FACTOR = "transport.amqp.ReconnectProgressFactor";
    /** @see PARAM_RECON_INIT_DURATION */
    public static final String PARAM_RECON_MAX_DURATION = "transport.amqp.MaxReconnectDuration";

    /** The username to use when obtaining a JMS Connection */
    public static final String PARAM_JMS_USERNAME = "transport.amqp.UserName";
    /** The password to use when obtaining a JMS Connection */
    public static final String PARAM_JMS_PASSWORD = "transport.amqp.Password";

    //-------------- message context / transport header properties and client options --------------
    /**
     * A MessageContext property or client Option indicating the JMS message type
     */
    public static final String AMQP_MESSAGE_TYPE = "AMQP_MESSAGE_TYPE";
    /**
     * The message type indicating a BytesMessage. See {@link JMS_MESSAGE_TYPE}
     */
    public static final String JMS_BYTE_MESSAGE = "JMS_BYTE_MESSAGE";
    /**
     * The message type indicating a TextMessage. See {@link JMS_MESSAGE_TYPE}
     */
    public static final String JMS_TEXT_MESSAGE = "JMS_TEXT_MESSAGE";
    /**
     * A MessageContext property or client Option indicating the time to wait for a response JMS message
     */
    public static final String JMS_WAIT_REPLY = "JMS_WAIT_REPLY";
    /**
     * A MessageContext property or client Option indicating the JMS correlation id
     */
    public static final String AMQP_CORRELATION_ID = "AMQP_CORRELATION_ID";
     /**
     * A MessageContext property or client Option indicating the JMS delivery mode as an Integer or String
     * Value 1 - javax.amqp.DeliveryMode.NON_PERSISTENT
     * Value 2 - javax.amqp.DeliveryMode.PERSISTENT
     */
    public static final String JMS_DELIVERY_MODE = "JMS_DELIVERY_MODE";
    /**
     * A MessageContext property or client Option indicating the JMS destination to use on a Send
     */
    public static final String JMS_DESTINATION = "JMS_DESTINATION";
    /**
     * A MessageContext property or client Option indicating the JMS message expiration - a Long value
     * specified as a String
     */
    public static final String JMS_EXPIRATION = "JMS_EXPIRATION";
    /**
     * A MessageContext property indicating if the message is a redelivery (Boolean as a String)
     */
    public static final String JMS_REDELIVERED = "JMS_REDELIVERED";
    /**
     * A MessageContext property or client Option indicating the JMS replyTo Destination
     */
    public static final String AMQP_REPLY_TO = "AMQP_REPLY_TO";
    /**
     * A MessageContext property or client Option indicating the JMS replyTo Destination type
     * See {@link DESTINATION_TYPE_QUEUE} and {@link DESTINATION_TYPE_TOPIC}
     */
    public static final String AMQP_REPLY_TO_TYPE = "AMQP_REPLY_TO_TYPE";
    /**
     * A MessageContext property or client Option indicating the JMS timestamp (Long specified as String)
     */
    public static final String JMS_TIMESTAMP = "JMS_TIMESTAMP";
    /**
     * A MessageContext property indicating the JMS type String returned by {@link javax.amqp.Message.getJMSType()}
     */
    public static final String AMQP_TYPE = "AMQP_TYPE";
    /**
     * A MessageContext property or client Option indicating the JMS priority
     */
    public static final String JMS_PRIORITY = "JMS_PRIORITY";
    /**
     * A MessageContext property or client Option indicating the JMS time to live for message sent
     */
    public static final String JMS_TIME_TO_LIVE = "JMS_TIME_TO_LIVE";

    /** The prefix that denotes JMSX properties */
    public static final String JMSX_PREFIX = "JMSX";
    /** The JMSXGroupID property */
    public static final String JMSX_GROUP_ID = "JMSXGroupID";
    /** The JMSXGroupSeq property */
    public static final String JMSX_GROUP_SEQ = "JMSXGroupSeq";

    public static final String DESTINATION_TYPE_FANOUT = "generic";

	public static final String PARAM_AMQP_HOST = "transport.amqp.hostname";
	public static final String  PARAM_AMQP_PASSWORD= "transport.amqp.password";
	public static final String  PARAM_AMQP_PORT= "transport.amqp.port";
	public static final String PARAM_AMQP_USERNAME = "transport.amqp.username";
	public static final String PARAM_AMQP_VHOST = "transport.amqp.virtualhost";

	// TODO fill from JMS Constants
	public static final String DELIVERY_MODE = null;

	public static final String PRIORITY = null;

	public static final String TIME_TO_LIVE = null;

    //------------------------------------ defaults ------------------------------------
    /**
     * The local (Axis2) AMQP connection name of the default connection
     * to be used.
     */
    public static final String DEFAULT_CONNECTION = "default";
    /**
     * The default AMQP time out waiting for a reply
     */
    public static final long DEFAULT_AMQP_TIMEOUT = Options.DEFAULT_TIMEOUT_MILLISECONDS;

    //-------------------------- axis2.xml parameters --------------------------------
    /** Connection URL specified in the axis2.xml or services.xml */
    public static final String CONNECTION_URL_PARAM = "transport.amqp.ConnectionURL";

    /** default exchange name specified axis2.xml */
    public static final String EXCHANGE_NAME_PARAM = "transport.amqp.ExchangeName";

    /** default exchange type specified axis2.xml */
    public static final String EXCHANGE_TYPE_PARAM = "transport.amqp.ExchangeType";

    //-------------------------- services.xml parameters --------------------------------
    /** routing key specified in the services.xml */
    public static final String BINDING_ROUTING_KEY_ATTR = "routingKey";

    /** exchange name specified in the services.xml */
    public static final String BINDING_EXCHANGE_NAME_ATTR = "exchangeName";

    /** exchange type specified in the services.xml */
    public static final String BINDING_EXCHANGE_TYPE_ATTR = "exchangeType";

    /** bindings specified in the services.xml */
    public static final String BINDINGS_PARAM = "transport.amqp.Bindings";

    /** bindings specified in the services.xml */
    public static final String BINDINGS_PRIMARY_ATTR = "primary";

    /**
     * The Parameter name indicating the response AMQP destination
     */
    public static final String REPLY_EXCHANGE_TYPE_PARAM = "transport.amqp.ReplyExchangeType";
    public static final String REPLY_EXCHANGE_NAME_PARAM = "transport.amqp.ReplyExchangeName";
    /**
     * The Parameter name indicating the response AMQP destination class.Ex direct,topic,fannot ..etc
     */
    public static final String REPLY_ROUTING_KEY_PARAM = "transport.amqp.ReplyRoutingKey";

    /**
     * The Parameter name of an Axis2 service, indicating the AMQP connection
     * which should be used to listen for messages for it.
     */
    public static final String CONNECTION_NAME_PARAM = "transport.amqp.ConnectionName";
    /**
     * If reconnect timeout if connection error occurs in seconds
     */
    public static final String RECONNECT_TIMEOUT = "transport.amqp.ReconnectTimeout";

    //------------ message context / transport header properties and client options ------------
    /**
     * A MessageContext property or client Option stating the time to wait for a response JMS message
     */
    public static final String AMQP_WAIT_REPLY = "AMQP_WAIT_REPLY";
    /**
     * A MessageContext property or client Option stating the AMQP correlation id
     */
    public static final String AMQP_CORELATION_ID = "AMQP_CORELATION_ID";
    /**
     * A MessageContext property or client Option stating the AMQP message id
     */
    public static final String AMQP_MESSAGE_ID = "AMQP_MESSAGE_ID";
    /**
     * A MessageContext property or client Option stating the AMQP delivery mode
     */
    public static final String AMQP_DELIVERY_MODE = "AMQP_DELIVERY_MODE";
    /**
     * A MessageContext property or client Option stating the AMQP destination
     */
    public static final String AMQP_EXCHANGE_NAME = "AMQP_EXCHANGE_NAME";

    public static final String AMQP_EXCHANGE_TYPE = "AMQP_EXCHANGE_TYPE";

    public static final String AMQP_ROUTING_KEY = "AMQP_ROUTING_KEY";
    /**
     * A MessageContext property or client Option stating the AMQP expiration
     */
    public static final String AMQP_EXPIRATION = "AMQP_EXPIRATION";
    /**
     * A MessageContext property or client Option stating the AMQP priority
     */
    public static final String AMQP_PRIORITY = "AMQP_PRIORITY";
    /**
     * A MessageContext property stating if the message is a redelivery
     */
    public static final String AMQP_REDELIVERED = "AMQP_REDELIVERED";
    /**
     * A MessageContext property or client Option stating the AMQP replyTo
     */
    public static final String AMQP_REPLY_TO_EXCHANGE_NAME = "AMQP_REPLY_TO_EXCHANGE_NAME";

    public static final String AMQP_REPLY_TO_EXCHANGE_TYPE = "AMQP_REPLY_TO_EXCHANGE_TYPE";

    public static final String AMQP_REPLY_TO_ROUTING_KEY = "AMQP_REPLY_TO_ROUTING_KEY";
    /**
     * A MessageContext property or client Option stating the AMQP timestamp
     */
    public static final String AMQP_TIMESTAMP = "AMQP_TIMESTAMP";
    /**
     * A MessageContext property or client Option stating the AMQP type
     */
    public static final String AMQP_CONTENT_TYPE = "AMQP_CONTENT_TYPE";
}
