package org.apache.axis2.transport.amqp.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterIncludeImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

public class AMQPUtils
{

    private static final Log log = LogFactory.getLog(AMQPUtils.class);

    private static AMQPUtils _instance = new AMQPUtils();

    public static AMQPUtils getInstace() {
        return _instance;
    }

    public InputStream getInputStream(Object message)
    {
        Message msg = (Message)message;
        return new ByteArrayInputStream(msg.getBody());
    }

    
    public byte[] getMessageBinaryPayload(Object message)
    {
        return ((Message)message).getBody();
    }

    
    public String getMessageTextPayload(Object message)
    {
        return null;
    }

    public static String getProperty(Object message, String property)
    {
        try {
            return (String)((Message)message).getMessageProperties().getHeaders().get(property);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Extract transport level headers for JMS from the given message into a Map
     *
     * @param message the JMS message
     * @return a Map of the transport headers
     */
    public static Map getTransportHeaders(Message message) {
        // create a Map to hold transport headers
        Map<String,Object> map = new HashMap<String,Object>();
        MessageProperties msg_prop = message.getMessageProperties();
        // correlation ID
        if (msg_prop.getCorrelationId() != null) {
            map.put(AMQPConstants.AMQP_CORRELATION_ID, msg_prop.getCorrelationId());
        }

        // set the delivery mode as persistent or not
        try {
            map.put(AMQPConstants.AMQP_DELIVERY_MODE,msg_prop.getDeliveryMode());
        } catch (Exception ignore) {}

        // destination name
        map.put(AMQPConstants.AMQP_EXCHANGE_NAME,msg_prop.getReceivedExchange());
        map.put(AMQPConstants.AMQP_ROUTING_KEY,msg_prop.getReceivedRoutingKey());

        // expiration
        try {
            map.put(AMQPConstants.AMQP_EXPIRATION, msg_prop.getExpiration());
        } catch (Exception ignore) {}

        // if a JMS message ID is found
        if (msg_prop.getMessageId() != null) {
            map.put(AMQPConstants.AMQP_MESSAGE_ID, msg_prop.getMessageId());
        }

        // priority
        map.put(AMQPConstants.AMQP_PRIORITY,msg_prop.getPriority());

        // redelivered
        map.put(AMQPConstants.AMQP_REDELIVERED, msg_prop.isRedelivered());

        // replyto destination name
        Address replyTo=msg_prop.getReplyToAddress();
        if (replyTo != null) {
        	
            map.put(AMQPConstants.AMQP_REPLY_TO_EXCHANGE_NAME, replyTo.getExchangeName());
            map.put(AMQPConstants.AMQP_REPLY_TO_ROUTING_KEY, replyTo.getRoutingKey());
        }

        // priority
        map.put(AMQPConstants.AMQP_TIMESTAMP,msg_prop.getTimestamp());

        // any other transport properties / headers
        map.putAll(msg_prop.getHeaders());

        return map;
    }

    /**
     * Get the AMQP destination used by this service
     *
     * @param service the Axis Service
     * @return the name of the JMS destination
     */
    public static List<Destination> getBindingsForService(AxisService service) {
        Parameter bindingsParam = service.getParameter(AMQPConstants.BINDINGS_PARAM);
        ParameterIncludeImpl pi = new ParameterIncludeImpl();
        try {
            pi.deserializeParameters((OMElement) bindingsParam.getValue());
        } catch (AxisFault axisFault) {
            log.error("Error reading parameters for AMQP binding definitions" +
                    bindingsParam.getName(), axisFault);
        }

        Iterator params = pi.getParameters().iterator();
        ArrayList<Destination> list = new ArrayList<Destination>();
        if(params.hasNext())
        {
            while (params.hasNext())
            {
                Parameter p = (Parameter) params.next();
                Destination binding = new Destination();
                OMAttribute exchangeTypeAttr = p.getParameterElement().getAttribute(new QName(AMQPConstants.BINDING_EXCHANGE_TYPE_ATTR));
                OMAttribute exchangeNameAttr = p.getParameterElement().getAttribute(new QName(AMQPConstants.BINDING_EXCHANGE_NAME_ATTR));
                OMAttribute routingKeyAttr = p.getParameterElement().getAttribute(new QName(AMQPConstants.BINDING_ROUTING_KEY_ATTR));
                OMAttribute primaryAttr = p.getParameterElement().getAttribute(new QName(AMQPConstants.BINDINGS_PRIMARY_ATTR));

                if ( exchangeTypeAttr != null) {
                    binding.setExchangeType(exchangeTypeAttr.getAttributeValue());
                }else if ( exchangeNameAttr != null) {
                    binding.setExchangeName(exchangeNameAttr.getAttributeValue());
                }else if ( primaryAttr != null) {
                    binding.setPrimary(true);
                }
                list.add(binding);
            }
        }else{
            // go for the defaults
            Destination binding = new Destination();
            binding.setRoutingKey(service.getName());
            list.add(binding);
        }

        return list;
    }

    /**
     * Return a String representation of the destination type
     * @param destType the destination type indicator int
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
