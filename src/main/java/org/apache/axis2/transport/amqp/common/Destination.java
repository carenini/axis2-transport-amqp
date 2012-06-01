package org.apache.axis2.transport.amqp.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.axis2.AxisFault;


/** sample uri formats - this is temporary until the AMQP WG defines a proper addressing scheme
*
* uri="amqp:/direct/amq.direct?transport.amqp.RoutingKey=SimpleStockQuoteService&amp;transport.amqp.ConnectionURL=qpid:virtualhost=test;client_id=foo@tcp:myhost.com:5672"
* amqp:/topic/amq.topic?transport.amqp.RoutingKey=weather.us.ny&amp;transport.amqp.ConnectionURL=qpid:virtualhost=test;client_id=foo@tcp:myhost.com:5672
*/
public class Destination {

	private String name=null;
    private String routingKey;
    private int type=AMQPConstants.QUEUE;
    private boolean primary=false;
    
    public Destination(){
    }

    
    public String getRoutingKey()
    {
        return routingKey;
    }
    public void setRoutingKey(String routingKey)
    {
        this.routingKey = routingKey;
    }

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}
	
    public static Map<String, String>  parse(String uri){
        Map<String, String> props = new HashMap<String, String>();
        String temp = uri.substring(6,uri.indexOf("?"));
        String exchangeType =  temp.substring(0,temp.indexOf("/"));
        String exchangeName =  temp.substring(temp.indexOf("/"),temp.length());
        if (exchangeType == null || exchangeType.trim().equals("")){
           throw new IllegalArgumentException("exchange type cannot be null");
        }
        if (exchangeType == null || exchangeType.trim().equals("")){
            throw new IllegalArgumentException("exchange name cannot be null");
         }
        props.put(AMQPConstants.EXCHANGE_NAME_PARAM, exchangeName);
        props.put(AMQPConstants.EXCHANGE_TYPE_PARAM, exchangeType);
        String paramStr =  uri.substring(uri.indexOf("?")+1,uri.length());
        String[] params = paramStr.split("&amp;");
        for (String param:params){
            String key = param.substring(0,param.indexOf("="));
            String value = param.substring(param.indexOf("=")+1,param.length());
            if("connectionURL".equals(key)){
                key = AMQPConstants.CONNECTION_URL_PARAM;
            }
            props.put(key, value);
        }
        return props;
     }


	public boolean isPrimary() {
		return primary;
	}

	public void setPrimary(boolean primary) {
		this.primary = primary;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String toAddress(){
		return null;
		
	}
	
	public static int param_to_destination_type(String value) throws AxisFault{
		if (AMQPConstants.DESTINATION_TYPE_QUEUE.equals(value)) return AMQPConstants.QUEUE;
		else if (AMQPConstants.DESTINATION_TYPE_DIRECT_EXCHANGE.equals(value)) return AMQPConstants.DIRECT_EXCHANGE;
		else if (AMQPConstants.DESTINATION_TYPE_TOPIC_EXCHANGE.equals(value)) return AMQPConstants.TOPIC_EXCHANGE;
		else if (AMQPConstants.DESTINATION_TYPE_FANOUT_EXCHANGE.equals(value)) return AMQPConstants.FANOUT_EXCHANGE;
		else throw new AxisFault("Invalid destinaton type value " + value);
	}

	public static String destination_type_to_param(int value) {
		if (AMQPConstants.QUEUE==value) 
			return AMQPConstants.DESTINATION_TYPE_QUEUE;
		else if (AMQPConstants.DIRECT_EXCHANGE==value) 
			return AMQPConstants.DESTINATION_TYPE_DIRECT_EXCHANGE;
		else if (AMQPConstants.TOPIC_EXCHANGE==value) 
			return AMQPConstants.DESTINATION_TYPE_TOPIC_EXCHANGE;
		else if (AMQPConstants.FANOUT_EXCHANGE==value) 
			return AMQPConstants.DESTINATION_TYPE_FANOUT_EXCHANGE;
		return null;
	}
}
