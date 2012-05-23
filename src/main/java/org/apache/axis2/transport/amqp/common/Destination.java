package org.apache.axis2.transport.amqp.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rabbitmq.client.Connection;


/** sample uri formats - this is temporary until the AMQP WG defines a proper addressing scheme
*
* uri="amqp:/direct/amq.direct?transport.amqp.RoutingKey=SimpleStockQuoteService&amp;transport.amqp.ConnectionURL=qpid:virtualhost=test;client_id=foo@tcp:myhost.com:5672"
* amqp:/topic/amq.topic?transport.amqp.RoutingKey=weather.us.ny&amp;transport.amqp.ConnectionURL=qpid:virtualhost=test;client_id=foo@tcp:myhost.com:5672
*/
public class Destination {

	private String name=null;
    private String exchangeName = "amq.direct";
    private String exchangeType = "direct";
    private String routingKey;
    private String queue=null;
    private int type=AMQPConstants.QUEUE;
    private boolean primary=false;
    
    public Destination(){
    }

    public Destination(String queue_name) {
    	this.queue=queue_name;
    	name=queue;
    	type=AMQPConstants.QUEUE;
    }
    public Destination(String exchangeName, String exchangeType, String routingKey, boolean primary)
    {
        super();
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.routingKey = routingKey;
        this.setPrimary(primary);
        name=exchangeName;
        this.type=AMQPConstants.EXCHANGE;
    }

    public String getExchangeName()
    {
        return exchangeName;
    }
    public void setExchangeName(String exchangeName)
    {
        this.exchangeName = exchangeName;
    }
    public String getExchangeType()
    {
        return exchangeType;
    }
    public void setExchangeType(String exchangeType)
    {
        this.exchangeType = exchangeType;
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
	
    public static Map parse(String uri){
        Map props = new HashMap();
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

     /**
      * Get the EPR for the given AMQP details
      * the form of the URL is
      * uri="amqp:/direct?routingKey=SimpleStockQuoteService&amp;connectionURL=qpid:virtualhost=test;client_id=foo@tcp:myhost.com:5672"
      *
      * Creates the EPR with the primary binding
      *
      */
     public static String getEPR(List<Destination> list, String url) {

         String epr = null;
         for (Destination binding:list){

             if (binding.isPrimary()){
                 StringBuffer sb = new StringBuffer();
                 sb.append(AMQPConstants.AMQP_PREFIX).append("/").append(binding.getExchangeType());
                 sb.append("/").append(binding.getExchangeName());
                 sb.append("?").append(AMQPConstants.BINDING_ROUTING_KEY_ATTR).append("=").append(binding.getRoutingKey());
                 sb.append("&amp;").append("connectionURL=").append(url);
                 epr = sb.toString();
             }
         }

         // If no primary is defined just get the first
         if(epr == null){
             Destination binding = list.get(0);
             StringBuffer sb = new StringBuffer();
             sb.append(AMQPConstants.AMQP_PREFIX).append("/").append(binding.getExchangeType());
             sb.append("/").append(binding.getExchangeName());
             sb.append("?").append(AMQPConstants.BINDING_ROUTING_KEY_ATTR).append("=").append(binding.getRoutingKey());
             sb.append("&amp;").append("connectionURL=").append(url);
             epr = sb.toString();
         }

         return epr;
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


}
