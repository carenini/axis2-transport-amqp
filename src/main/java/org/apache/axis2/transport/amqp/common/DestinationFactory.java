package org.apache.axis2.transport.amqp.common;

import java.util.Map;

import org.apache.axis2.AxisFault;

public class DestinationFactory {
	
	public static Destination queueDestination(String name){
		Destination d=new Destination();
		d.setType(AMQPConstants.QUEUE);
		d.setName(name);
		return d;
	}

	public static Destination exchangeDestination(String exchangeName, int exchangeType, String routingKey) {
		Destination d=new Destination();
		d.setType(exchangeType);
		d.setName(exchangeName);
		return d;
	}
	
	public static Destination parseAddress(String addr) throws AxisFault {
		Map<String, String> address_props = null;
	
		address_props=Destination.parse(addr);
		return exchangeDestination(address_props.get(AMQPConstants.EXCHANGE_NAME_PARAM),
				Destination.param_to_destination_type(address_props.get(AMQPConstants.EXCHANGE_TYPE_PARAM)),
				null);
	}
}
