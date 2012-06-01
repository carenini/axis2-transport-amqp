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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.transport.base.threads.WorkerPool;

public class ServiceTaskManagerFactory {
	private ServiceTaskManagerFactory() {
	}

	/**
	 * Create a ServiceTaskManager for the service passed in and its
	 * corresponding JMSConnectionFactory
	 * 
	 * @param jcf
	 * @param service
	 * @param workerPool
	 * @return
	 */
	public static ServiceTaskManager createTaskManagerForService(AMQPConnectionFactory jcf, AxisService service, WorkerPool workerPool) {
		Destination dest=null;
		String name = service.getName();
		Map<String, String> svc = getServiceStringParameters(service.getParameters());
		Map<String, String> cf = jcf.getParameters();

		ServiceTaskManager stm = new ServiceTaskManager();
        Integer value = null;

		stm.setServiceName(name);

		//TODO create the Destination
		
		String destName = getOptionalStringProperty(AMQPConstants.PARAM_DESTINATION, svc, cf);
		if (destName == null) {
			destName = service.getName();
		}


		
		//stm.setMessageSelector(getOptionalStringProperty(AMQPConstants.PARAM_MSG_SELECTOR, svc, cf));

		stm.setPubSubNoLocal(getOptionalBooleanProperty(AMQPConstants.PARAM_PUBSUB_NO_LOCAL, svc, cf));

		value = getOptionalIntProperty(AMQPConstants.PARAM_RECON_INIT_DURATION, svc, cf);
		if (value != null) {
			stm.setInitialReconnectDuration(value);
		}
		value = getOptionalIntProperty(AMQPConstants.PARAM_RECON_MAX_DURATION, svc, cf);
		if (value != null) {
			stm.setMaxReconnectDuration(value);
		}
		Double dValue = getOptionalDoubleProperty(AMQPConstants.PARAM_RECON_FACTOR, svc, cf);
		if (dValue != null) {
			stm.setReconnectionProgressionFactor(dValue);
		}

		stm.setWorkerPool(workerPool);

		return stm;
	}

	private static Map<String, String> getServiceStringParameters(List<Parameter> list) {

		Map<String, String> map = new HashMap<String, String>();
		for (Parameter p : list) {
			if (p.getValue() instanceof String) {
				map.put(p.getName(), (String) p.getValue());
			}
		}
		return map;
	}

	private static String getOptionalStringProperty(String key, Map<String, String> svcMap, Map<String, String> cfMap) {

		String value = svcMap.get(key);
		if (value == null) {
			value = cfMap.get(key);
		}
		return value;
	}

	private static Boolean getOptionalBooleanProperty(String key, Map<String, String> svcMap, Map<String, String> cfMap) {

		String value = svcMap.get(key);
		if (value == null) {
			value = cfMap.get(key);
		}
		if (value == null) {
			return null;
		} else {
			return Boolean.valueOf(value);
		}
	}

	private static Integer getOptionalIntProperty(String key, Map<String, String> svcMap, Map<String, String> cfMap) {

		String value = svcMap.get(key);
		if (value == null) {
			value = cfMap.get(key);
		}
		if (value != null) {
			try {
				return Integer.parseInt(value);
			} catch (NumberFormatException e) {
				throw new AxisAMQPException("Invalid value : " + value + " for " + key);
			}
		}
		return null;
	}

	private static Double getOptionalDoubleProperty(String key, Map<String, String> svcMap, Map<String, String> cfMap) {

		String value = svcMap.get(key);
		if (value == null) {
			value = cfMap.get(key);
		}
		if (value != null) {
			try {
				return Double.parseDouble(value);
			} catch (NumberFormatException e) {
				throw new AxisAMQPException("Invalid value : " + value + " for " + key);
			}
		}
		return null;
	}

}
