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
package org.apache.axis2.transport.amqp.in;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.transport.amqp.common.AMQPConnectionFactory;
import org.apache.axis2.transport.amqp.common.AMQPConstants;
import org.apache.axis2.transport.amqp.common.Destination;
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

}
