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

import org.apache.axis2.transport.amqp.common.AMQPConnectionFactory;
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
	public static ServiceTaskManager createTaskManagerForService(AMQPConnectionFactory jcf, AMQPEndpoint ep, String service_name, Destination in_dest, WorkerPool workerPool) {
		ServiceTaskManager stm = new ServiceTaskManager();
		stm.setEndpoint(ep);
		stm.setServiceName(service_name);
		stm.setDestination(in_dest);
		stm.setWorkerPool(workerPool);

		return stm;
	}

}
