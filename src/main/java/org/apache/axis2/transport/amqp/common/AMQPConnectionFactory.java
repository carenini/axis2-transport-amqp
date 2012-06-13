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

import java.io.IOException;
import java.util.Hashtable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterIncludeImpl;
import org.apache.axis2.AxisFault;
import org.apache.axiom.om.OMElement;
import org.cloudfoundry.runtime.env.CloudEnvironment;
import org.cloudfoundry.runtime.env.RabbitServiceInfo;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Encapsulate an AMQP Connection factory definition within an Axis2.xml
 *
 * AMQP Connection Factory definitions, allows JNDI properties as well as other service
 * level parameters to be defined, and re-used by each service that binds to it
 *
 * When used for sending messages out, the AMQPConnectionFactory'ies are able to cache
 * a Connection, Session or Producer
 */
public class AMQPConnectionFactory {

    private static final Log log = LogFactory.getLog(AMQPConnectionFactory.class);
    private ConnectionFactory confac=null;
    private Hashtable<String, String> parameters = new Hashtable<String, String>();
	private String name;

    /**
     * Digest a AMQP CF definition from an axis2.xml 'Parameter' and construct
     * @param parameter the axis2.xml 'Parameter' that defined the AMQP CF
     */
    public AMQPConnectionFactory(Parameter parameter) {
        confac=new ConnectionFactory();

        this.name = parameter.getName();
        ParameterIncludeImpl pi = new ParameterIncludeImpl();

        try {
            pi.deserializeParameters((OMElement) parameter.getValue());
        } catch (AxisFault axisFault) {
            handleException("Error reading parameters for AMQP connection factory" + name, axisFault);
        }

        for (Object o : pi.getParameters()) {
            Parameter p = (Parameter) o;
            parameters.put(p.getName(), (String) p.getValue());
        }

        log.info("Connection factory parameters: "+parameters);
        if (AMQPConstants.EXECUTION_CLOUD.equals(parameters.get(AMQPConstants.PARAM_EXECUTION_ENV))){
        	CloudEnvironment environment= new CloudEnvironment();
        	String cf_servicename=parameters.get(AMQPConstants.PARAM_AMQP_SERVICENAME);
        	RabbitServiceInfo service = environment.getServiceInfo(cf_servicename,RabbitServiceInfo.class);
        	if (service==null) 
        		log.error("Cannot retrieve CF service "+cf_servicename);
        	log.info("Initialising AMQP ConnectionFactory : " + name + " using CloudFoundry data: "+service);
        	confac.setHost(service.getHost());
        	confac.setPort(service.getPort());
        	confac.setUsername(service.getUserName());
        	confac.setPassword(service.getPassword());
        	confac.setVirtualHost(service.getVirtualHost());
        }
        else {
        	confac.setHost(parameters.get(AMQPConstants.PARAM_AMQP_HOST));
        	confac.setPassword(parameters.get(AMQPConstants.PARAM_AMQP_PASSWORD)); 
            confac.setPort(Integer.parseInt(parameters.get(AMQPConstants.PARAM_AMQP_PORT))); 
            confac.setUsername(parameters.get(AMQPConstants.PARAM_AMQP_USERNAME)); 
            confac.setVirtualHost(parameters.get(AMQPConstants.PARAM_AMQP_VHOST));
        }
        
		log.info("AMQP ConnectionFactory : " + name + " initialized");

    }

    /**
     * Return the name assigned to this AMQP CF definition
     * @return name of the AMQP CF
     */
    public String getName() {
        return name;
    }

    /**
     * The list of properties (including JNDI and non-JNDI)
     * @return properties defined on the AMQP CF
     */
    public Hashtable<String, String> getParameters() {
        return parameters;
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new AxisAMQPException(msg, e);
    }

    /**
     * Get a new Connection from this AMQP CF
     * @return new or shared Connection from this AMQP CF
     * @throws IOException 
     */
    public Connection getConnection() throws IOException {
    	return confac.newConnection();
    }

	
}
