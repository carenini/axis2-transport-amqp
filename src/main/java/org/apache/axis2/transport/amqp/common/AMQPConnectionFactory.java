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

import java.util.Hashtable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterIncludeImpl;
import org.apache.axis2.AxisFault;
import org.apache.axiom.om.OMElement;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;

import java.util.Hashtable;

/**
 * Encapsulate an AMQP Connection factory definition within an Axis2.xml
 *
 * JMS Connection Factory definitions, allows JNDI properties as well as other service
 * level parameters to be defined, and re-used by each service that binds to it
 *
 * When used for sending messages out, the JMSConnectionFactory'ies are able to cache
 * a Connection, Session or Producer
 */
public class AMQPConnectionFactory {

    private static final Log log = LogFactory.getLog(AMQPConnectionFactory.class);
    private CachingConnectionFactory confac=null;
    private Hashtable<String, String> parameters = new Hashtable<String, String>();
	private String name;

    /**
     * Digest a JMS CF definition from an axis2.xml 'Parameter' and construct
     * @param parameter the axis2.xml 'Parameter' that defined the JMS CF
     */
    public AMQPConnectionFactory(Parameter parameter) {
        confac=new CachingConnectionFactory();

        this.name = parameter.getName();
        ParameterIncludeImpl pi = new ParameterIncludeImpl();

        try {
            pi.deserializeParameters((OMElement) parameter.getValue());
        } catch (AxisFault axisFault) {
            handleException("Error reading parameters for JMS connection factory" + name, axisFault);
        }

        for (Object o : pi.getParameters()) {
            Parameter p = (Parameter) o;
            parameters.put(p.getName(), (String) p.getValue());
        }

        //TODO: get the parameters from CloudFoundry
        confac.setHost(parameters.get(AMQPConstants.PARAM_AMQP_HOST));
        confac.setPassword(parameters.get(AMQPConstants.PARAM_AMQP_PASSWORD)); 
        confac.setPort(Integer.parseInt(parameters.get(AMQPConstants.PARAM_AMQP_PORT))); 
        confac.setUsername(parameters.get(AMQPConstants.PARAM_AMQP_USERNAME)); 
        confac.setVirtualHost(parameters.get(AMQPConstants.PARAM_AMQP_VHOST));
		log.info("AMQP ConnectionFactory : " + name + " initialized");

    }

    /**
     * Return the name assigned to this JMS CF definition
     * @return name of the JMS CF
     */
    public String getName() {
        return name;
    }

    /**
     * The list of properties (including JNDI and non-JNDI)
     * @return properties defined on the JMS CF
     */
    public Hashtable<String, String> getParameters() {
        return parameters;
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new AxisAMQPException(msg, e);
    }

    /**
     * Get a new Connection or shared Connection from this JMS CF
     * @return new or shared Connection from this JMS CF
     */
    public Connection getConnection() {
    	return confac.createConnection();
    }

	
}
