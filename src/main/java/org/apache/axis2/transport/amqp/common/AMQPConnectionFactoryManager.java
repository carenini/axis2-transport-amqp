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
import java.util.Map;

import javax.naming.Context;

import org.apache.axis2.AxisFault;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class managing a set of {@link AMQPConnectionFactory} objects.
 */
public class AMQPConnectionFactoryManager {

    private static final Log log = LogFactory.getLog(AMQPConnectionFactoryManager.class);

    /** A Map containing the AMQP connection factories managed by this, keyed by name */
    private final Map<String,AMQPConnectionFactory> connectionFactories =new HashMap<String,AMQPConnectionFactory>();

    /**
     * Construct a Connection factory manager for the AMQP transport sender or receiver
     * @param trpInDesc
     */
    public AMQPConnectionFactoryManager(ParameterInclude trpInDesc) {
        loadConnectionFactoryDefinitions(trpInDesc);
    }

    /**
     * Create AMQPConnectionFactory instances for the definitions in the transport configuration,
     * and add these into our collection of connectionFactories map keyed by name
     *
     * @param trpDesc the transport description for AMQP
     */
    private void loadConnectionFactoryDefinitions(ParameterInclude trpDesc) {
    	log.info("Connection Factory Parameters: "+trpDesc);
        for (Parameter p : trpDesc.getParameters()) {
            try {
                AMQPConnectionFactory amqpConFactory = new AMQPConnectionFactory(p);
                connectionFactories.put(amqpConFactory.getName(), amqpConFactory);
            } catch (AxisAMQPException e) {
                log.error("Error setting up connection factory : " + p.getName(), e);
            }
        }
    }

    /**
     * Get the AMQP connection factory with the given name.
     *
     * @param name the name of the AMQP connection factory
     * @return the AMQP connection factory or null if no connection factory with
     *         the given name exists
     */
    public AMQPConnectionFactory getAMQPConnectionFactory(String name) {
        return connectionFactories.get(name);
    }

    /**
     * Get the AMQP connection factory that matches the given properties, i.e. referring to
     * the same underlying connection factory. Used by the AMQPSender to determine if already
     * available resources should be used for outgoing messages
     *
     * @param props a Map of connection factory JNDI properties and name
     * @return the AMQP connection factory or null if no connection factory compatible
     *         with the given properties exists
     */
    public AMQPConnectionFactory getAMQPConnectionFactory(Map<String,String> props) {
        for (AMQPConnectionFactory cf : connectionFactories.values()) {
            Map<String,String> cfProperties = cf.getParameters();

            if (equals(props.get(AMQPConstants.PARAM_AMQP_CONFAC),
                cfProperties.get(AMQPConstants.PARAM_AMQP_CONFAC))
                &&
                equals(props.get(Context.PROVIDER_URL),
                    cfProperties.get(Context.PROVIDER_URL))
                &&
                equals(props.get(Context.SECURITY_PRINCIPAL),
                    cfProperties.get(Context.SECURITY_PRINCIPAL))
                &&
                equals(props.get(Context.SECURITY_CREDENTIALS),
                    cfProperties.get(Context.SECURITY_CREDENTIALS))) {
                return cf;
            }
        }
        return null;
    }

    /**
     * Compare two values preventing NPEs
     */
    private static boolean equals(Object s1, Object s2) {
        return s1 == s2 || s1 != null && s1.equals(s2);
    }

    protected void handleException(String msg, Exception e) throws AxisFault {
        log.error(msg, e);
        throw new AxisFault(msg, e);
    }

}
