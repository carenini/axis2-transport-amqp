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

import org.apache.axis2.AxisFault;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.axis2.transport.amqp.common.AMQPConnectionFactory;
import org.apache.axis2.transport.amqp.common.AMQPConstants;
import org.apache.axis2.transport.amqp.common.Destination;
import org.apache.axis2.transport.amqp.common.DestinationFactory;
import org.apache.axis2.transport.base.ProtocolEndpoint;
import org.apache.axis2.transport.base.threads.WorkerPool;
import org.apache.axis2.addressing.EndpointReference;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;


/**
 * Class that links an Axis2 service to a AMQP destination. Additionally, it contains
 * all the required information to process incoming AMQP messages and to inject them
 * into Axis2.
 */
public class AMQPEndpoint extends ProtocolEndpoint {
    private static final Log log = LogFactory.getLog(AMQPEndpoint.class);
    
    private AMQPListener amqp_listener;
    private final WorkerPool workerPool;
    
    private AMQPConnectionFactory cf;
    
    private Destination sink=null;
    private Destination source=null;
    
    private Set<EndpointReference> endpointReferences = new HashSet<EndpointReference>();
    private ServiceTaskManager serviceTaskManager;

    public AMQPEndpoint(AMQPListener listener, WorkerPool workerPool) {
        this.amqp_listener = listener;
        this.workerPool = workerPool;
    }

    @Override
    public EndpointReference[] getEndpointReferences(String ip) {
        return endpointReferences.toArray(new EndpointReference[endpointReferences.size()]);
    }

    private void computeEPRs() {
        List<EndpointReference> eprs = new ArrayList<EndpointReference>();
        for (Object o : getService().getParameters()) {
            Parameter p = (Parameter) o;
            if (AMQPConstants.PARAM_PUBLISH_EPR.equals(p.getName()) && p.getValue() instanceof String) {
                if ("legacy".equalsIgnoreCase((String) p.getValue())) {
                    // if "legacy" specified, compute and replace it
                    endpointReferences.add(new EndpointReference(getEPR()));
                } else {
                    endpointReferences.add(new EndpointReference((String) p.getValue()));
                }
            }
        }

        if (eprs.isEmpty()) {
            // if nothing specified, compute and return legacy EPR
            endpointReferences.add(new EndpointReference(getEPR()));
        }
    }

    /**
     * Get the EPR for the given AMQP connection factory and destination
     * the form of the URL is
     * amqp:/<destination>?[<key>=<value>&]*
     *
     * @return the EPR as a String
     */
    private String getEPR() {
        StringBuffer sb = new StringBuffer();

        sb.append(AMQPConstants.AMQP_PREFIX).append(sink.getName());
        sb.append("?").append(AMQPConstants.PARAM_DEST_TYPE).append("=").append(Destination.destination_type_to_param(source.getType()));
            
       	for (Map.Entry<String, String> entry : cf.getParameters().entrySet()) {
			sb.append("&").append(entry.getKey()).append("=").append(entry.getValue());
        }
        return sb.toString();
    }

    public AMQPConnectionFactory getConnectionFactory() {
        return cf;
    }

    public ServiceTaskManager getServiceTaskManager() {
        return serviceTaskManager;
    }

    public void setServiceTaskManager(ServiceTaskManager serviceTaskManager) {
        this.serviceTaskManager = serviceTaskManager;
    }

    @Override
    public boolean loadConfiguration(ParameterInclude params) throws AxisFault {
    	AxisService service = null;
    	Parameter destParam = null;
    	Parameter replyParam = null;
    	Parameter destTypeParam = null;
    	Parameter replyDestTypeParam = null;
    	String sourceName=null;
        int sourceType=0;
        String sinkName=null;
        int sinkType=0;
        
    	// We only support endpoints configured at service level
        if (!(params instanceof AxisService)) return false;
               
        service=(AxisService)params;
        cf = amqp_listener.getConnectionFactory(service);
        if (cf == null) return false;
        
        destParam=service.getParameter(AMQPConstants.PARAM_DESTINATION);
        
        /* SINK = DESTINATION */
		if (destParam != null) sinkName = (String)destParam.getValue();
        else sinkName = service.getName();
        
        destTypeParam=service.getParameter(AMQPConstants.PARAM_DEST_TYPE);
        if (destTypeParam != null) {
            String paramValue = (String) destTypeParam.getValue();
            sinkType=Destination.param_to_destination_type(paramValue);
        } else {
            log.debug("AMQP destination type not given. default queue");
            sinkType = AMQPConstants.QUEUE;
        }

        /* SOURCE = REPLY_TO */
        replyParam=service.getParameter(AMQPConstants.PARAM_REPLY_DESTINATION);
        
		if (replyParam != null) sourceName = (String)replyParam.getValue();
        else sourceName = service.getName();
         
        replyDestTypeParam=service.getParameter(AMQPConstants.PARAM_REPLY_DEST_TYPE);
        if (replyDestTypeParam != null) {
            String paramValue = (String) replyDestTypeParam.getValue();
            sourceType=Destination.param_to_destination_type(paramValue);
        } else {
            log.debug("AMQP reply destination type not given. default queue");
            sourceType = AMQPConstants.QUEUE;
        }
        
        // compute service EPR and keep for later use
        computeEPRs();         
        
        serviceTaskManager = ServiceTaskManagerFactory.createTaskManagerForService(cf, service, workerPool);

        if (sourceType==AMQPConstants.QUEUE) source=DestinationFactory.queueDestination(sourceName);
        else source=DestinationFactory.exchangeDestination(sourceName, sourceType, null);
        
        if (sinkType==AMQPConstants.QUEUE) sink=DestinationFactory.queueDestination(sinkName);
        else sink=DestinationFactory.exchangeDestination(sinkName, sinkType, null);
        
        return true;
    }

 	public String getReplyDestinationAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	public AMQPListener getAmqpListener() {
		return amqp_listener;
	}

	public void setAmqpListener(AMQPListener amqp_listener) {
		this.amqp_listener = amqp_listener;
	}

	public Destination getSink() {
		return sink;
	}

	public void setSink(Destination sink) {
		this.sink = sink;
	}

	public Destination getSource() {
		return source;
	}

	public void setSource(Destination source) {
		this.source = source;
	}

}
