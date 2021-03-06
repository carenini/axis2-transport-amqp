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
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.TransportInDescription;
import org.apache.axis2.transport.amqp.common.AMQPConnectionFactory;
import org.apache.axis2.transport.amqp.common.AMQPConnectionFactoryManager;
import org.apache.axis2.transport.amqp.common.AMQPConstants;
import org.apache.axis2.transport.amqp.common.AxisAMQPException;
import org.apache.axis2.transport.base.AbstractTransportListenerEx;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.ManagementSupport;
import org.apache.axis2.transport.base.event.TransportErrorListener;
import org.apache.axis2.transport.base.event.TransportErrorSource;
import org.apache.axis2.transport.base.event.TransportErrorSourceSupport;

/**
 * The revamped AMQP Transport listener implementation. Creates
 * {@link ServiceTaskManager} instances for each service requesting exposure
 * over AMQP, and stops these if they are undeployed / stopped.
 * <p>
 * A service indicates a AMQP Connection factory definition by name, which would
 * be defined in the AMQPListner on the axis2.xml, and this provides a way to
 * reuse common configuration between services, as well as to optimize resources
 * utilized
 * <p>
 * If the connection factory name was not specified, it will default to the one
 * named "default" {@see AMQPConstants.DEFAULT_CONFAC_NAME}
 * <p>
 * If a destination JNDI name is not specified, a service will expect to use a
 * Queue with the same JNDI name as of the service. Additional Parameters allows
 * one to bind to a Topic or specify many more detailed control options. See
 * package documentation for more details
 * <p>
 * All Destinations / AMQP Administered objects used MUST be pre-created or
 * already available
 */
public class AMQPListener extends AbstractTransportListenerEx<AMQPEndpoint> implements ManagementSupport, TransportErrorSource {

	public static final String TRANSPORT_NAME = "amqp";

	/**
	 * The AMQPConnectionFactoryManager which centralizes the management of
	 * defined factories
	 */
	private AMQPConnectionFactoryManager connFacManager;

	private final TransportErrorSourceSupport tess = new TransportErrorSourceSupport(this);

	/**
	 * TransportListener initialization
	 * 
	 * @param cfgCtx
	 *            the Axis configuration context
	 * @param trpInDesc
	 *            the TransportIn description
	 */
	@Override
	public void init(ConfigurationContext cfgCtx, TransportInDescription trpInDesc) throws AxisFault {

		super.init(cfgCtx, trpInDesc);
		connFacManager = new AMQPConnectionFactoryManager(trpInDesc);
		log.info("AMQP Transport Receiver/Listener initialized...");
	}

	@Override
	protected AMQPEndpoint createEndpoint() {
		return new AMQPEndpoint(this, workerPool);
	}

	/**
	 * Listen for AMQP messages on behalf of the given service
	 * 
	 * @param service
	 *            the Axis service for which to listen for messages
	 */
	@Override
	protected void startEndpoint(AMQPEndpoint endpoint) throws AxisFault {
		ServiceTaskManager stm = endpoint.getServiceTaskManager();
		stm.start();
	}

	/**
	 * Stops listening for messages for the service thats undeployed or stopped
	 * 
	 * @param service
	 *            the service that was undeployed or stopped
	 */
	@Override
	protected void stopEndpoint(AMQPEndpoint endpoint) {
		ServiceTaskManager stm = endpoint.getServiceTaskManager();
		log.debug("Stopping listening on destination : " + stm.getDestination().getName() + " for service : " + stm.getServiceName());

		stm.stop();

		log.info("Stopped listening for AMQP messages to service : " + endpoint.getServiceName());
	}

	/**
	 * Return the connection factory name for this service. If this service
	 * refers to an invalid factory or defaults to a non-existent default
	 * factory, this returns null
	 * 
	 * @param service
	 *            the AxisService
	 * @return the AMQPConnectionFactory to be used, or null if reference is
	 *         invalid
	 */
	public AMQPConnectionFactory getConnectionFactory(AxisService service) {

		Parameter conFacParam = service.getParameter(AMQPConstants.PARAM_AMQP_CONFAC);
		// validate connection factory name (specified or default)
		if (conFacParam != null) {
			return connFacManager.getAMQPConnectionFactory((String) conFacParam.getValue());
		} else {
			return connFacManager.getAMQPConnectionFactory(AMQPConstants.DEFAULT_CONFAC_NAME);
		}
	}

	// -- jmx/management methods--
	/**
	 * Pause the listener - Stop accepting/processing new messages, but
	 * continues processing existing messages until they complete. This helps
	 * bring an instance into a maintenence mode
	 * 
	 * @throws AxisFault
	 *             on error
	 */
	@Override
	public void pause() throws AxisFault {
		if (state != BaseConstants.STARTED)
			return;
		try {
			for (AMQPEndpoint endpoint : getEndpoints()) {
				endpoint.getServiceTaskManager().pause();
			}
			state = BaseConstants.PAUSED;
			log.info("Listener paused");
		} catch (AxisAMQPException e) {
			log.error("At least one service could not be paused", e);
		}
	}

	/**
	 * Resume the lister - Brings the lister into active mode back from a paused
	 * state
	 * 
	 * @throws AxisFault
	 *             on error
	 */
	@Override
	public void resume() throws AxisFault {
		if (state != BaseConstants.PAUSED)
			return;
		try {
			for (AMQPEndpoint endpoint : getEndpoints()) {
				endpoint.getServiceTaskManager().resume();
			}
			state = BaseConstants.STARTED;
			log.info("Listener resumed");
		} catch (AxisAMQPException e) {
			log.error("At least one service could not be resumed", e);
		}
	}

	/**
	 * Stop processing new messages, and wait the specified maximum time for
	 * in-flight requests to complete before a controlled shutdown for
	 * maintenence
	 * 
	 * @param millis
	 *            a number of milliseconds to wait until pending requests are
	 *            allowed to complete
	 * @throws AxisFault
	 *             on error
	 */
	@Override
	public void maintenenceShutdown(long millis) throws AxisFault {
		if (state != BaseConstants.STARTED)
			return;
		try {
			long start = System.currentTimeMillis();
			stop();
			state = BaseConstants.STOPPED;
			log.info("Listener shutdown in : " + (System.currentTimeMillis() - start) / 1000 + "s");
		} catch (Exception e) {
			handleException("Error shutting down the listener for maintenence", e);
		}
	}

	public void addErrorListener(TransportErrorListener listener) {
		tess.addErrorListener(listener);
	}

	public void removeErrorListener(TransportErrorListener listener) {
		tess.removeErrorListener(listener);
	}

	void error(AxisService service, Throwable ex) {
		tess.error(service, ex);
	}
}
