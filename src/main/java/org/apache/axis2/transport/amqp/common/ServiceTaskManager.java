package org.apache.axis2.transport.amqp.common;

import java.io.IOException;
import org.apache.axis2.transport.amqp.in.Consumer;
import org.apache.axis2.transport.base.threads.WorkerPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * Each service will have one ServiceTaskManager instance that will create,
 * manage and also destroy idle tasks created for it, for message receipt. This
 * will also allow individual tasks to cache the Connection, Channel or Consumer
 * as necessary, considering the transactionality required and user preference.
 * 
 * This also acts as the ExceptionListener for all JMS connections made on
 * behalf of the service. Since the ExceptionListener is notified by a JMS
 * provider on a "serious" error, we simply try to re-connect. Thus a connection
 * failure for a single task, will re-initialize the state afresh for the
 * service, by discarding all connections.
 */
public class ServiceTaskManager {
	 /** The logger */
    private static final Log log = LogFactory.getLog(ServiceTaskManager.class);
    
    private static final int STATE_STOPPED = 0; /** The Task manager is stopped or has not started */
    private static final int STATE_STARTED = 1; /** The Task manager is started and active */
    private static final int STATE_PAUSED = 2; /** The Task manager is paused temporarily */ 
    private static final int STATE_SHUTTING_DOWN = 3; /** The Task manager is started, but a shutdown has been requested */
    private static final int STATE_FAILURE = 4; /** The Task manager has encountered an error */ 
    
    private volatile int serviceTaskManagerState = STATE_STOPPED; /** State of this Task Manager */
    private String serviceName; /** The name of the service managed by this instance */
    private WorkerPool workerPool = null; /** The shared thread pool from the Listener */
    
	private Channel chan = null; 
    private Consumer message_consumer=null;
    private AMQPEndpoint endpoint=null;
    
    /**
     * The AMQP Connection shared between multiple polling tasks - when enabled
     * (reccomended)
     */
    private Connection sharedConnection = null;
	private Destination dest=null;

	
	public ServiceTaskManager(String service_name, AMQPEndpoint ep) {
		this();
		serviceName=service_name;
		endpoint=ep;
		
	}
	
	public ServiceTaskManager() {
	}

	public int getActiveTaskCount() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int getDestinationType() {
		return dest.getType();
	}

	/*=============== Thread management methods ===============*/
	public void start() {
		message_consumer=new Consumer(endpoint, null, workerPool);
		Destination source=null;
		String queue_name=null;
		String routing_key=null;
		
        if (serviceTaskManagerState == STATE_PAUSED) {
            log.info("Attempt to re-start paused TaskManager is ignored. Please use resume instead");
            return;
        }

        source=endpoint.getSource();
        try {
			chan=sharedConnection.createChannel();
        	if (source.getType()!=AMQPConstants.QUEUE)
        		queue_name=source.getName();
        	else {
        		queue_name = chan.queueDeclare().getQueue();
        		routing_key=source.getRoutingKey();
        		// If routing key is not present, subscribe to everything
        		if ((routing_key==null)&&(source.getType()==AMQPConstants.TOPIC_EXCHANGE)) routing_key="#";
        		else if (routing_key==null) routing_key="";
        		
        		chan.queueBind(queue_name, source.getName(), routing_key);
        	}

			chan.basicConsume(queue_name, true, "myConsumerTag", message_consumer);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
        serviceTaskManagerState = STATE_STARTED;
        log.info("Task manager for service : " + serviceName + " [re-]initialized");

	}
	
	public void stop() {
		log.debug("Stopping ServiceTaskManager for service : " + serviceName);
        if (serviceTaskManagerState != STATE_FAILURE) {
            serviceTaskManagerState = STATE_SHUTTING_DOWN;
        }

        // FIXME perform teardown

        if (sharedConnection != null) {
            try {
                sharedConnection.close();      
            } catch (IOException e) {    
                log.error("Error stopping shared Connection", e);
            } finally {
                sharedConnection = null;       
            }
        }

/*        if (activeTaskCount > 0) {     
            log.warn("Unable to shutdown all polling tasks of service : " + serviceName);
        }*/

        if (serviceTaskManagerState != STATE_FAILURE) {
            serviceTaskManagerState = STATE_STOPPED;
        }
        log.info("Task manager for service : " + serviceName + " shutdown");
	}

	public void pause() {
		// TODO Auto-generated method stub
		
	}

	public void resume() {
		// TODO Auto-generated method stub
		
	}

	/* ================== GETTER / SETTER  ==================*/
	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public Destination getDestination() {
		return dest;
	}

	public void setDestination(Destination dest) {
		this.dest = dest;
	}

	public WorkerPool getWorkerPool() {
		return workerPool;
	}

	public void setWorkerPool(WorkerPool wp) {
		workerPool=wp;
		
	}

	public void setReconnectionProgressionFactor(Double dValue) {
		// TODO Auto-generated method stub
		
	}

	public void setMaxReconnectDuration(Integer value) {
		// TODO Auto-generated method stub
		
	}

	public void setInitialReconnectDuration(Integer value) {
		// TODO Auto-generated method stub
		
	}

	public void setPubSubNoLocal(Boolean optionalBooleanProperty) {
		// TODO Auto-generated method stub
		
	}

	public AMQPEndpoint getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(AMQPEndpoint endpoint) {
		this.endpoint = endpoint;
	}








}
