package org.apache.axis2.transport.amqp.common;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.axis2.description.AxisService;
import org.apache.axis2.transport.base.threads.WorkerPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    
    private Consumer message_consumer=null;
    
    /**
     * The AMQP Connection shared between multiple polling tasks - when enabled
     * (reccomended)
     */
    private Connection sharedConnection = null;
	private Destination dest=null;
	private Set<String> consumers=null;
	
	public ServiceTaskManager() {
		consumers=new HashSet<String>();
		message_consumer=new Consumer(null, null);
	}
	
	public ServiceTaskManager(String service_name) {
		this();
		serviceName=service_name;
		
	}
	
	public int getActiveTaskCount() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int getDestinationType() {
		return dest.getType();
	}

	public void setMessageReceiver(AMQPMessageReceiver amqpMessageReceiver) {
		// TODO Auto-generated method stub
		
	}
	
	/*=============== Thread management methods ===============*/
	public void start() {
        if (serviceTaskManagerState == STATE_PAUSED) {
            log.info("Attempt to re-start paused TaskManager is ignored. Please use resume instead");
            return;
        }

        // if any tasks are running, stop whats running now
        if (!pollingTasks.isEmpty()) {
            stop();
        }

        for (int i = 0; i < concurrentConsumers; i++) {
            workerPool.execute(new MessageListenerTask());
        }

        serviceTaskManagerState = STATE_STARTED;
        log.info("Task manager for service : " + serviceName + " [re-]initialized");

	}
	
	public void stop() {
		log.debug("Stopping ServiceTaskManager for service : " + serviceName);
        if (serviceTaskManagerState != STATE_FAILURE) {
            serviceTaskManagerState = STATE_SHUTTING_DOWN;
        }
/*
        synchronized (consumers=null) {  
            for (String lstTask : consumers) {
                     
            }
        }
*/
        // try to wait a bit for task shutdown
        for (int i = 0; i < 5; i++) {  
            if (activeTaskCount == 0) {    
                break;        
            }
            try {
                Thread.sleep(1000);            
            } catch (InterruptedException ignore) {
            }
        }

        if (sharedConnection != null) {
            try {
                sharedConnection.close();      
            } catch (IOException e) {    
                log.error("Error stopping shared Connection", e);
            } finally {
                sharedConnection = null;       
            }
        }

        if (activeTaskCount > 0) {     
            log.warn("Unable to shutdown all polling tasks of service : " + serviceName);
        }

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








}
