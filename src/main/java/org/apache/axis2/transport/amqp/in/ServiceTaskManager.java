package org.apache.axis2.transport.amqp.in;

import java.io.IOException;

import org.apache.axis2.transport.amqp.common.AMQPConnectionFactory;
import org.apache.axis2.transport.amqp.common.AMQPConstants;
import org.apache.axis2.transport.amqp.common.AMQPUtils;
import org.apache.axis2.transport.amqp.common.Destination;
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
    private AMQPConnectionFactory confac=null;
    private Connection sharedConnection = null;
	private Destination dest=null;

	
	public ServiceTaskManager(String service_name, AMQPConnectionFactory cf, Destination in_dest, WorkerPool wp) {
		serviceName=service_name;
		workerPool=wp;
		dest=in_dest;
		confac=cf;
	}

	public int getDestinationType() {
		return dest.getType();
	}

	/*=============== Thread management methods ===============*/
	public void start() {
		message_consumer=new Consumer(chan, workerPool);
		String queue_name=null;
		String exchange_name=null;
		String routing_key=null;

		if (serviceTaskManagerState == STATE_PAUSED) {
			log.info("Attempt to re-start paused TaskManager is ignored. Please use resume instead");
			return;
		}

		try {
			sharedConnection=confac.getConnection();
			chan=sharedConnection.createChannel();
			if (dest.getType()==AMQPConstants.QUEUE) {
				queue_name=dest.getName();
				if (! check_destination(chan, dest)) {		
					chan.queueDeclare(queue_name, false, false, true, null);
					log.debug("Created queue "+queue_name);
				}
			}
			else {
				exchange_name=dest.getName();
				// DeclarePassive does not work as expected, this is a workaround
				if (! check_destination(chan, dest)) {
					chan.exchangeDeclare(exchange_name, Destination.destination_type_to_param(dest.getType()));
					log.debug("Created exchange "+exchange_name);
				}
				routing_key=dest.getRoutingKey();
				// If routing key is not present, subscribe to everything
				if ((routing_key==null)&&(dest.getType()==AMQPConstants.TOPIC_EXCHANGE)) routing_key="#";
				else if (routing_key==null) routing_key="";

				queue_name = chan.queueDeclare().getQueue();
				log.debug("Created queue "+queue_name);
				chan.queueBind(queue_name, dest.getName(), routing_key);
			}

			//FIXME what's the consumer tag?
			chan.basicConsume(queue_name, true, "myConsumerTag", message_consumer);

		} catch (IOException e) {
			e.printStackTrace();
		}
		serviceTaskManagerState = STATE_STARTED;
		log.info("Task manager for service : " + serviceName + " [re-]initialized");
		log.info("Started to listen on destination : " + dest.getName() + 
				" of type " + AMQPUtils.getDestinationTypeAsString(dest.getType()) + 
				" for service " + serviceName);

	}
	/** DeclarePassive does not work as expected, this is a workaround */
	private boolean check_destination(Channel c, Destination d) {
		boolean res=true;
		try {
			if (d.getType()==AMQPConstants.QUEUE) {
				chan.queueDeclarePassive(d.getName());
			}
			else {
				chan.exchangeDeclarePassive(d.getName());			
			}
		} catch (IOException e) {
			res=false;
		}
		try {
			if (! chan.isOpen())
				chan=sharedConnection.createChannel();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return res;

	}
	
	public void stop() {
		log.debug("Stopping ServiceTaskManager for service : " + serviceName);
        if (serviceTaskManagerState != STATE_FAILURE) {
            serviceTaskManagerState = STATE_SHUTTING_DOWN;
        }

        if (sharedConnection != null) {
            try {
            	chan.close();
                sharedConnection.close();      
            } catch (IOException e) {    
                log.error("Error stopping shared Connection", e);
                serviceTaskManagerState= STATE_FAILURE;
            } finally {
                sharedConnection = null;       
            }
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

	public Connection getSharedConnection() {
		return sharedConnection;
	}

	public void setSharedConnection(Connection sharedConnection) {
		this.sharedConnection = sharedConnection;
	}

}
