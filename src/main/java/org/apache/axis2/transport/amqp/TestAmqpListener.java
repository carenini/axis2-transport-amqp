package org.apache.axis2.transport.amqp;

import java.util.Map;

import org.apache.axis2.AxisFault;
import org.apache.axis2.transport.base.AbstractTransportListenerEx;
import org.apache.axis2.transport.base.ManagementSupport;
import org.apache.axis2.transport.base.event.TransportErrorListener;
import org.apache.axis2.transport.base.event.TransportErrorSource;

public class TestAmqpListener extends AbstractTransportListenerEx<E> implements ManagementSupport, TransportErrorSource {

	public void addErrorListener(TransportErrorListener listener) {
		// TODO Auto-generated method stub

	}

	public void removeErrorListener(TransportErrorListener listener) {
		// TODO Auto-generated method stub

	}

	public void pause() throws AxisFault {
		// TODO Auto-generated method stub

	}

	public void resume() throws AxisFault {
		// TODO Auto-generated method stub

	}

	public void maintenenceShutdown(long millis) throws AxisFault {
		// TODO Auto-generated method stub

	}

	public int getActiveThreadCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getQueueSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMessagesReceived() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getFaultsReceiving() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getTimeoutsReceiving() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMessagesSent() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getFaultsSending() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getTimeoutsSending() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getBytesReceived() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getBytesSent() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMinSizeReceived() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMaxSizeReceived() {
		// TODO Auto-generated method stub
		return 0;
	}

	public double getAvgSizeReceived() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMinSizeSent() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMaxSizeSent() {
		// TODO Auto-generated method stub
		return 0;
	}

	public double getAvgSizeSent() {
		// TODO Auto-generated method stub
		return 0;
	}

	public Map getResponseCodeTable() {
		// TODO Auto-generated method stub
		return null;
	}

	public void resetStatistics() {
		// TODO Auto-generated method stub

	}

	public long getLastResetTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMetricsWindow() {
		// TODO Auto-generated method stub
		return 0;
	}

}
