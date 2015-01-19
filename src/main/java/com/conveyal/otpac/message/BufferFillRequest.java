package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.Collection;

/**
 * A request from a WorkerManager to an Executive to fill its buffer.
 */
public class BufferFillRequest implements Serializable {
	/**
	 * The number of cluster requests that are requested. Note that less may be
	 * sent back if the queue is nearly dry.
	 */
	public int size;
	
	/** The routerId available on the workermanager */
	public String routerId;
	
	public BufferFillRequest(int size, String id) {
		this.size = size;
		this.routerId = id;
	}
}
