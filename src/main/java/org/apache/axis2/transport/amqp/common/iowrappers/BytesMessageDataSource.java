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
package org.apache.axis2.transport.amqp.common.iowrappers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.axiom.attachments.SizeAwareDataSource;
import org.apache.axis2.transport.amqp.common.AMQPMessage;

/**
 * Data source implementation wrapping a JMS {@link BytesMessage}.
 * <p>
 * Note that two input streams created by the same instance of this class can
 * not be used at the same time.
 */
public class BytesMessageDataSource implements SizeAwareDataSource {
	private final AMQPMessage message;
	/**
	 * @uml.property name="contentType"
	 */
	private final String contentType;

	public BytesMessageDataSource(AMQPMessage message, String contentType) {
		this.message = message;
		this.contentType = contentType;
	}

	public BytesMessageDataSource(AMQPMessage message) {
		this(message, "application/octet-stream");
	}

	public long getSize() {
		return message.getBody().length;
	}

	public String getContentType() {
		return contentType;
	}

	public InputStream getInputStream() throws IOException {
			message.reset();
		return new BytesMessageInputStream(message);
	}

	public String getName() {
		return null;
	}

	public OutputStream getOutputStream() throws IOException {
		throw new UnsupportedOperationException();
	}
}
