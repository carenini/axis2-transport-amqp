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

import java.io.OutputStream;

import org.apache.axis2.transport.amqp.common.AMQPMessage;

public class BytesMessageOutputStream extends OutputStream {
    private final AMQPMessage message;

    public BytesMessageOutputStream(AMQPMessage message) {
        this.message = message;
    }

    @Override
    public void write(int b) throws AMQPExceptionWrapper {
        try {
            message.writeByte((byte)b);
        } catch (JMSException ex) {
            throw new AMQPExceptionWrapper(ex);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws AMQPExceptionWrapper {
        try {
            message.writeBytes(b, off, len);
        } catch (JMSException ex) {
            new AMQPExceptionWrapper(ex);
        }
    }

    @Override
    public void write(byte[] b) throws AMQPExceptionWrapper {
        try {
            message.writeBytes(b);
        } catch (JMSException ex) {
            throw new AMQPExceptionWrapper(ex);
        }
    }
}