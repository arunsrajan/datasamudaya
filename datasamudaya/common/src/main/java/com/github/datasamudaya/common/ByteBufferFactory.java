package com.github.datasamudaya.common;

import java.nio.ByteBuffer;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class ByteBufferFactory extends BasePooledObjectFactory<ByteBuffer> {

    private final int bufferSize;

    public ByteBufferFactory(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public ByteBuffer create() throws Exception {
        return ByteBuffer.allocateDirect(bufferSize);
    }

    @Override
    public PooledObject<ByteBuffer> wrap(ByteBuffer buffer) {
        return new DefaultPooledObject<>(buffer);
    }

    // Optionally, you can override methods to validate objects, destroy objects, etc.
}
