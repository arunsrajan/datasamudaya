package com.github.datasamudaya.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockReaderInputStream extends InputStream implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(BlockReaderInputStream.class);
	private static final long serialVersionUID = 7812408972554339334L;
	private final long limit;
	private long bytesRead;
	private transient FSDataInputStream fsdis;
	ByteBuffer buffer;
	long startoffset;

	public BlockReaderInputStream(FSDataInputStream fsdis, long startoffset, long limit) throws Exception {
		this.fsdis = fsdis;
		this.startoffset = startoffset;
		fsdis.seek(startoffset);
		buffer = ByteBufferPoolDirect.get(DataSamudayaConstants.BYTEBUFFERSIZE);		
		this.limit = limit;
		this.bytesRead = 0;
	}

	@Override
	public int read() throws IOException {
		if (bytesRead >= limit) {
            return -1; // Read limit reached
        }
        if (!buffer.hasRemaining()) {
            buffer.clear(); // Clear the buffer for writing
            int bytesToRead = (int) Math.min(buffer.capacity(), limit - bytesRead);
            buffer.limit(bytesToRead); // Adjust buffer limit based on the read limit
            int numread = fsdis.read(buffer);
            if (numread == -1) {
                return -1; // End of file reached
            }
            buffer.flip(); // Flip the buffer for reading
        }
        bytesRead++;
        return buffer.get() & 0xFF; // Return the next byte
	}

	@Override
	public void close() {
		try {
			fsdis.close();
			fsdis = null;			
			ByteBufferPoolDirect.destroy(buffer);
			buffer = null;
		} catch (Exception e) {
			log.warn("Error While closing BlockReader {}", fsdis);
		}
	}

}
