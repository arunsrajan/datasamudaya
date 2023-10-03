package com.github.datasamudaya.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.hadoop.hdfs.BlockReader;

public class BlockReaderInputStream extends InputStream implements Serializable{
	private static final long serialVersionUID = 7812408972554339334L;
	private final long limit;
    private long bytesRead;
    BlockReader br;
    byte[] onebyt = new byte[1];
    public BlockReaderInputStream(BlockReader br, long limit) {
    	this.br = br;
        this.limit = limit;
        this.bytesRead = 0;
    }

    @Override
    public int read() throws IOException {
        if (bytesRead >= limit) {
            return -1;
        }
        int result = br.read(onebyt, 0, 1);
        if (result != -1) {
            bytesRead++;
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (bytesRead >= limit) {
            return -1;
        }
        int bytesToRead = (int) Math.min(len, limit - bytesRead);
        int bytesReadNow = br.read(b, off, bytesToRead);
        if (bytesReadNow != -1) {
            bytesRead += bytesReadNow;
        }
        return bytesReadNow;
    }

    @Override
    public void close() {
    	try {
			br.close();
		} catch (IOException e) {			
		}
    }
    
}
