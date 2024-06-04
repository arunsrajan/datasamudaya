package com.github.datasamudaya.common;

/**
 * The interface for storing and retrieving the ShuffleBlocks via implementations such as Memory Or File Block Manager
 * @author arun
 *
 */
public interface BlockManager {
	public abstract boolean storeBlocks(ShuffleBlock shuffleBlock);

	public abstract ShuffleBlock retrieveBlockData(String blockId);
}
