package com.github.datasamudaya.common;

import java.io.Serializable;

/**
 * The interface for implementing the RemoteBlocksTransferManager or LocalBlocksTransferManager
 * @author arun
 *
 */
public interface TransferManager extends Serializable {
	public abstract boolean transferBlocks(ShuffleBlock[] shuffleBlocks);
}
