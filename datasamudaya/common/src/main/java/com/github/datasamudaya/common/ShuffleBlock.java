package com.github.datasamudaya.common;

import java.io.Serializable;

/**
 * The records for shuffled block to transfer and store either locally or remotely
 * @author arun
 *
 */
public record ShuffleBlock(String blockId, byte[] partitionId, Object data) implements Serializable{

}
