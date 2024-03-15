package com.github.datasamudaya.common;

import java.io.Serializable;

/**
 * The record for TaskExecutors with filepartition ids
 * @author arun
 *
 */
public record TaskExecutorId(String id, String host, String port, FilePartitionId[] filePartitionIds) implements Serializable {

}
