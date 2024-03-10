package com.github.datasamudaya.common;

import java.io.Serializable;

import akka.actor.ActorSelection;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The record for storing the FilePartition Information
 * 
 * @author arun
 *
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class FilePartitionId implements Serializable {
	String partitionId;
	String localFilePath;
	ActorSelection actorSelection;
	int startRange;
	int endRange;
	int partitionNumber;

}
