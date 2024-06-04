package com.github.datasamudaya.common;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

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
@EqualsAndHashCode
@ToString
public class FilePartitionId implements Serializable,Cloneable {
	private static final long serialVersionUID = 5674301742484954617L;
	String partitionId;
	String localFilePath;
	int startRange;
	int endRange;
	int partitionNumber;

	@Override
	public Object clone() {
		try {
			return super.clone();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
