package com.github.datasamudaya.common;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The records for shuffled block to transfer and store either locally or remotely
 * @author arun
 *
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ShuffleBlock implements Serializable {
	private static final long serialVersionUID = 3738460167739240777L;
	String blockId;
	byte[] partitionId;
	Object data;
}
