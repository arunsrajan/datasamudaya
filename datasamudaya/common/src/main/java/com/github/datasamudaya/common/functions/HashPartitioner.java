package com.github.datasamudaya.common.functions;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class HashPartitioner implements Serializable {
	private static final long serialVersionUID = -5978113889847160389L;
	private int partitionnumber;
}
