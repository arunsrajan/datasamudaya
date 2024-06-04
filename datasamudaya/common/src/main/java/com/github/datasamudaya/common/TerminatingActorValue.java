package com.github.datasamudaya.common;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class TerminatingActorValue implements Serializable {

	private static final long serialVersionUID = 1469782440350386009L;
	int terminatingval;
}
