package com.github.datasamudaya.common.utils;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The class get value from the remote iterator server
 * @author arun
 *
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class RemoteIteratorClose implements Serializable {

	private static final long serialVersionUID = -8817024385824956183L;
	private RequestType requestType;

}
