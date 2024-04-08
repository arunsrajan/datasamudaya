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
public class RemoteIteratorNext implements Serializable {

	private static final long serialVersionUID = 5456804524884828242L;
	private RequestType requestType;

}
