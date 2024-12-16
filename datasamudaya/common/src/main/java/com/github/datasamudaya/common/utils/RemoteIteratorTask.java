package com.github.datasamudaya.common.utils;

import java.io.Serializable;
import java.util.List;

import com.github.datasamudaya.common.FieldCollationDirection;
import com.github.datasamudaya.common.Task;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The class initializes the remote iterator with the given task for sorting
 * @author arun
 *
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class RemoteIteratorTask implements Serializable {

	private static final long serialVersionUID = 1L;

	private Task task;
	private String appendwithpath;
	private boolean appendintermediate;
	private boolean left;
	private boolean right;
	private boolean mr;
	private List<FieldCollationDirection> fcsc;
	private IteratorType iteratortype;
}
