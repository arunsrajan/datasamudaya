package com.github.datasamudaya.common.utils;

import java.io.Serializable;

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
public class RemoteListIteratorTask implements Serializable {

	private static final long serialVersionUID = 1L;

	private Task task;
	
}
