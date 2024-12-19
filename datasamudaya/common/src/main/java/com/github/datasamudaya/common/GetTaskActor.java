package com.github.datasamudaya.common;

import java.io.Serializable;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * GetTask Actors Url Class
 * @author arun
 *
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class GetTaskActor implements Serializable {
	private static final long serialVersionUID = -4550987408435507107L;
	private Task task;
	private List<String> childtaskactors;
	private int terminatingparentcount;
	private boolean tostartdummy;
}
