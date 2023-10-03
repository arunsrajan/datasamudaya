package com.github.datasamudaya.common;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * This class holds information on the user information like username, percentage allocation etc.
 * @author arun
 *
 */
@Setter
@Getter
@AllArgsConstructor
public class User {
	private String user;
	private Integer percentage;
	private Map<String,Boolean> isallocated;
	private Map<String, List<ContainerResources>> nodecontainersmap;
}
