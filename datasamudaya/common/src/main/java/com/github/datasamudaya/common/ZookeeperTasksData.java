package com.github.datasamudaya.common;

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ZookeeperTasksData {
	private String primaryhostport;
	private List<String> resultshardhostports=new ArrayList<>();
	private Boolean isresultavailable = false;
}
