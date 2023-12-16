package com.github.datasamudaya.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.github.datasamudaya.common.PipelineConfig;
/**
 * 
 * @author arun
 * This class is abstract and base class for pipeline api.
 */
public sealed class AbstractPipeline permits StreamPipeline, MapPair{
	List<AbstractPipeline> parents = new ArrayList<>();
	List<AbstractPipeline> childs = new ArrayList<>();
	public List<Object> tasks = new ArrayList<>();
	public PipelineConfig pipelineconfig;
	protected CsvOptions csvoptions;
	@Override
	public int hashCode() {
		return Objects.hash(tasks);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AbstractPipeline other = (AbstractPipeline) obj;
		return Objects.equals(tasks, other.tasks);
	}
	
}
