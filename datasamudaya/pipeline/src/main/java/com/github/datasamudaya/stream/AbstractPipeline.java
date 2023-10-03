package com.github.datasamudaya.stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import com.github.datasamudaya.common.Dummy;
/**
 * 
 * @author arun
 * This class is abstract and base class for pipeline api.
 */
public sealed class AbstractPipeline permits IgniteCommon,MapPair,StreamPipeline,PipelineIntStream{
	AbstractPipeline root=this;
	Collection<AbstractPipeline> mdsroots = new LinkedHashSet<>();
	List<AbstractPipeline> parents = new ArrayList<>();
	List<AbstractPipeline> childs = new ArrayList<>();
	public Object task = new Dummy();
	Object finaltask = null;
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((task == null) ? 0 : task.hashCode());
		return result;
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
		if (task == null) {
			if (other.task != null)
				return false;
		} else if (!task.equals(other.task))
			return false;
		return true;
	}
	
	
	
	
}
