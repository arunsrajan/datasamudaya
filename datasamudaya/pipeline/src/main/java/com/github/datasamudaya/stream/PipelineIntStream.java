/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasamudaya.stream;

import java.util.List;
import java.util.function.IntUnaryOperator;
import org.apache.log4j.Logger;

import com.github.datasamudaya.common.functions.AtomicBiConsumer;
import com.github.datasamudaya.common.functions.AtomicIntegerSupplier;
import com.github.datasamudaya.common.functions.AtomicObjIntConsumer;
import com.github.datasamudaya.common.functions.Distinct;
import com.github.datasamudaya.common.functions.Max;
import com.github.datasamudaya.common.functions.Min;
import com.github.datasamudaya.common.functions.SToIntFunction;
import com.github.datasamudaya.common.functions.StandardDeviation;
import com.github.datasamudaya.common.functions.Sum;
import com.github.datasamudaya.common.functions.SummaryStatistics;

/**
 * This class is used to calculate the integer functions using stream pipeline
 * @author arun
 *
 * @param <I1>
 */
public final class PipelineIntStream<I1> extends AbstractPipeline {
	private static Logger log = Logger.getLogger(PipelineIntStream.class);

	/**
	 * Constructor for integer function.
	 * @param root
	 * @param tointfunction
	 */
	PipelineIntStream(AbstractPipeline root,
			SToIntFunction<I1> tointfunction)  {
		this.task = tointfunction;
		this.root = root;
		root.finaltask = task;
	}


	/**
	 * private constructor for unary operator
	 * @param root
	 * @param intunaryoperator
	 */
	private PipelineIntStream(AbstractPipeline root,
			IntUnaryOperator intunaryoperator)  {
		this.task = intunaryoperator;
		this.root = root;
		root.finaltask = task;
	}


	/**
	 * The private constructor for statistical summary.
	 * @param root
	 * @param summarystatistics
	 */
	private PipelineIntStream(AbstractPipeline root,
			SummaryStatistics summarystatistics)  {
		this.task = summarystatistics;
		this.root = root;
		root.finaltask = task;
	}


	/**
	 * The private constructor for statistical sum function.
	 * @param root
	 * @param sum
	 */
	private PipelineIntStream(AbstractPipeline root,
			Sum sum)  {
		this.task = sum;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * The private constructor for statistical max function.
	 * @param root
	 * @param max
	 */
	private PipelineIntStream(AbstractPipeline root,
			Max max)  {
		this.task = max;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * The private constructor for statistical min function.
	 * @param root
	 * @param min
	 */
	private PipelineIntStream(AbstractPipeline root,
			Min min)  {
		this.task = min;
		this.root = root;
		root.finaltask = task;
	}
	/**
	 * The private constructor for statistical standard deviation function.
	 * @param root
	 * @param min
	 */
	private PipelineIntStream(AbstractPipeline root,
			StandardDeviation stddev)  {
		this.task = stddev;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * The public constructor for integer unary operator. 
	 * @param intunaryoperator
	 * @return
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public PipelineIntStream<I1> map(IntUnaryOperator intunaryoperator)  {
		var map = new PipelineIntStream(root, intunaryoperator);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}

	/**
	 * The private constructor for distinct function. 
	 * @param root
	 * @param distinct
	 */
	private PipelineIntStream(AbstractPipeline root,
			Distinct distinct) {
		this.task = distinct;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * The public constructor for distinct function. 
	 * @return integer stream pipeline object
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public PipelineIntStream<I1> distinct()  {
		var distinct = new Distinct();
		var map = new PipelineIntStream(root, distinct);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}


	/**
	 * The private constructor for the pipeline collect
	 * @param root
	 * @param piplinint
	 */
	private PipelineIntStream(AbstractPipeline root,
			PipelineIntStreamCollect piplinint)  {
		this.task = piplinint;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * The collect function which starts execution and provide results. 
	 * @param <R>
	 * @param toexecute
	 * @param supplier
	 * @param objintconsumer
	 * @param biconsumer
	 * @return list of results.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public <R> List collect(boolean toexecute, AtomicIntegerSupplier<R> supplier,
			AtomicObjIntConsumer<R> objintconsumer,
			AtomicBiConsumer<R, R> biconsumer) throws PipelineException  {
		log.debug("Collect task begin...");
		var pintstr = new PipelineIntStream(root, new PipelineIntStreamCollect(supplier,
				objintconsumer, biconsumer));
		pintstr.parents.add(this);
		this.childs.add(pintstr);
		var mdscollect = (StreamPipeline) pintstr.root;
		mdscollect.finaltasks.clear();
		mdscollect.finaltasks.add(pintstr.task);
		mdscollect.mdsroots.add(root);
		var result = mdscollect.collect(toexecute, null);
		log.debug("Collect task ended.");
		return result;
	}
	
	/**
	 * The statistical summary for the integer pipeline.
	 * @return statistics
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public List summaryStatistics() throws PipelineException {
		log.debug("Summary Statistics task begin...");
		var map = new PipelineIntStream(root, new SummaryStatistics());
		map.parents.add(this);
		this.childs.add(map);
		var mdscollect = (StreamPipeline) root;
		mdscollect.finaltasks.clear();
		mdscollect.finaltasks.add(map.task);
		mdscollect.mdsroots.add(root);
		var result = mdscollect.collect(true, null);
		log.debug("Summary Statistics task ended.");
		return result;
	}

	/**
	 * The sum function for integer pipeline.
	 * @return results in list
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public List sum() throws PipelineException {
		log.debug("Sum task begin...");
		var map = new PipelineIntStream(root, new Sum());
		map.parents.add(this);
		this.childs.add(map);
		var mdscollect = (StreamPipeline) root;
		mdscollect.finaltasks.clear();
		mdscollect.finaltasks.add(map.task);
		mdscollect.mdsroots.add(root);
		var result = mdscollect.collect(true, null);
		log.debug("Sum task ended.");
		return result;
	}
	
	/**
	 * The max function for integer pipeline.
	 * @return results in list
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public List max() throws PipelineException {
		log.debug("Max task begin...");
		var map = new PipelineIntStream(root, new Max());
		map.parents.add(this);
		this.childs.add(map);
		var mdscollect = (StreamPipeline) root;
		mdscollect.finaltasks.clear();
		mdscollect.finaltasks.add(map.task);
		mdscollect.mdsroots.add(root);
		var result = mdscollect.collect(true, null);
		log.debug("Max task ended.");
		return result;
	}
	/**
	 * The min function for integer pipeline.
	 * @return results in list
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public List min() throws PipelineException {
		log.debug("Min task begin...");
		var map = new PipelineIntStream(root, new Min());
		map.parents.add(this);
		this.childs.add(map);
		var mdscollect = (StreamPipeline) root;
		mdscollect.finaltasks.clear();
		mdscollect.finaltasks.add(map.task);
		mdscollect.mdsroots.add(root);
		var result = mdscollect.collect(true, null);
		log.debug("Min task ended.");
		return result;
	}

	/**
	 * The standardDeviation function for integer pipeline.
	 * @return results in list
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public List standardDeviation() throws PipelineException {
		log.debug("StandardDeviation task begin...");
		var map = new PipelineIntStream(root, new StandardDeviation());
		map.parents.add(this);
		this.childs.add(map);
		var mdscollect = (StreamPipeline) root;
		mdscollect.finaltasks.clear();
		mdscollect.finaltasks.add(map.task);
		mdscollect.mdsroots.add(root);
		var result = mdscollect.collect(true, null);
		log.debug("StandardDeviation task ended.");
		return result;
	}
}
