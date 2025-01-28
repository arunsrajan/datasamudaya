package com.github.datasamudaya.stream;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;

import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.functions.CalculateCount;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.functions.CoalesceFunction;
import com.github.datasamudaya.common.functions.CountByKeyFunction;
import com.github.datasamudaya.common.functions.CountByValueFunction;
import com.github.datasamudaya.common.functions.Distinct;
import com.github.datasamudaya.common.functions.DoubleTupleFlatMapFunction;
import com.github.datasamudaya.common.functions.FlatMapFunction;
import com.github.datasamudaya.common.functions.FoldByKey;
import com.github.datasamudaya.common.functions.GroupByKeyFunction;
import com.github.datasamudaya.common.functions.HashPartitioner;
import com.github.datasamudaya.common.functions.IntersectionFunction;
import com.github.datasamudaya.common.functions.Join;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftJoin;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.LongTupleFlatMapFunction;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.MapToPairFunction;
import com.github.datasamudaya.common.functions.MapValuesFunction;
import com.github.datasamudaya.common.functions.PeekConsumer;
import com.github.datasamudaya.common.functions.PredicateSerializable;
import com.github.datasamudaya.common.functions.ReduceByKeyFunction;
import com.github.datasamudaya.common.functions.RightJoin;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.SortedComparator;
import com.github.datasamudaya.common.functions.TupleFlatMapFunction;
import com.github.datasamudaya.common.functions.UnionFunction;

/**
 * 
 * @author arun MapPair holding the MapPairFunction, JoinPair, ReduceByKey,
 *         etc... functions runs on standalone executors.
 * @param <I1>
 * @param <I2>
 */
@SuppressWarnings("rawtypes")
public sealed class MapPair<I1, I2> extends AbstractPipeline permits MapValues {
	private static Logger log = LogManager.getLogger(MapPair.class);

	public MapPair() {

	}

	/**
	 * MapPair constructor for rightouterjoin.
	 * 
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param conditionrightouterjoin
	 */
	@SuppressWarnings({ "unchecked" })
	protected <T, O1, O2> MapPair(AbstractPipeline parent, Object task) {
		parent.childs.add(this);
		this.parents.add(parent);
		tasks.add(task);
		this.pipelineconfig = parent.pipelineconfig;
		this.csvoptions = parent.csvoptions;
		this.json = parent.json;
	}

	/**
	 * MapPair accepts for Mapvalues
	 * 
	 * @param <I3>
	 * @param <I4>
	 * @param mvf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3, I4> MapValues<I1, Tuple2<I3, I4>> mapValues(MapValuesFunction<? super I2, ? extends Tuple2<I3, I4>> mvf)
			throws PipelineException {
		if (Objects.isNull(mvf)) {
			throw new PipelineException(PipelineConstants.MAPVALUESNULL);
		}
		var mapvalues = new MapValues(this, mvf);
		return mapvalues;
	}

	/**
	 * MapPair map function accepting MapPairFunction.
	 * 
	 * @param <T>
	 * @param map
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <T> StreamPipeline<T> map(MapFunction<? super Tuple2<I1, I2>, ? extends T> map) throws PipelineException {
		if (Objects.isNull(map)) {
			throw new PipelineException(PipelineConstants.MAPFUNCTIONNULL);
		}
		var mapobj = new StreamPipeline(this, map);
		return mapobj;
	}

	/**
	 * MapPair accepts join function
	 * 
	 * @param <T>
	 * @param mapright
	 * @param conditioninnerjoin
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <T, T1, T2> MapPair<T, T> join(AbstractPipeline mapright,
			JoinPredicate<Tuple2<I1, T1>, Tuple2<I1, T2>> conditioninnerjoin) throws PipelineException {
		if (Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		if (Objects.isNull(conditioninnerjoin)) {
			throw new PipelineException(PipelineConstants.INNERJOINCONDITION);
		}
		var mp = new MapPair(this, mapright, conditioninnerjoin);
		return mp;
	}

	private MapPair(AbstractPipeline parent1, AbstractPipeline parent2, Object task) {
		parent1.childs.add(this);
		this.parents.add(parent1);
		parent2.childs.add(this);
		this.parents.add(parent2);
		this.tasks.add(task);
		this.pipelineconfig = parent1.pipelineconfig;
	}

	/**
	 * MapPair accepts Join
	 * 
	 * @param <I3>
	 * @param mapright
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3> MapPair<I1, Tuple2<I2, I3>> join(MapPair<I1, I3> mapright) throws PipelineException {
		if (Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		var mp = new MapPair(this, mapright, new Join());
		return mp;
	}

	/**
	 * MapPair accepts Right Join
	 * 
	 * @param <I3>
	 * @param mapright
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3> MapPair<I1, Tuple2<I2, I3>> leftJoin(MapPair<I1, I3> mapright) throws PipelineException {
		if (Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		var mp = new MapPair(this, mapright, new LeftJoin());
		return mp;
	}

	/**
	 * MapPair accepts Right Join
	 * 
	 * @param <I3>
	 * @param mapright
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3> MapPair<I1, Tuple2<I2, I3>> rightJoin(MapPair<I1, I3> mapright) throws PipelineException {
		if (Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		var mp = new MapPair(this, mapright, new RightJoin());
		return mp;
	}

	/**
	 * MapPair accepts distinct
	 * 
	 * @return MapPair objet
	 */
	public MapPair<I1, I2> distinct() {
		var distinct = new Distinct();
		var map = new MapPair<I1, I2>(this, distinct);
		return map;
	}

	/**
	 * MapPair accepts the Predicate for filter.
	 * 
	 * @param predicate
	 * @return MapPair object
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I1> filter(PredicateSerializable<? super Tuple2> predicate) throws PipelineException {
		if (Objects.isNull(predicate)) {
			throw new PipelineException(PipelineConstants.PREDICATENULL);
		}
		var filter = new MapPair(this, predicate);
		return filter;
	}

	/**
	 * MapPair accepts the union mappair object.
	 * 
	 * @param union
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I1> union(MapPair union) throws PipelineException {
		if (Objects.isNull(union)) {
			throw new PipelineException(PipelineConstants.UNIONNULL);
		}
		var unionfunction = new UnionFunction();
		var unionchild = new MapPair(this, union, unionfunction);
		return unionchild;
	}

	/**
	 * MapPair accept the intersection mappair object.
	 * 
	 * @param intersection
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I1> intersection(MapPair intersection) throws PipelineException {
		if (Objects.isNull(intersection)) {
			throw new PipelineException(PipelineConstants.INTERSECTIONNULL);
		}
		var intersectionfunction = new IntersectionFunction();
		var intersectionchild = new MapPair(this, intersection, intersectionfunction);
		return intersectionchild;
	}

	/**
	 * MapPair which accepts the MapPair function.
	 * 
	 * @param <I3>
	 * @param <I4>
	 * @param pf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3, I4> MapPair<I3, I4> mapToPair(MapToPairFunction<? super Tuple2<I1, I2>, Tuple2<I3, I4>> pf)
			throws PipelineException {
		if (Objects.isNull(pf)) {
			throw new PipelineException(PipelineConstants.MAPPAIRNULL);
		}
		var mappair = new MapPair(this, pf);
		return mappair;
	}

	/**
	 * MapPair accepts the sample.
	 * 
	 * @param numsample
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I2> sample(Integer numsample) throws PipelineException {
		if (Objects.isNull(numsample)) {
			throw new PipelineException(PipelineConstants.SAMPLENULL);
		}
		var sampleintegersupplier = new SampleSupplierInteger(numsample);
		var samplesupplier = new MapPair(this, sampleintegersupplier);
		return samplesupplier;
	}

	/**
	 * MapPair accepts the right outer join.
	 * 
	 * @param mappair
	 * @param conditionrightouterjoin
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I2> rightOuterjoin(AbstractPipeline mappair,
			RightOuterJoinPredicate<Tuple2<I1, I2>, Tuple2<I1, I2>> conditionrightouterjoin) throws PipelineException {
		if (Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOIN);
		}
		if (Objects.isNull(conditionrightouterjoin)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOINCONDITION);
		}
		var mdp = new MapPair(this, mappair, conditionrightouterjoin);
		return mdp;
	}

	/**
	 * MapPair accepts the left outer join.
	 * 
	 * @param mappair
	 * @param conditionleftouterjoin
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I2> leftOuterjoin(AbstractPipeline mappair,
			LeftOuterJoinPredicate<Tuple2<I1, I2>, Tuple2<I1, I2>> conditionleftouterjoin) throws PipelineException {
		if (Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOIN);
		}
		if (Objects.isNull(conditionleftouterjoin)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOINCONDITION);
		}
		var mdp = new MapPair(this, mappair, conditionleftouterjoin);
		return mdp;
	}

	/**
	 * MapPair accepts the flatmap function.
	 * 
	 * @param <T>
	 * @param fmf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3, I4> MapPair<I3, I4> flatMap(FlatMapFunction<? super Tuple2<I1, I2>, ? extends Tuple2<I3, I4>> fmf)
			throws PipelineException {
		if (Objects.isNull(fmf)) {
			throw new PipelineException(PipelineConstants.FLATMAPNULL);
		}
		var mdp = new MapPair(this, fmf);
		return mdp;
	}

	/**
	 * MapPair accepts the TupleFlatMap function.
	 * 
	 * @param <I3>
	 * @param <I4>
	 * @param pfmf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3, I4> MapPair<I3, I4> flatMapToTuple(TupleFlatMapFunction<? super I1, ? extends Tuple2<I3, I4>> pfmf)
			throws PipelineException {
		if (Objects.isNull(pfmf)) {
			throw new PipelineException(PipelineConstants.FLATMAPPAIRNULL);
		}
		var mdp = new MapPair(this, pfmf);
		return mdp;
	}

	/**
	 * MapPair accepts the LongTupleFlatMap function.
	 * 
	 * @param lfmf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<Long, Long> flatMapToLong(LongTupleFlatMapFunction<Tuple2<I1, I2>> lfmf) throws PipelineException {
		if (Objects.isNull(lfmf)) {
			throw new PipelineException(PipelineConstants.LONGFLATMAPNULL);
		}
		var mdp = new MapPair(this, lfmf);
		return mdp;
	}

	/**
	 * MapPair accepts the DoubleTupleFlatMap function.
	 * 
	 * @param dfmf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<Double, Double> flatMapToDouble(DoubleTupleFlatMapFunction<Tuple2<I1, I2>> dfmf)
			throws PipelineException {
		if (Objects.isNull(dfmf)) {
			throw new PipelineException(PipelineConstants.DOUBLEFLATMAPNULL);
		}
		var mdp = new MapPair(this, dfmf);
		return mdp;
	}

	/**
	 * MapPair accepts the peek function.
	 * 
	 * @param consumer
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I1> peek(PeekConsumer consumer) throws PipelineException {
		if (Objects.isNull(consumer)) {
			throw new PipelineException(PipelineConstants.PEEKNULL);
		}
		var map = new MapPair(this, consumer);
		return map;
	}

	/**
	 * MapPair accepts the sort function.
	 * 
	 * @param sortedcomparator
	 * @return MapPair object
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I2> sorted(SortedComparator<? super Tuple2> sortedcomparator) throws PipelineException {
		if (Objects.isNull(sortedcomparator)) {
			throw new PipelineException(PipelineConstants.SORTEDNULL);
		}
		var map = new MapPair(this, sortedcomparator);
		return map;
	}

	/**
	 * MapPair accepts the coalesce function.
	 * 
	 * @param partition
	 * @param cf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I2> coalesce(int partition, CoalesceFunction<I2> cf) throws PipelineException {
		if (Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.COALESCENULL);
		}
		var mappaircoalesce = new MapPair(this, new Coalesce(partition, cf));
		return mappaircoalesce;
	}

	/**
	 * MapPair accepts the coalesce partition number.
	 * 
	 * @param partition
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I2> coalesce(int partition) throws PipelineException {
		var mappaircoalesce = new MapPair(this, new Coalesce(partition, null));
		return mappaircoalesce;
	}

	/**
	 * MapPair with single partition number.
	 * 
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I2> coalesce() throws PipelineException {
		var mappaircoalesce = new MapPair(this, new Coalesce(1, null));
		return mappaircoalesce;
	}

	/**
	 * MapPair accepts the reducefunction.
	 * 
	 * @param rf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I2> reduceByKey(ReduceByKeyFunction<I2> rf) throws PipelineException {
		if (Objects.isNull(rf)) {
			throw new PipelineException(PipelineConstants.REDUCENULL);
		}
		var mappair = new MapPair(this, rf);
		return mappair;
	}

	/**
	 * MapPair accepts the fold left.
	 * 
	 * @param value
	 * @param rf
	 * @param partition
	 * @param cf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, I2> foldLeft(Object value, ReduceByKeyFunction<I2> rf, int partition, CoalesceFunction<I2> cf)
			throws PipelineException {
		if (Objects.isNull(rf)) {
			throw new PipelineException(PipelineConstants.FOLDLEFTREDUCENULL);
		}
		if (Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.FOLDLEFTCOALESCENULL);
		}
		var mappair = new MapPair(this, new FoldByKey(value, rf, true));
		if (cf != null) {
			var mappaircoalesce = new MapPair(mappair, new Coalesce(partition, cf));
			return mappaircoalesce;
		}
		return mappair;
	}

	/**
	 * MapPair accepts the fold right.
	 * 
	 * @param value
	 * @param rf
	 * @param partition
	 * @param cf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	public MapPair<I1, I2> foldRight(Object value, ReduceByKeyFunction<I2> rf, int partition, CoalesceFunction<I2> cf)
			throws PipelineException {
		if (Objects.isNull(rf)) {
			throw new PipelineException(PipelineConstants.FOLDRIGHTREDUCENULL);
		}
		if (Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.FOLDRIGHTCOALESCENULL);
		}
		var mappair = new MapPair(this, new FoldByKey(value, rf, false));
		if (cf != null) {
			var mappaircoalesce = new MapPair(mappair, new Coalesce(partition, cf));
			mappair.childs.add(mappaircoalesce);
			mappaircoalesce.parents.add(mappair);
			return mappaircoalesce;
		}
		return mappair;
	}

	/**
	 * MapPair accepts the groupbykey.
	 * 
	 * @return MapPair object.
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, List<I2>> groupByKey() {
		var mappair = new MapPair(this, new GroupByKeyFunction());
		return mappair;
	}

	/**
	 * MapPair accepts the cogroup mappair object.
	 * 
	 * @param mappair2
	 * @return MapPair object.
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public MapPair<I1, Tuple2<List<I2>, List<I2>>> cogroup(MapPair<I1, I2> mappair2) {
		var gbkleft = this.groupByKey();
		var gbkright = mappair2.groupByKey();
		var mdp = new MapPair(gbkleft, gbkright,
				(LeftOuterJoinPredicate<Tuple2<I1, List<I2>>, Tuple2<I1, List<I2>>>) ((Tuple2<I1, List<I2>> tuple1,
						Tuple2<I1, List<I2>> tuple2) -> tuple1.v1.equals(tuple2.v1)));
		return mdp;
	}

	/**
	 * MapPair accepts the CountByKey function.
	 * 
	 * @return MapPair object.
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I1, Long> countByKey() {
		var mappair = new MapPair(this, new CountByKeyFunction());
		return mappair;
	}

	/**
	 * MapPair accepts the HashPartition object.
	 * 
	 * @return MapPair object.
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<Integer, List<Tuple2<I1, I2>>> partition(HashPartitioner partitoner) {
		var mappair = new MapPair(this, partitoner);
		return mappair;
	}

	/**
	 * MapPair accepts the CountByValue function.
	 * 
	 * @return MapPair object.
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPair<I2, Long> countByValue() {
		var mappair = new MapPair(this, new CountByValueFunction());
		return mappair;
	}

	/**
	 * This function executes the collect tasks.
	 * 
	 * @param toexecute
	 * @param supplier
	 * @return list
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public List collect(boolean toexecute, IntSupplier supplier) throws PipelineException {
		try {
			var sp = new StreamPipeline(this, null);
			return sp.collect(toexecute, supplier);
		} catch(PipelineException pex) {
			throw pex;
		} catch (Exception ex) {
			log.error(PipelineConstants.PIPELINECOLLECTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOLLECTERROR, ex);
		}
	}

	/**
	 * This function executes the count tasks.
	 * 
	 * @param supplier
	 * @return object
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public Object count(IntSupplier supplier) throws PipelineException {
		try {
			var mdp = new StreamPipeline(this, new CalculateCount());
			return mdp.collect(true, supplier);
		} catch (Exception ex) {
			log.error(PipelineConstants.PIPELINECOUNTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOUNTERROR, ex);
		}
	}

	/**
	 * This method saves the result to the hdfs.
	 * 
	 * @param uri
	 * @param path
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public void saveAsTextFile(URI uri, String path) throws PipelineException {
		try {
			var mdscollect = (StreamPipeline) new StreamPipeline<>(this, null);
			mdscollect.saveAsTextFile(uri, path);
		} catch (Exception e) {
			log.error(PipelineConstants.FILEIOERROR, e);
			throw new PipelineException(PipelineConstants.FILEIOERROR, e);
		}
	}

	/**
	 * This function executes the forEach tasks.
	 * 
	 * @param consumer
	 * @param supplier
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public void forEach(Consumer<List<Tuple2>> consumer, IntSupplier supplier) throws PipelineException {
		try {
			var mdscollect = (StreamPipeline) new StreamPipeline<>(this, null);
			mdscollect.forEach(consumer, supplier);
		} catch (Exception e) {
			log.error(PipelineConstants.PIPELINEFOREACHERROR, e);
			throw new PipelineException(PipelineConstants.PIPELINEFOREACHERROR, e);
		}
	}

	@Override
	public String toString() {
		return "MapPair [task=" + tasks + "]";
	}

}
