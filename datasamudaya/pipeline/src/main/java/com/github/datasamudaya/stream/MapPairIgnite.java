package com.github.datasamudaya.stream;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.datasamudaya.common.Job;
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
import com.github.datasamudaya.common.functions.KeyByFunction;
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
 * @author arun
 * MapPairIgnite holding the MapPairFunction and JoinPair,
 * ReduceByKey, etc functions to run on ignite server.
 * @param <I1>
 * @param <I2>
 */
@SuppressWarnings("rawtypes")
public sealed class MapPairIgnite<I1, I2> extends IgniteCommon permits MapValuesIgnite {
	private static Logger log = Logger.getLogger(MapPairIgnite.class);

	public MapPairIgnite() {

	}
	
	/**
	 * MapPairIgnite constructor for rightouterjoin.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param conditionrightouterjoin
	 */
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root,
			RightOuterJoinPredicate<? super I1, ? super I2> conditionrightouterjoin) {
		this.task = conditionrightouterjoin;
		this.root = root;
		root.finaltask = task;
	}
	
	/**
	 * MapPairIgnite constructor for left outer join. 
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param conditionleftouterjoin
	 */
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root,
			LeftOuterJoinPredicate<? super I1, ? super I2> conditionleftouterjoin) {
		this.task = conditionleftouterjoin;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite constructor for keybyfunction.
	 * @param <I1>
	 * @param <I2>
	 * @param root
	 * @param keybyfunction
	 */
	@SuppressWarnings({"hiding" })
	<I1, I2> MapPairIgnite(AbstractPipeline root, KeyByFunction<I1, I2> keybyfunction) {
		this.task = keybyfunction;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts for MapvaluesIgnite
	 * @param <I3>
	 * @param <I4>
	 * @param mvf
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3, I4> MapValuesIgnite<I1, Tuple2<I3, I4>> mapValues(MapValuesFunction<? super I2, ? extends Tuple2<I3, I4>> mvf)
			throws PipelineException {
		if (Objects.isNull(mvf)) {
			throw new PipelineException(PipelineConstants.MAPVALUESNULL);
		}
		var mapvalues = new MapValuesIgnite(root, mvf);
		this.childs.add(mapvalues);
		mapvalues.parents.add(this);
		return mapvalues;
	}

	/**
	 * MapPairIgnite constructor for MapPairIgnite function.
	 * @param <I3>
	 * @param <I4>
	 * @param root
	 * @param pf
	 */
	protected <I3, I4> MapPairIgnite(AbstractPipeline root,
			MapToPairFunction<? super I2, ? extends Tuple2<? super I3, ? super I4>> pf) {
		this.task = pf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite map function accepting MapPairIgniteFunction.
	 * @param <T>
	 * @param map
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <T> MapPairIgnite<T, T> map(MapToPairFunction<? super Tuple2<I1, I2>, ? extends T> map)
			throws PipelineException {
		if (Objects.isNull(map)) {
			throw new PipelineException(PipelineConstants.MAPFUNCTIONNULL);
		}
		var mapobj = new MapPairIgnite(root, map);
		this.childs.add(mapobj);
		mapobj.parents.add(this);
		return mapobj;
	}
	
	/**
	 * MapPair map function accepting MapPairFunction.
	 * @param <T>
	 * @param map
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <T> IgnitePipeline<T> map(MapFunction<? super Tuple2<I1,I2> ,? extends T> map) throws PipelineException  {
		if(Objects.isNull(map)) {
			throw new PipelineException(PipelineConstants.MAPFUNCTIONNULL);
		}
		var mapobj = new IgnitePipeline(root,map);
		this.childs.add(mapobj);
		mapobj.parents.add(this);
		return mapobj;
	}

	/**
	 * MapPairIgnite constructor for Peek function
	 * @param <T>
	 * @param root
	 * @param peekConsumer
	 */
	private <T> MapPairIgnite(AbstractPipeline root, PeekConsumer peekConsumer) {
		this.task = peekConsumer;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask = task;
	}
	
	/**
	 * MapPairIgnite constructor for Join function
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param joinpredicate
	 */
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root, JoinPredicate joinpredicate) {
		this.task = joinpredicate;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite constructor for count
	 * @param root
	 * @param calculatecount
	 */
	protected MapPairIgnite(AbstractPipeline root, CalculateCount calculatecount) {
		this.task = calculatecount;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts join function
	 * @param <T>
	 * @param mapright
	 * @param conditioninnerjoin
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <T> MapPairIgnite<T, T> join(AbstractPipeline mapright,
			JoinPredicate<Tuple2<I1, I2>, Tuple2<I1, I2>> conditioninnerjoin) throws PipelineException {
		if (Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		if (Objects.isNull(conditioninnerjoin)) {
			throw new PipelineException(PipelineConstants.INNERJOINCONDITION);
		}
		var mp = new MapPairIgnite(root, conditioninnerjoin);
		this.childs.add(mp);
		mp.parents.add(this);
		mapright.childs.add(mp);
		mp.parents.add(mapright);
		root.mdsroots.add(mapright.root);
		return mp;
	}
	
	/**
	 * MapPairIgnite constructor for Join
	 * @param root
	 * @param join
	 */
	protected MapPairIgnite(AbstractPipeline root,
			Join join)  {
		this.task = join;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask=task;
	}
	
	/**
	 * MapPairIgnite accepts Join
	 * @param <I3>
	 * @param mapright
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3> MapPairIgnite<I1,Tuple2<I2,I3>> join(MapPairIgnite<I1,I3> mapright) throws PipelineException  {
		if(Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		var mp = new MapPairIgnite(root, new Join());
		this.childs.add(mp);
		mp.parents.add(this);
		mapright.childs.add(mp);
		mp.parents.add(mapright);
		root.mdsroots.add(mapright.root);
		return mp;
	}
	
	
	/**
	 * MapPairIgnite constructor for Left Join
	 * @param root
	 * @param join
	 */
	protected MapPairIgnite(AbstractPipeline root,
			LeftJoin join)  {
		this.task = join;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask=task;
	}
	
	/**
	 * MapPairIgnite accepts Right Join
	 * @param <I3>
	 * @param mapright
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3> MapPairIgnite<I1,Tuple2<I2,I3>> leftJoin(MapPairIgnite<I1,I3> mapright) throws PipelineException  {
		if(Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		var mp = new MapPairIgnite(root, new LeftJoin());
		this.childs.add(mp);
		mp.parents.add(this);
		mapright.childs.add(mp);
		mp.parents.add(mapright);
		root.mdsroots.add(mapright.root);
		return mp;
	}
	
	
	/**
	 * MapPairIgnite constructor for Left Join
	 * @param root
	 * @param join
	 */
	protected MapPairIgnite(AbstractPipeline root,
			RightJoin join)  {
		this.task = join;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask=task;
	}
	
	/**
	 * MapPairIgnite accepts Right Join
	 * @param <I3>
	 * @param mapright
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3> MapPairIgnite<I1,Tuple2<I2,I3>> rightJoin(MapPairIgnite<I1,I3> mapright) throws PipelineException  {
		if(Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		var mp = new MapPairIgnite(root, new RightJoin());
		this.childs.add(mp);
		mp.parents.add(this);
		mapright.childs.add(mp);
		mp.parents.add(mapright);
		root.mdsroots.add(mapright.root);
		return mp;
	}
	
	/**
	 * MapPairIgnite constructor for distinct 
	 * @param root
	 * @param distinct
	 */
	private MapPairIgnite(AbstractPipeline root, Distinct distinct) {
		this.task = distinct;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts distinct
	 * @return MapPairIgnite objet
	 */
	public MapPairIgnite<I1, I2> distinct() {
		var distinct = new Distinct();
		var map = new MapPairIgnite<I1, I2>(root, distinct);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}

	/**
	 * MapPairIgnite constructor for Predicate for filter.
	 * @param root
	 * @param predicate
	 */
	private MapPairIgnite(AbstractPipeline root, PredicateSerializable<I1> predicate) {
		this.task = predicate;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the Predicate for filter.
	 * @param predicate
	 * @return MapPairIgnite object
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I1> filter(PredicateSerializable<? super Tuple2> predicate)
			throws PipelineException {
		if (Objects.isNull(predicate)) {
			throw new PipelineException(PipelineConstants.PREDICATENULL);
		}
		var filter = new MapPairIgnite(root, predicate);
		this.childs.add(filter);
		filter.parents.add(this);
		return filter;
	}

	/**
	 * MapPairIgnite constructor for union.
	 * @param root
	 * @param unionfunction
	 */
	private MapPairIgnite(AbstractPipeline root, UnionFunction unionfunction) {
		this.task = unionfunction;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the union mappair object.
	 * @param union
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I1> union(MapPairIgnite union) throws PipelineException {
		if (Objects.isNull(union)) {
			throw new PipelineException(PipelineConstants.UNIONNULL);
		}
		var unionfunction = new UnionFunction();
		var unionchild = new MapPairIgnite(root, unionfunction);
		this.childs.add(unionchild);
		unionchild.parents.add(this);
		union.childs.add(unionchild);
		unionchild.parents.add(union);
		root.mdsroots.add(union.root);
		return unionchild;
	}

	/**
	 * MapPairIgnite constructor for intersection function.
	 * @param root
	 * @param intersectionfunction
	 */
	private MapPairIgnite(AbstractPipeline root, IntersectionFunction intersectionfunction) {
		this.task = intersectionfunction;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accept the intersection mappair object.
	 * @param intersection
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I1> intersection(MapPairIgnite intersection) throws PipelineException {
		if (Objects.isNull(intersection)) {
			throw new PipelineException(PipelineConstants.INTERSECTIONNULL);
		}
		var intersectionfunction = new IntersectionFunction();
		var intersectionchild = new MapPairIgnite(root, intersectionfunction);
		this.childs.add(intersectionchild);
		intersectionchild.parents.add(this);
		intersection.childs.add(intersectionchild);
		intersectionchild.parents.add(intersection);
		root.mdsroots.add(intersection.root);
		return intersectionchild;
	}

	/**
	 * MapPairIgnite which accepts the MapPairIgnite function.
	 * @param <I3>
	 * @param <I4>
	 * @param pf
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3, I4> MapPairIgnite<I3, I4> mapToPair(MapToPairFunction<? super Tuple2<I1, I2>, Tuple2<I3, I4>> pf)
			throws PipelineException {
		if (Objects.isNull(pf)) {
			throw new PipelineException(PipelineConstants.MAPPAIRNULL);
		}
		var mappair = new MapPairIgnite(root, pf);
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}

	/**
	 * MapPairIgnite constructor for sample.
	 * @param root
	 * @param sampleintegersupplier
	 */
	private MapPairIgnite(AbstractPipeline root, SampleSupplierInteger sampleintegersupplier) {
		this.task = sampleintegersupplier;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the sample. 
	 * @param numsample
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> sample(Integer numsample) throws PipelineException {
		if (Objects.isNull(numsample)) {
			throw new PipelineException(PipelineConstants.SAMPLENULL);
		}
		var sampleintegersupplier = new SampleSupplierInteger(numsample);
		var samplesupplier = new MapPairIgnite(root, sampleintegersupplier);
		this.childs.add(samplesupplier);
		samplesupplier.parents.add(this);
		return samplesupplier;
	}

	/**
	 * MapPairIgnite accepts the right outer join. 
	 * @param mappair
	 * @param conditionrightouterjoin
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> rightOuterjoin(AbstractPipeline mappair,
			RightOuterJoinPredicate<Tuple2<I1, I2>, Tuple2<I1, I2>> conditionrightouterjoin)
			throws PipelineException {
		if (Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOIN);
		}
		if (Objects.isNull(conditionrightouterjoin)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOINCONDITION);
		}
		var mdp = new MapPairIgnite(root, conditionrightouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappair.childs.add(mdp);
		mdp.parents.add(mappair);
		root.mdsroots.add(mappair.root);
		return mdp;
	}

	/**
	 * MapPairIgnite accepts the left outer join.
	 * @param mappair
	 * @param conditionleftouterjoin
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> leftOuterjoin(AbstractPipeline mappair,
			LeftOuterJoinPredicate<Tuple2<I1, I2>, Tuple2<I1, I2>> conditionleftouterjoin)
			throws PipelineException {
		if (Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOIN);
		}
		if (Objects.isNull(conditionleftouterjoin)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOINCONDITION);
		}
		var mdp = new MapPairIgnite(root, conditionleftouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappair.childs.add(mdp);
		mdp.parents.add(mappair);
		root.mdsroots.add(mappair.root);
		return mdp;
	}

	/**
	 * MapPairIgnite constructor for FlatMap function.
	 * @param root
	 * @param fmf
	 */
	private MapPairIgnite(AbstractPipeline root, FlatMapFunction fmf) {
		this.task = fmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the flatmap function.
	 * @param <T>
	 * @param fmf
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3,I4> MapPairIgnite<I3, I4> flatMap(FlatMapFunction<? super Tuple2<I1, I2>, ? extends Tuple2<I3,I4>> fmf)
			throws PipelineException {
		if (Objects.isNull(fmf)) {
			throw new PipelineException(PipelineConstants.FLATMAPNULL);
		}
		var mdp = new MapPairIgnite(root, fmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * MapPairIgnite accepts the TupleFlatMap function.
	 * @param <I3>
	 * @param <I4>
	 * @param pfmf
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3, I4> MapPairIgnite<I3, I4> flatMapToTuple(
			TupleFlatMapFunction<? super I1, ? extends Tuple2<I3, I4>> pfmf) throws PipelineException {
		if (Objects.isNull(pfmf)) {
			throw new PipelineException(PipelineConstants.FLATMAPPAIRNULL);
		}
		var mdp = new MapPairIgnite(root, pfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * MapPairIgnite constructor for LongTupleFlatMap function.
	 * @param root
	 * @param lfmf
	 */
	private MapPairIgnite(AbstractPipeline root, LongTupleFlatMapFunction lfmf) {
		this.task = lfmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the LongTupleFlatMap function.
	 * @param lfmf
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<Long, Long> flatMapToLong(LongTupleFlatMapFunction<Tuple2<I1, I2>> lfmf)
			throws PipelineException {
		if (Objects.isNull(lfmf)) {
			throw new PipelineException(PipelineConstants.LONGFLATMAPNULL);
		}
		var mdp = new MapPairIgnite(root, lfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * MapPairIgnite constructor for DoubleTupleFlatMap function.
	 * @param root
	 * @param dfmf
	 */
	private MapPairIgnite(AbstractPipeline root, DoubleTupleFlatMapFunction dfmf) {
		this.task = dfmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the DoubleTupleFlatMap function.
	 * @param dfmf
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<Double, Double> flatMapToDouble(DoubleTupleFlatMapFunction<Tuple2<I1, I2>> dfmf)
			throws PipelineException {
		if (Objects.isNull(dfmf)) {
			throw new PipelineException(PipelineConstants.DOUBLEFLATMAPNULL);
		}
		var mdp = new MapPairIgnite(root, dfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * MapPairIgnite accepts the peek function.
	 * @param consumer
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I1> peek(PeekConsumer consumer) throws PipelineException {
		if (Objects.isNull(consumer)) {
			throw new PipelineException(PipelineConstants.PEEKNULL);
		}
		var map = new MapPairIgnite(root, consumer);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}

	/**
	 * MapPairIgnite constructor for the sorting.
	 * @param root
	 * @param sortedcomparator
	 */
	private MapPairIgnite(AbstractPipeline root, SortedComparator<Tuple2<I1, I2>> sortedcomparator) {
		this.task = sortedcomparator;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite constructor for the right outer join.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param mdp
	 * @param rightouterjoinpredicate
	 */
	protected <T, O1, O2> MapPairIgnite(MapPairIgnite mdp, RightOuterJoinPredicate rightouterjoinpredicate) {
		this.task = rightouterjoinpredicate;
		this.root = mdp.root;
		root.mdsroots.add(mdp.root);
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite constructor for the left outer join.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param mdp
	 * @param leftouterjoinpredicate
	 */
	protected <T, O1, O2> MapPairIgnite(MapPairIgnite mdp, LeftOuterJoinPredicate leftouterjoinpredicate) {
		this.task = leftouterjoinpredicate;
		this.root = mdp.root;
		root.mdsroots.add(mdp.root);
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the sort function.
	 * @param sortedcomparator
	 * @return MapPairIgnite object
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> sorted(SortedComparator<? super Tuple2> sortedcomparator)
			throws PipelineException {
		if (Objects.isNull(sortedcomparator)) {
			throw new PipelineException(PipelineConstants.SORTEDNULL);
		}
		var map = new MapPairIgnite(root, sortedcomparator);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}

	/**
	 * MapPairIgnite constructor for the Reducefunction.
	 * @param <O1>
	 * @param root
	 * @param rf
	 */
	private <O1> MapPairIgnite(AbstractPipeline root, ReduceByKeyFunction<I1> rf) {
		this.task = rf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite constructor for the Coalescefunction.
	 * @param <O1>
	 * @param root
	 * @param cf
	 */
	private <O1> MapPairIgnite(AbstractPipeline root, Coalesce<I1> cf) {
		this.task = cf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the coalesce function.
	 * @param partition
	 * @param cf
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> coalesce(int partition, CoalesceFunction<I2> cf) throws PipelineException {
		if (Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.COALESCENULL);
		}
		var mappaircoalesce = new MapPairIgnite(root, new Coalesce(partition, cf));
		this.childs.add(mappaircoalesce);
		mappaircoalesce.parents.add(this);
		return mappaircoalesce;
	}

	/**
	 * MapPairIgnite accepts the reducefunction.
	 * @param rf
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> reduceByKey(ReduceByKeyFunction<I2> rf) throws PipelineException {
		if (Objects.isNull(rf)) {
			throw new PipelineException(PipelineConstants.REDUCENULL);
		}
		var mappair = new MapPairIgnite(root, rf);
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}

	/**
	 * MapPairIgnite constructor for the fold function.
	 * @param <O1>
	 * @param root
	 * @param cf
	 */
	private <O1> MapPairIgnite(AbstractPipeline root, FoldByKey cf) {
		this.task = cf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the fold left.
	 * @param value
	 * @param rf
	 * @param partition
	 * @param cf
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> foldLeft(Object value, ReduceByKeyFunction<I2> rf, int partition, CoalesceFunction<I2> cf)
			throws PipelineException {
		if (Objects.isNull(rf)) {
			throw new PipelineException(PipelineConstants.FOLDLEFTREDUCENULL);
		}
		if (Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.FOLDLEFTCOALESCENULL);
		}
		var mappair = new MapPairIgnite(root, new FoldByKey(value, rf, true));
		this.childs.add(mappair);
		mappair.parents.add(this);
		if (cf != null) {
			var mappaircoalesce = new MapPairIgnite(root, new Coalesce(partition, cf));
			mappair.childs.add(mappaircoalesce);
			mappaircoalesce.parents.add(mappair);
			return mappaircoalesce;
		}
		return mappair;
	}

	/**
	 * MapPairIgnite accepts the fold right.
	 * @param value
	 * @param rf
	 * @param partition
	 * @param cf
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	public MapPairIgnite<I1, I2> foldRight(Object value, ReduceByKeyFunction<I2> rf, int partition, CoalesceFunction<I2> cf)
			throws PipelineException {
		if (Objects.isNull(rf)) {
			throw new PipelineException(PipelineConstants.FOLDRIGHTREDUCENULL);
		}
		if (Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.FOLDRIGHTCOALESCENULL);
		}
		var mappair = new MapPairIgnite(root, new FoldByKey(value, rf, false));
		this.childs.add(mappair);
		mappair.parents.add(this);
		if (cf != null) {
			var mappaircoalesce = new MapPairIgnite(root, new Coalesce(partition, cf));
			mappair.childs.add(mappaircoalesce);
			mappaircoalesce.parents.add(mappair);
			return mappaircoalesce;
		}
		return mappair;
	}

	/**
	 * MapPairIgnite constructor for the GroupByKey function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param gbkf
	 */
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root, GroupByKeyFunction gbkf) {
		this.task = gbkf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the groupbykey.
	 * @return MapPairIgnite object.
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, List<I2>> groupByKey() {

		var mappair = new MapPairIgnite(root, new GroupByKeyFunction());
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}

	/**
	 * MapPairIgnite accepts the cogroup mappair object.
	 * @param mappair2
	 * @return MapPairIgnite object.
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public MapPairIgnite<I1, Tuple2<List<I2>, List<I2>>> cogroup(MapPairIgnite<I1, I2> mappair2) throws Exception {
		MapPairIgnite<I1, List<I2>> gbkleft = this.groupByKey();
		Thread.sleep(500);
		var gbkright = mappair2.groupByKey();
		var mdp = new MapPairIgnite(root,
				(LeftOuterJoinPredicate<Tuple2<I1, List<I2>>, Tuple2<I1, List<I2>>>) ((Tuple2<I1, List<I2>> tuple1,
						Tuple2<I1, List<I2>> tuple2) -> tuple1.v1.equals(tuple2.v1)));
		gbkleft.childs.add(mdp);
		mdp.parents.add(gbkleft);
		gbkright.childs.add(mdp);
		mdp.parents.add(gbkright);
		root.mdsroots.add(gbkleft.root);
		root.mdsroots.add(gbkright.root);
		return mdp;
	}

	/**
	 * MapPairIgnite constructor for the CountByKey function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param cbkf
	 */
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root, CountByKeyFunction cbkf) {
		this.task = cbkf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the CountByKey function.
	 * @return MapPairIgnite object.
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, Long> countByKey() {
		var mappair = new MapPairIgnite(root, new CountByKeyFunction());
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}
	
	/**
	 * MapPair constructor for the HashPartitoner.
	 * @param <O1>
	 * @param root
	 * @param cf
	 */
	private <O1> MapPairIgnite(AbstractPipeline root,
			HashPartitioner hp)  {
		this.task = hp;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the HashPartiton object.
	 * @return MapPair object.
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<Integer,List<Tuple2<I1,I2>>> partition(HashPartitioner partitoner)  {
		var mappairignite = new MapPairIgnite(root, partitoner);
		this.childs.add(mappairignite);
		mappairignite.parents.add(this);
		return mappairignite;
	}

	/**
	 * MapPairIgnite constructor for the CountByValue function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param cbkf
	 */
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root, CountByValueFunction cbkf) {
		this.task = cbkf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the CountByValue function.
	 * @return MapPairIgnite object.
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I2, Long> countByValue() {
		var mappair = new MapPairIgnite(root, new CountByValueFunction());
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}

	
	/**
	 * MapPairIgnite constructor for the TupleFlatMap function.
	 * @param root
	 * @param tfmf
	 */
	protected MapPairIgnite(AbstractPipeline root, TupleFlatMapFunction<I1, Tuple> tfmf) {
		this.task = tfmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * This function caches the data and returns MapPairIgnite object.
	 * @param isresultsneeded
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	public MapPairIgnite<I1, I2> cache(boolean isresultsneeded) throws PipelineException {
		try {
			log.debug("Caching...");
			var mdpcached = new MapPairIgnite<I1, I2>();
			Job job = null;
			if (root instanceof IgnitePipeline mdpi) {
				mdpi.finaltasks.clear();
				mdpi.finaltasks.add(mdpi.finaltask);
				mdpi.mdsroots.add(root);
				job = mdpi.cacheInternal(isresultsneeded,null,null);
				mdpcached.pipelineconfig = mdpi.pipelineconfig;
				log.debug("Cached....");
			} else if (root instanceof MapPairIgnite mti) {
				mti.finaltasks.clear();
				mti.finaltasks.add(mti.finaltask);
				mti.mdsroots.add(root);
				job = mti.cacheInternal(isresultsneeded,null,null);
				mdpcached.pipelineconfig = mti.pipelineconfig;
				log.debug("Cached....");
			}

			mdpcached.job = job;
			return mdpcached;
		} catch (Exception ex) {
			log.error(PipelineConstants.PIPELINECOLLECTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOLLECTERROR, ex);
		}
	}

	/**
	 * This method saves the result to the hdfs. 
	 * @param uri
	 * @param path
	 * @throws PipelineException
	 */
	public void saveAsTextFile(URI uri, String path) throws PipelineException {
		
			if (root instanceof IgnitePipeline mdpi) {
				mdpi.finaltasks.clear();
				mdpi.finaltasks.add(mdpi.finaltask);
				mdpi.mdsroots.add(root);
				job = mdpi.cacheInternal(true,uri,path);
				log.debug("Cached....");
			} else if (root instanceof MapPairIgnite mti) {
				mti.finaltasks.clear();
				mti.finaltasks.add(mti.finaltask);
				mti.mdsroots.add(root);
				job = mti.cacheInternal(true,uri,path);
				log.debug("Cached....");
			}
			
	}

	
	/**
	 * This function executes the forEach tasks.
	 * @param consumer
	 * @param supplier
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public void forEach(Consumer<List<Tuple2>> consumer, IntSupplier supplier) throws PipelineException {
		try {
			Job job = null;
			if (root instanceof IgnitePipeline mdpi) {
				mdpi.finaltasks.clear();
				mdpi.finaltasks.add(mdpi.finaltask);
				mdpi.mdsroots.add(root);
				job = mdpi.cacheInternal(true,null,null);
				log.debug("Cached....");
			} else if (root instanceof MapPairIgnite mti) {
				mti.finaltasks.clear();
				mti.finaltasks.add(mti.finaltask);
				mti.mdsroots.add(root);
				job = mti.cacheInternal(true,null,null);
				log.debug("Cached....");
			}
			var results = (List<?>) job.getResults();
			results.stream().forEach((Consumer) consumer);

		} catch (Exception e) {
			log.error(PipelineConstants.PIPELINEFOREACHERROR, e);
			throw new PipelineException(PipelineConstants.PIPELINEFOREACHERROR, e);
		}
	}

	/**
	 * This function executes the count tasks.
	 * @param supplier
	 * @return object
	 * @throws PipelineException
	 */
	public Object count(NumPartitions supplier) throws PipelineException {
		try {
			if (root instanceof IgnitePipeline mdpi) {
				var mdp = new IgnitePipeline(root, new CalculateCount());
				mdp.parents.add(this);
				this.childs.add(mdp);
				mdpi.finaltasks.clear();
				mdpi.finaltasks.add(mdpi.finaltask);
				mdpi.mdsroots.add(root);
				job = mdpi.cacheInternal(true,null,null);
				log.debug("Cached....");
			} else if (root instanceof MapPairIgnite mti) {
				var mdp = new MapPairIgnite(root, new CalculateCount());
				mdp.parents.add(this);
				this.childs.add(mdp);
				mti.finaltasks.clear();
				mti.finaltasks.add(mti.finaltask);
				mti.mdsroots.add(root);
				job = mti.cacheInternal(true,null,null);
				log.debug("Cached....");
			}
			return (List) job.getResults();
		} catch (Exception ex) {
			log.error(PipelineConstants.PIPELINECOUNTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOUNTERROR, ex);
		}
	}
	
	/**
	 * This function executes the collect tasks.
	 * @param toexecute
	 * @param supplier
	 * @return list
	 * @throws PipelineException
	 */
	public Object collect(boolean toexecute, IntSupplier supplier) throws PipelineException {
		try {
			if (root instanceof IgnitePipeline mdpi) {
				mdpi.finaltasks.clear();
				mdpi.finaltasks.add(mdpi.finaltask);
				mdpi.mdsroots.add(root);
				job = mdpi.cacheInternal(true,null,null);
				log.debug("Cached....");
			} else if (root instanceof MapPairIgnite mti) {
				var mdp = new IgnitePipeline(root, new CalculateCount());
				mdp.parents.add(this);
				this.childs.add(mdp);
				mti.finaltasks.clear();
				mti.finaltasks.add(mti.finaltask);
				mti.mdsroots.add(root);
				job = mti.cacheInternal(true,null,null);
				log.debug("Cached....");
			}
			return (List) job.getResults();
		} catch (Exception ex) {
			log.error(PipelineConstants.PIPELINECOUNTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOUNTERROR, ex);
		}
	}
	@Override
	public String toString() {
		return "MapPairIgnite [task=" + task + "]";
	}

}
