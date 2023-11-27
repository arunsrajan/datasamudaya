package com.github.datasamudaya.stream;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
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
 * MapPair holding the MapPairFunction, JoinPair, ReduceByKey, etc... functions
 * runs on standalone executors.
 * @param <I1>
 * @param <I2>
 */
@SuppressWarnings("rawtypes")
public sealed class MapPair<I1,I2> extends AbstractPipeline permits MapValues{
	private static Logger log = Logger.getLogger(MapPair.class);
	
	public MapPair() {
		
	}
	
	/**
	 * MapPair constructor for rightouterjoin.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param conditionrightouterjoin
	 */
	@SuppressWarnings({"unchecked"})
	private <T, O1, O2> MapPair(AbstractPipeline root,
			RightOuterJoinPredicate<? super I1, ? super I2> conditionrightouterjoin) {
		this.task = conditionrightouterjoin;
		this.root = root;
		root.finaltask=task;
	}
	/**
	 * MapPair constructor for left outer join. 
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param conditionleftouterjoin
	 */
	@SuppressWarnings({"unchecked"})
	private <T, O1, O2> MapPair(AbstractPipeline root,
			LeftOuterJoinPredicate<? super I1, ? super I2> conditionleftouterjoin) {
		this.task = conditionleftouterjoin;
		this.root = root;
		root.finaltask=task;
	}

	/**
	 * MapPair constructor for keybyfunction.
	 * @param <I1>
	 * @param <I2>
	 * @param root
	 * @param keybyfunction
	 */
	@SuppressWarnings({"unchecked", "hiding"}) <I1, I2> MapPair(AbstractPipeline root,
			KeyByFunction<I1,I2> keybyfunction) {
		this.task = keybyfunction;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts for Mapvalues
	 * @param <I3>
	 * @param <I4>
	 * @param mvf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public <I3, I4> MapValues<I1, Tuple2<I3, I4>> mapValues(MapValuesFunction<? super I2, ? extends Tuple2<I3, I4>> mvf) throws PipelineException {
		if(Objects.isNull(mvf)) {
			throw new PipelineException(PipelineConstants.MAPVALUESNULL);
		}
		var mapvalues = new MapValues(root, mvf);
		this.childs.add(mapvalues);
		mapvalues.parents.add(this);
		return mapvalues;
	}
	
	/**
	 * MapPair constructor for MapPair function.
	 * @param <I3>
	 * @param <I4>
	 * @param root
	 * @param pf
	 */
	@SuppressWarnings({"unchecked"})
	protected <I3, I4> MapPair(AbstractPipeline root,
			MapToPairFunction<? super I2, ? extends Tuple2<? super I3,? super I4>> pf) {
		this.task = pf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair map function accepting MapPairFunction.
	 * @param <T>
	 * @param map
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public <T> StreamPipeline<T> map(MapFunction<? super Tuple2<I1, I2> , ? extends T> map) throws PipelineException {
		if(Objects.isNull(map)) {
			throw new PipelineException(PipelineConstants.MAPFUNCTIONNULL);
		}
		var mapobj = new StreamPipeline(root, map);
		this.childs.add(mapobj);
		mapobj.parents.add(this);
		return mapobj;
	}
	
	/**
	 * MapPair constructor for Peek function
	 * @param <T>
	 * @param root
	 * @param peekConsumer
	 */
	@SuppressWarnings({"unchecked"})
	private <T> MapPair(AbstractPipeline root,
			PeekConsumer peekConsumer) {
		this.task = peekConsumer;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask=task;
	}
	
	/**
	 * MapPair constructor for Join function
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param joinpredicate
	 */
	@SuppressWarnings({"unchecked"})
	private <T, O1, O2> MapPair(AbstractPipeline root,
			JoinPredicate joinpredicate) {
		this.task = joinpredicate;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask=task;
	}
	
	/**
	 * MapPair constructor for count
	 * @param root
	 * @param calculatecount
	 */
	@SuppressWarnings({"unchecked"})
	protected MapPair(AbstractPipeline root,
			CalculateCount calculatecount) {
		this.task = calculatecount;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts join function
	 * @param <T>
	 * @param mapright
	 * @param conditioninnerjoin
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public <T> MapPair<T, T> join(AbstractPipeline mapright, JoinPredicate<Tuple2<I1,I2>,Tuple2<I1,I2>> conditioninnerjoin) throws PipelineException {
		if(Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		if(Objects.isNull(conditioninnerjoin)) {
			throw new PipelineException(PipelineConstants.INNERJOINCONDITION);
		}
		var mp = new MapPair(root, conditioninnerjoin);
		this.childs.add(mp);
		mp.parents.add(this);
		mapright.childs.add(mp);
		mp.parents.add(mapright);
		root.mdsroots.add(mapright.root);
		return mp;
	}
	

	/**
	 * MapPair constructor for Join
	 * @param root
	 * @param join
	 */
	protected MapPair(AbstractPipeline root,
			Join join) {
		this.task = join;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts Join
	 * @param <I3>
	 * @param mapright
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public <I3> MapPair<I1, Tuple2<I2, I3>> join(MapPair<I1, I3> mapright) throws PipelineException {
		if(Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		var mp = new MapPair(root, new Join());
		this.childs.add(mp);
		mp.parents.add(this);
		mapright.childs.add(mp);
		mp.parents.add(mapright);
		root.mdsroots.add(mapright.root);
		return mp;
	}
	
	
	/**
	 * MapPair constructor for Left Join
	 * @param root
	 * @param join
	 */
	protected MapPair(AbstractPipeline root,
			LeftJoin join) {
		this.task = join;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts Right Join
	 * @param <I3>
	 * @param mapright
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public <I3> MapPair<I1, Tuple2<I2, I3>> leftJoin(MapPair<I1, I3> mapright) throws PipelineException {
		if(Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		var mp = new MapPair(root, new LeftJoin());
		this.childs.add(mp);
		mp.parents.add(this);
		mapright.childs.add(mp);
		mp.parents.add(mapright);
		root.mdsroots.add(mapright.root);
		return mp;
	}
	
	
	/**
	 * MapPair constructor for Left Join
	 * @param root
	 * @param join
	 */
	protected MapPair(AbstractPipeline root,
			RightJoin join) {
		this.task = join;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts Right Join
	 * @param <I3>
	 * @param mapright
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public <I3> MapPair<I1, Tuple2<I2, I3>> rightJoin(MapPair<I1, I3> mapright) throws PipelineException {
		if(Objects.isNull(mapright)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		var mp = new MapPair(root, new RightJoin());
		this.childs.add(mp);
		mp.parents.add(this);
		mapright.childs.add(mp);
		mp.parents.add(mapright);
		root.mdsroots.add(mapright.root);
		return mp;
	}
	
	/**
	 * MapPair constructor for distinct 
	 * @param root
	 * @param distinct
	 */
	@SuppressWarnings({"unchecked"})
	private MapPair(AbstractPipeline root,
			Distinct distinct) {
		this.task = distinct;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts distinct
	 * @return MapPair objet
	 */
	public MapPair<I1, I2> distinct() {
		var distinct = new Distinct();
		var map = new MapPair<I1, I2>(root, distinct);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}
	
	/**
	 * MapPair constructor for Predicate for filter.
	 * @param root
	 * @param predicate
	 */
	@SuppressWarnings({"unchecked"})
	private MapPair(AbstractPipeline root,
			PredicateSerializable<I1> predicate) {
		this.task = predicate;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the Predicate for filter.
	 * @param predicate
	 * @return MapPair object
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I1> filter(PredicateSerializable<? super Tuple2> predicate) throws PipelineException {
		if(Objects.isNull(predicate)) {
			throw new PipelineException(PipelineConstants.PREDICATENULL);
		}
		var filter = new MapPair(root, predicate);
		this.childs.add(filter);
		filter.parents.add(this);
		return filter;
	}
	
	/**
	 * MapPair constructor for union.
	 * @param root
	 * @param unionfunction
	 */
	@SuppressWarnings({"unchecked"})
	private MapPair(AbstractPipeline root,
			UnionFunction unionfunction) {
		this.task = unionfunction;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the union mappair object.
	 * @param union
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I1> union(MapPair union) throws PipelineException {
		if(Objects.isNull(union)) {
			throw new PipelineException(PipelineConstants.UNIONNULL);
		}
		var unionfunction = new UnionFunction();
		var unionchild =new  MapPair(root, unionfunction);
		this.childs.add(unionchild);
		unionchild.parents.add(this);
		union.childs.add(unionchild);
		unionchild.parents.add(union);
		root.mdsroots.add(union.root);
		return unionchild;
	}
	
	/**
	 * MapPair constructor for intersection function.
	 * @param root
	 * @param intersectionfunction
	 */
	@SuppressWarnings({"unchecked"})
	private MapPair(AbstractPipeline root,
			IntersectionFunction intersectionfunction) {
		this.task = intersectionfunction;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accept the intersection mappair object.
	 * @param intersection
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I1> intersection(MapPair intersection) throws PipelineException {
		if(Objects.isNull(intersection)) {
			throw new PipelineException(PipelineConstants.INTERSECTIONNULL);
		}
		var intersectionfunction = new IntersectionFunction();
		var intersectionchild =new  MapPair(root, intersectionfunction);
		this.childs.add(intersectionchild);
		intersectionchild.parents.add(this);
		intersection.childs.add(intersectionchild);
		intersectionchild.parents.add(intersection);
		root.mdsroots.add(intersection.root);
		return intersectionchild;
	}
	
	/**
	 * MapPair which accepts the MapPair function.
	 * @param <I3>
	 * @param <I4>
	 * @param pf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public <I3, I4> MapPair<I3, I4> mapToPair(MapToPairFunction<? super Tuple2<I1, I2>, Tuple2<I3, I4>> pf) throws PipelineException {
		if(Objects.isNull(pf)) {
			throw new PipelineException(PipelineConstants.MAPPAIRNULL);
		}
		var mappair = new MapPair(root, pf);
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}
	
	/**
	 * MapPair constructor for sample.
	 * @param root
	 * @param sampleintegersupplier
	 */
	@SuppressWarnings({"unchecked"})
	private MapPair(AbstractPipeline root,
			SampleSupplierInteger sampleintegersupplier) {
		this.task = sampleintegersupplier;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the sample. 
	 * @param numsample
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I2> sample(Integer numsample) throws PipelineException {
		if(Objects.isNull(numsample)) {
			throw new PipelineException(PipelineConstants.SAMPLENULL);
		}
		var sampleintegersupplier = new SampleSupplierInteger(numsample);
		var samplesupplier = new MapPair(root, sampleintegersupplier);
		this.childs.add(samplesupplier);
		samplesupplier.parents.add(this);
		return samplesupplier;
	}
	
	/**
	 * MapPair accepts the right outer join. 
	 * @param mappair
	 * @param conditionrightouterjoin
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I2> rightOuterjoin(AbstractPipeline mappair, RightOuterJoinPredicate<Tuple2<I1,I2>,Tuple2<I1,I2>> conditionrightouterjoin) throws PipelineException {
		if(Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOIN);
		}
		if(Objects.isNull(conditionrightouterjoin)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOINCONDITION);
		}
		var mdp = new MapPair(root, conditionrightouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappair.childs.add(mdp);
		mdp.parents.add(mappair);
		root.mdsroots.add(mappair.root);
		return mdp;
	}
	
	/**
	 * MapPair accepts the left outer join.
	 * @param mappair
	 * @param conditionleftouterjoin
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I2> leftOuterjoin(AbstractPipeline mappair, LeftOuterJoinPredicate<Tuple2<I1,I2>,Tuple2<I1,I2>> conditionleftouterjoin) throws PipelineException {
		if(Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOIN);
		}
		if(Objects.isNull(conditionleftouterjoin)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOINCONDITION);
		}
		var mdp = new MapPair(root, conditionleftouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappair.childs.add(mdp);
		mdp.parents.add(mappair);
		root.mdsroots.add(mappair.root);
		return mdp;
	}
	
	/**
	 * MapPair constructor for FlatMap function.
	 * @param root
	 * @param fmf
	 */
	@SuppressWarnings({"unchecked"})
	private MapPair(AbstractPipeline root,
			FlatMapFunction fmf) {
		this.task = fmf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the flatmap function.
	 * @param <T>
	 * @param fmf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public <I3, I4> MapPair<I3, I4> flatMap(FlatMapFunction<? super Tuple2<I1, I2>, ? extends Tuple2<I3, I4>> fmf) throws PipelineException {
		if(Objects.isNull(fmf)) {
			throw new PipelineException(PipelineConstants.FLATMAPNULL);
		}
		var mdp = new MapPair(root, fmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MapPair accepts the TupleFlatMap function.
	 * @param <I3>
	 * @param <I4>
	 * @param pfmf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public <I3, I4> MapPair<I3, I4> flatMapToTuple(TupleFlatMapFunction<? super I1, ? extends Tuple2<I3, I4>> pfmf) throws PipelineException {
		if(Objects.isNull(pfmf)) {
			throw new PipelineException(PipelineConstants.FLATMAPPAIRNULL);
		}
		var mdp = new MapPair(root, pfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MapPair constructor for LongTupleFlatMap function.
	 * @param root
	 * @param lfmf
	 */
	@SuppressWarnings({"unchecked"})
	private MapPair(AbstractPipeline root,
			LongTupleFlatMapFunction lfmf) {
		this.task = lfmf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the LongTupleFlatMap function.
	 * @param lfmf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<Long, Long> flatMapToLong(LongTupleFlatMapFunction<Tuple2<I1, I2>> lfmf) throws PipelineException {
		if(Objects.isNull(lfmf)) {
			throw new PipelineException(PipelineConstants.LONGFLATMAPNULL);
		}
		var mdp = new MapPair(root, lfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MapPair constructor for DoubleTupleFlatMap function.
	 * @param root
	 * @param dfmf
	 */
	@SuppressWarnings({"unchecked"})
	private MapPair(AbstractPipeline root,
			DoubleTupleFlatMapFunction dfmf) {
		this.task = dfmf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the DoubleTupleFlatMap function.
	 * @param dfmf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<Double, Double> flatMapToDouble(DoubleTupleFlatMapFunction<Tuple2<I1, I2>> dfmf) throws PipelineException {
		if(Objects.isNull(dfmf)) {
			throw new PipelineException(PipelineConstants.DOUBLEFLATMAPNULL);
		}
		var mdp = new MapPair(root, dfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MapPair accepts the peek function.
	 * @param consumer
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I1> peek(PeekConsumer consumer) throws PipelineException {
		if(Objects.isNull(consumer)) {
			throw new PipelineException(PipelineConstants.PEEKNULL);
		}
		var map = new MapPair(root, consumer);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}
	
	/**
	 * MapPair constructor for the sorting.
	 * @param root
	 * @param sortedcomparator
	 */
	@SuppressWarnings({"unchecked"})
	private MapPair(AbstractPipeline root,
			SortedComparator<Tuple2<I1,I2>> sortedcomparator) {
		this.task = sortedcomparator;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair constructor for the right outer join.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param mdp
	 * @param rightouterjoinpredicate
	 */
	@SuppressWarnings({"unchecked"})
	protected <T, O1, O2> MapPair(MapPair mdp,
			RightOuterJoinPredicate rightouterjoinpredicate) {
		this.task = rightouterjoinpredicate;
		this.root = mdp.root;
		root.mdsroots.add(mdp.root);
		root.finaltask=task;
	}
	
	/**
	 * MapPair constructor for the left outer join.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param mdp
	 * @param leftouterjoinpredicate
	 */
	@SuppressWarnings({"unchecked"})
	protected <T, O1, O2> MapPair(MapPair mdp,
			LeftOuterJoinPredicate leftouterjoinpredicate) {
		this.task = leftouterjoinpredicate;
		this.root = mdp.root;
		root.mdsroots.add(mdp.root);
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the sort function.
	 * @param sortedcomparator
	 * @return MapPair object
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I2> sorted(SortedComparator<? super Tuple2> sortedcomparator) throws PipelineException {
		if(Objects.isNull(sortedcomparator)) {
			throw new PipelineException(PipelineConstants.SORTEDNULL);
		}
		var map = new MapPair(root, sortedcomparator);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}
	
	/**
	 * MapPair constructor for the Reducefunction.
	 * @param <O1>
	 * @param root
	 * @param rf
	 */
	@SuppressWarnings({"unchecked"})
	private <O1> MapPair(AbstractPipeline root,
			ReduceByKeyFunction<I1> rf) {
		this.task = rf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair constructor for the Coalescefunction.
	 * @param <O1>
	 * @param root
	 * @param cf
	 */
	@SuppressWarnings({"unchecked"})
	private <O1> MapPair(AbstractPipeline root,
			Coalesce<I1> cf) {
		this.task = cf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the coalesce function.
	 * @param partition
	 * @param cf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I2> coalesce(int partition, CoalesceFunction<I2> cf) throws PipelineException {
		if(Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.COALESCENULL);
		}
		var mappaircoalesce = new MapPair(root, new Coalesce(partition, cf));
		this.childs.add(mappaircoalesce);
		mappaircoalesce.parents.add(this);
		return mappaircoalesce;
	}
		
	/**
	 * MapPair accepts the coalesce partition number.
	 * @param partition
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I2> coalesce(int partition) throws PipelineException {
		var mappaircoalesce = new MapPair(root, new Coalesce(partition, null));
		this.childs.add(mappaircoalesce);
		mappaircoalesce.parents.add(this);
		return mappaircoalesce;
	}
	
	/**
	 * MapPair with single partition number.
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I2> coalesce() throws PipelineException {
		var mappaircoalesce = new MapPair(root, new Coalesce(1, null));
		this.childs.add(mappaircoalesce);
		mappaircoalesce.parents.add(this);
		return mappaircoalesce;
	}
	
	/**
	 * MapPair accepts the reducefunction.
	 * @param rf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I2> reduceByKey(ReduceByKeyFunction<I2> rf) throws PipelineException {
		if(Objects.isNull(rf)) {
			throw new PipelineException(PipelineConstants.REDUCENULL);
		}
		var mappair = new MapPair(root, rf);
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}
	
	/**
	 * MapPair constructor for the fold function.
	 * @param <O1>
	 * @param root
	 * @param cf
	 */
	@SuppressWarnings({"unchecked"})
	private <O1> MapPair(AbstractPipeline root,
			FoldByKey cf) {
		this.task = cf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the fold left.
	 * @param value
	 * @param rf
	 * @param partition
	 * @param cf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, I2> foldLeft(Object value, ReduceByKeyFunction<I2> rf, int partition, CoalesceFunction<I2> cf) throws PipelineException {
		if(Objects.isNull(rf)) {
			throw new PipelineException(PipelineConstants.FOLDLEFTREDUCENULL);
		}
		if(Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.FOLDLEFTCOALESCENULL);
		}
		var mappair = new MapPair(root, new FoldByKey(value, rf, true));
		this.childs.add(mappair);
		mappair.parents.add(this);
		if(cf!=null) {
			var mappaircoalesce = new MapPair(root, new Coalesce(partition, cf));
			mappair.childs.add(mappaircoalesce);
			mappaircoalesce.parents.add(mappair);
			return mappaircoalesce;
		}
		return mappair;
	}
	
	/**
	 * MapPair accepts the fold right.
	 * @param value
	 * @param rf
	 * @param partition
	 * @param cf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	public MapPair<I1, I2> foldRight(Object value, ReduceByKeyFunction<I2> rf, int partition, CoalesceFunction<I2> cf) throws PipelineException {
		if(Objects.isNull(rf)) {
			throw new PipelineException(PipelineConstants.FOLDRIGHTREDUCENULL);
		}
		if(Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.FOLDRIGHTCOALESCENULL);
		}
		var mappair = new MapPair(root, new FoldByKey(value, rf, false));
		this.childs.add(mappair);
		mappair.parents.add(this);
		if(cf!=null) {
			var mappaircoalesce = new MapPair(root, new Coalesce(partition, cf));
			mappair.childs.add(mappaircoalesce);
			mappaircoalesce.parents.add(mappair);
			return mappaircoalesce;
		}
		return mappair;
	}
	
	/**
	 * MapPair constructor for the GroupByKey function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param gbkf
	 */
	@SuppressWarnings({"unchecked"})
	private <T, O1, O2> MapPair(AbstractPipeline root,
			GroupByKeyFunction gbkf) {
		this.task = gbkf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the groupbykey.
	 * @return MapPair object.
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, List<I2>> groupByKey() {
		
		var mappair = new MapPair(root, new GroupByKeyFunction());
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}
	
	/**
	 * MapPair accepts the cogroup mappair object.
	 * @param mappair2
	 * @return MapPair object.
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public MapPair<I1, Tuple2<List<I2>, List<I2>>> cogroup(MapPair<I1, I2> mappair2) {
		var gbkleft = this.groupByKey();
		var gbkright = mappair2.groupByKey();
		var mdp = new MapPair(root, (LeftOuterJoinPredicate<Tuple2<I1, List<I2>>, Tuple2<I1, List<I2>>>)
				((Tuple2<I1, List<I2>> tuple1, Tuple2<I1, List<I2>> tuple2) -> tuple1.v1.equals(tuple2.v1)));
		gbkleft.childs.add(mdp);
		mdp.parents.add(gbkleft);
		gbkright.childs.add(mdp);
		mdp.parents.add(gbkright);
		root.mdsroots.add(gbkleft.root);
		root.mdsroots.add(gbkright.root);
		return mdp;
	}
	
	/**
	 * MapPair constructor for the CountByKey function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param cbkf
	 */
	@SuppressWarnings({"unchecked"})
	private <T, O1, O2> MapPair(AbstractPipeline root,
			CountByKeyFunction cbkf) {
		this.task = cbkf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the CountByKey function.
	 * @return MapPair object.
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I1, Long> countByKey() {
		
		var mappair = new MapPair(root, new CountByKeyFunction());
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
	private <O1> MapPair(AbstractPipeline root,
			HashPartitioner hp) {
		this.task = hp;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the HashPartition object.
	 * @return MapPair object.
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<Integer, List<Tuple2<I1, I2>>> partition(HashPartitioner partitoner) {
		var mappair = new MapPair(root, partitoner);
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}
	
	/**
	 * MapPair constructor for the CountByValue function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param cbkf
	 */
	@SuppressWarnings({"unchecked"})
	private <T, O1, O2> MapPair(AbstractPipeline root,
			CountByValueFunction cbkf) {
		this.task = cbkf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapPair accepts the CountByValue function.
	 * @return MapPair object.
	 */
	@SuppressWarnings({"unchecked"})
	public MapPair<I2, Long> countByValue() {
		
		var mappair = new MapPair(root, new CountByValueFunction());
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}
	
	/**
	 * MapPair constructor for the TupleFlatMap function.
	 * @param root
	 * @param tfmf
	 */
	@SuppressWarnings({})
	protected MapPair(AbstractPipeline root,
			TupleFlatMapFunction<I1,Tuple> tfmf) {
		this.task = tfmf;
		this.root = root;
		root.finaltask=task;
	}
	
	
	/**
	 * This function executes the collect tasks.
	 * @param toexecute
	 * @param supplier
	 * @return list
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public List collect(boolean toexecute, IntSupplier supplier) throws PipelineException {
		try {
			var mdscollect = (StreamPipeline) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(task);
			root.mdsroots.add(root);
			return mdscollect.collect(toexecute, supplier);
		}
		catch(Exception ex) {
			log.error(PipelineConstants.PIPELINECOLLECTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOLLECTERROR, ex);
		}
	}
	
	/**
	 * This function executes the count tasks.
	 * @param supplier
	 * @return object
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public Object count(IntSupplier supplier) throws PipelineException {
		try {
			var mdp = new StreamPipeline(root, new CalculateCount());
			mdp.parents.add(this);
			this.childs.add(mdp);
			var mdscollect = (StreamPipeline) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdp.task);
			mdscollect.mdsroots.add(root);
			if (mdscollect.supplier != null && supplier != null) {
				if (mdscollect.supplier.getAsInt()!=supplier.getAsInt()) {
					mdscollect.supplier = supplier;
					mdscollect.reexecutealltasks = true;
				} else {
					mdscollect.reexecutealltasks = false;
				}
			} else if (mdscollect.supplier == null && supplier != null) {
				mdscollect.supplier = supplier;
				mdscollect.reexecutealltasks = true;
			} else if (mdscollect.supplier == null && supplier == null) {
				mdscollect.reexecutealltasks = true;
			} else {
				mdscollect.reexecutealltasks = false;
			}
			return mdscollect.collect(true, supplier);
		}
		catch(Exception ex) {
			log.error(PipelineConstants.PIPELINECOUNTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOUNTERROR, ex);
		}
	}
	
	/**
	 * This method saves the result to the hdfs. 
	 * @param uri
	 * @param path
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public void saveAsTextFile(URI uri, String path) throws PipelineException {
		try {
			var mdscollect = (StreamPipeline) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(task);
			root.mdsroots.add(root);
			mdscollect.saveAsTextFile(uri, path);
		} catch (Exception e) {
			log.error(PipelineConstants.FILEIOERROR, e);
			throw new PipelineException(PipelineConstants.FILEIOERROR, e);
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
			var mdscollect = (StreamPipeline) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(task);
			root.mdsroots.add(root);
			mdscollect.forEach(consumer, supplier);
		} catch (Exception e) {
			log.error(PipelineConstants.PIPELINEFOREACHERROR, e);
			throw new PipelineException(PipelineConstants.PIPELINEFOREACHERROR, e);
		}
	}

	@Override
	public String toString() {
		return "MapPair [task=" + task + "]";
	}
	
	
}
