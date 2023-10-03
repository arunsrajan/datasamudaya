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

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.ToIntFunction;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.JSONObject;

import com.github.datasamudaya.common.FileSystemSupport;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.functions.CalculateCount;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.functions.Distinct;
import com.github.datasamudaya.common.functions.DoubleFlatMapFunction;
import com.github.datasamudaya.common.functions.FlatMapFunction;
import com.github.datasamudaya.common.functions.GroupByFunction;
import com.github.datasamudaya.common.functions.IntersectionFunction;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.KeyByFunction;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.LongFlatMapFunction;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.MapToPairFunction;
import com.github.datasamudaya.common.functions.PeekConsumer;
import com.github.datasamudaya.common.functions.PipelineCoalesceFunction;
import com.github.datasamudaya.common.functions.PredicateSerializable;
import com.github.datasamudaya.common.functions.ReduceFunction;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.SToIntFunction;
import com.github.datasamudaya.common.functions.SortedComparator;
import com.github.datasamudaya.common.functions.TupleFlatMapFunction;
import com.github.datasamudaya.common.functions.UnionFunction;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.utils.PipelineConfigValidator;

/**
 * 
 * @author arun
 * The class IgnitePipeline is the class for the core DataPipeline 
 * executes job in ignite server..
 * @param <I1>
 */
public sealed class IgnitePipeline<I1> extends IgniteCommon permits JsonStreamIgnite,CsvStreamIgnite {
	private List<Path> filepaths = new ArrayList<>();
	private String hdfspath;
	private static Logger log = Logger.getLogger(IgnitePipeline.class);

	protected IgnitePipeline() {

	}

	/**
	 * private Constructor for IgnitePipeline 
	 * @param hdfspath
	 * @param folder
	 * @param pipelineconfig
	 * @throws PipelineException
	 */
	private IgnitePipeline(String hdfspath, String folder, PipelineConfig pipelineconfig,
			String protocol) throws PipelineException {
		var validator = new PipelineConfigValidator();
		var errormessages = validator.validate(pipelineconfig);
		if (!errormessages.isEmpty()) {
			var errors = new StringBuilder();
			errormessages.stream().forEach(error -> errors.append(error + DataSamudayaConstants.NEWLINE));
			throw new PipelineException(errors.toString());
		}
		this.pipelineconfig = pipelineconfig;
		pipelineconfig.setMode(DataSamudayaConstants.MODE_DEFAULT);
		this.hdfspath = hdfspath;
		this.folder = folder;
		this.protocol = protocol;
		blocksize = Integer.parseInt(pipelineconfig.getBlocksize()) * 1024 * 1024;
	}

	/**
	 * The function newStreamHDFS creates Data Pipeline
	 * accepts the three params hdfs path, folder in HDFS and
	 * config object.
	 * @param hdfspath
	 * @param folder
	 * @param pipelineconfig
	 * @return IgnitePipeline object
	 * @throws PipelineException
	 */
	public static IgnitePipeline<String> newStreamHDFS(String hdfspath, String folder, PipelineConfig pipelineconfig) throws PipelineException {
		return new IgnitePipeline<String>(hdfspath, folder, pipelineconfig, FileSystemSupport.HDFS);
	}

	/**
	 * Creates Json stream for executing tasks in ignite
	 * @param hdfspath
	 * @param folder
	 * @param pipelineconfig
	 * @return Ignite Json stream
	 * @throws PipelineException
	 */
	public static JsonStreamIgnite<JSONObject> newJsonStreamHDFS(String hdfspath, String folder,PipelineConfig pipelineconfig) throws PipelineException {
		return new IgnitePipeline<String>(hdfspath,folder,pipelineconfig, FileSystemSupport.HDFS).toJson();
		
	}
	
	/**
	 * Creates Csv stream for executing tasks in ignite
	 * @param hdfspath
	 * @param folder
	 * @param pipelineconfig
	 * @param header
	 * @return Ignite Csv Stream
	 * @throws PipelineException
	 */
	public static CsvStreamIgnite<CSVRecord> newCsvStreamHDFS(String hdfspath, String folder,PipelineConfig pipelineconfig,String[] header) throws PipelineException {
		return new IgnitePipeline<String>(hdfspath,folder,pipelineconfig, FileSystemSupport.HDFS).csvWithHeader(header);
		
	}
	
	public static CsvStreamIgnite<Map<String,Object>> newCsvStreamHDFSSQL(String hdfspath, String folder,PipelineConfig pipelineconfig,String[] header, List<SqlTypeName> types, List<String> requiredcolumns) throws PipelineException {
		pipelineconfig.setStorage(STORAGE.COLUMNARSQL);
		return new IgnitePipeline<String>(hdfspath,folder,pipelineconfig, FileSystemSupport.HDFS).csvWithHeader(header, types, requiredcolumns);
		
	}
	
	/**
	 * Creates csv stream object
	 * @param header
	 * @return CsvStream object.
	 */
	private CsvStreamIgnite<CSVRecord> csvWithHeader(String[] header) {
		return new CsvStreamIgnite<>(this,new CsvOptions(header));
	}
	
	/**
	 * Creates csv stream object
	 * @param header
	 * @return CsvStream object.
	 */
	private CsvStreamIgnite<Map<String,Object>> csvWithHeader(String[] header, List<SqlTypeName> columntypes, List<String> columns) {
		return new CsvStreamIgnite<>(this,new CsvOptionsSQL(header, columntypes, columns));
	}
	
	/**
	 * Creates Json stream object.
	 * @return JsonStream object
	 */
	private JsonStreamIgnite<JSONObject> toJson() {
		return new JsonStreamIgnite<>(this);
	}

	/**
	 * The function newStreamFILE creates Data Pipeline
	 * accepts the three params file path, folder in FILE and
	 * config object.
	 * @param filepath
	 * @param folder
	 * @param pipelineconfig
	 * @return IgnitePipeline object
	 * @throws PipelineException
	 */
	public static IgnitePipeline<String> newStreamFILE(String folder, PipelineConfig pipelineconfig) throws PipelineException {
		return new IgnitePipeline<String>(DataSamudayaConstants.NULLSTRING, folder, pipelineconfig, FileSystemSupport.FILE);
	}

	public static IgnitePipeline<String> newStream(String filepathwithscheme, PipelineConfig pipelineconfig) throws PipelineException {
		IgnitePipeline<String> mdp = null;
		URL url;
		try {
			url = new URL(filepathwithscheme);
			if (url.getProtocol().equals(FileSystemSupport.HDFS)) {
				mdp = newStreamHDFS(url.getProtocol() + DataSamudayaConstants.COLON + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.FORWARD_SLASH + url.getHost() + DataSamudayaConstants.COLON + url.getPort(), url.getPath(), pipelineconfig);
			}
			else if (url.getProtocol().equals(FileSystemSupport.FILE)) {
				mdp = newStreamFILE(url.getPath(), pipelineconfig);
			}
			else {
				throw new UnsupportedOperationException(FileSystemSupport.EXCEPTIONUNSUPPORTEDFILESYSTEM);
			}
			return mdp;
		}
		catch (Exception e) {
			throw new PipelineException(PipelineConstants.URISYNTAXNOTPROPER, e);
		}
	}
	
	
	/**
	 * IgnitePipeline constructor for reduce function.
	 * @param root
	 * @param reduce
	 */
	protected IgnitePipeline(AbstractPipeline root,
			ReduceFunction<I1> reduce) {
		this.task = reduce;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * IgnitePipeline accepts the Reduce function.
	 * @param reduce
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	public IgnitePipeline<I1> reduce(ReduceFunction<I1> reduce) throws PipelineException  {
		if(Objects.isNull(reduce)) {
			throw new PipelineException(PipelineConstants.KEYBYNULL);
		}
		var mdp = new IgnitePipeline<I1>(root,reduce);
		mdp.parents.add(this);
		this.childs.add(mdp);
		return mdp;
	}

	/**
	 * IgnitePipeline constructor for MapFunction.
	 * @param <T>
	 * @param root
	 * @param map
	 */
	
	protected <T> IgnitePipeline(AbstractPipeline root,
			MapFunction<I1, ? extends T> map) {
		this.task = map;
		root.finaltask = task;
		this.root = root;
	}

	/**
	 * IgnitePipeline accepts the MapFunction.
	 * @param <T>
	 * @param map
	 * @return IgnitePipeline object
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public <T> IgnitePipeline<T> map(MapFunction<I1 , ? extends T> map) throws PipelineException {
		if (Objects.isNull(map)) {
			throw new PipelineException(PipelineConstants.MAPFUNCTIONNULL);
		}
		var mapobj = new IgnitePipeline(root, map);
		this.childs.add(mapobj);
		mapobj.parents.add(this);
		return mapobj;
	}

	public List<Path> getFilepaths() {
		return filepaths;
	}

	public void setFilepaths(List<Path> filepaths) {
		this.filepaths = filepaths;
	}

	/**
	 * IgnitePipeline constructor for Peek function.
	 * @param root
	 * @param peekConsumer
	 */
	
	private IgnitePipeline(AbstractPipeline root,
			PeekConsumer<I1> peekConsumer) {
		this.task = peekConsumer;
		this.root = root;
		root.finaltask = task;
		mdsroots.add(root);
	}

	/**
	 * IgnitePipeline constructor for count.
	 * @param root
	 * @param calculatecount
	 */
	
	protected IgnitePipeline(AbstractPipeline root,
			CalculateCount calculatecount) {
		this.task = calculatecount;
		this.root = root;
		mdsroots.add(root);
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline constructor for filter.
	 * @param root
	 * @param predicate
	 */
	
	private IgnitePipeline(AbstractPipeline root,
			PredicateSerializable<I1> predicate) {
		this.task = predicate;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline accepts the filter i.e predicate function.
	 * @param predicate
	 * @return IgnitePipeline object
	 * @throws PipelineException
	 */
	public IgnitePipeline<I1> filter(PredicateSerializable<I1> predicate) throws PipelineException {
		if (Objects.isNull(predicate)) {
			throw new PipelineException(PipelineConstants.PREDICATENULL);
		}
		var filter = new IgnitePipeline<>(root, predicate);
		this.childs.add(filter);
		filter.parents.add(this);
		return filter;
	}

	/**
	 * IgnitePipeline constructor for union.
	 * @param root
	 * @param unionfunction
	 */
	
	private IgnitePipeline(AbstractPipeline root,
			UnionFunction unionfunction) {
		this.task = unionfunction;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline accepts the union function.
	 * @param union
	 * @return IgnitePipeline object
	 * @throws PipelineException
	 */
	public IgnitePipeline<I1> union(IgnitePipeline<I1> union) throws PipelineException {
		if (Objects.isNull(union)) {
			throw new PipelineException(PipelineConstants.UNIONNULL);
		}
		var unionfunction = new UnionFunction();
		var unionchild = new  IgnitePipeline(root, unionfunction);
		this.childs.add(unionchild);
		unionchild.parents.add(this);
		union.childs.add(unionchild);
		unionchild.parents.add(union);
		root.mdsroots.add(this.root);
		root.mdsroots.add(union.root);
		return unionchild;
	}

	/**
	 * IgnitePipeline constructor for intersection.
	 * @param root
	 * @param intersectionfunction
	 */
	
	private IgnitePipeline(AbstractPipeline root,
			IntersectionFunction intersectionfunction) {
		this.task = intersectionfunction;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline accepts the intersection function.
	 * @param intersection
	 * @return IgnitePipeline object.
	 * @throws PipelineException
	 */
	public IgnitePipeline<I1> intersection(IgnitePipeline<I1> intersection) throws PipelineException {
		if (Objects.isNull(intersection)) {
			throw new PipelineException(PipelineConstants.INTERSECTIONNULL);
		}
		var intersectionfunction = new IntersectionFunction();
		var intersectionchild = new  IgnitePipeline(root, intersectionfunction);
		this.childs.add(intersectionchild);
		intersectionchild.parents.add(this);
		intersection.childs.add(intersectionchild);
		intersectionchild.parents.add(intersection);
		root.mdsroots.add(this.root);
		root.mdsroots.add(intersection.root);
		return intersectionchild;
	}

	/**
	 * IgnitePipeline accepts the MapPair function.
	 * @param <I3>
	 * @param <I4>
	 * @param pf
	 * @return MapPairIgnite object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public <I3, I4> MapPairIgnite<I3, I4> mapToPair(MapToPairFunction<? super I1, ? extends Tuple2<I3, I4>> pf) throws PipelineException {
		if (Objects.isNull(pf)) {
			throw new PipelineException(PipelineConstants.MAPPAIRNULL);
		}
		var mappairignite = new MapPairIgnite(root, pf);
		this.childs.add(mappairignite);
		mappairignite.parents.add(this);
		return mappairignite;
	}

	/**
	 * IgnitePipeline constructor for sample.
	 * @param root
	 * @param sampleintegersupplier
	 */
	
	private IgnitePipeline(AbstractPipeline root,
			SampleSupplierInteger sampleintegersupplier) {
		this.task = sampleintegersupplier;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline accepts the sample function.
	 * @param numsample
	 * @return IgnitePipeline object.
	 * @throws PipelineException
	 */
	public IgnitePipeline<I1> sample(Integer numsample) throws PipelineException {
		if (Objects.isNull(numsample)) {
			throw new PipelineException(PipelineConstants.SAMPLENULL);
		}
		var sampleintegersupplier = new SampleSupplierInteger(numsample);
		var samplesupplier = new IgnitePipeline(root, sampleintegersupplier);
		this.childs.add(samplesupplier);
		samplesupplier.parents.add(this);
		return samplesupplier;
	}

	/**
	 * IgnitePipeline accepts the right outer join function.
	 * @param mappairignite
	 * @param conditionrightouterjoin
	 * @return IgnitePipeline object.
	 * @throws PipelineException
	 */
	public <I2> IgnitePipeline<Tuple2<I1,I2>> rightOuterjoin(IgnitePipeline<? extends I2> mappairignite, RightOuterJoinPredicate<? super I1, ? super I2> conditionrightouterjoin) throws PipelineException {
		if (Objects.isNull(mappairignite)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOIN);
		}
		if (Objects.isNull(conditionrightouterjoin)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOINCONDITION);
		}
		var mdp = new IgnitePipeline(root, conditionrightouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappairignite.childs.add(mdp);
		mdp.parents.add(mappairignite);
		root.mdsroots.add(this.root);
		root.mdsroots.add(mappairignite.root);
		return mdp;
	}

	/**
	 * IgnitePipeline accepts the left outer join function.
	 * @param mappairignite
	 * @param conditionleftouterjoin
	 * @return IgnitePipeline object.
	 * @throws PipelineException
	 */
	public <I2> IgnitePipeline<Tuple2<I1,I2>> leftOuterjoin(IgnitePipeline<I2> mappairignite, LeftOuterJoinPredicate<I1, I2> conditionleftouterjoin) throws PipelineException {
		if (Objects.isNull(mappairignite)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOIN);
		}
		if (Objects.isNull(conditionleftouterjoin)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOINCONDITION);
		}
		var mdp = new IgnitePipeline(root, conditionleftouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappairignite.childs.add(mdp);
		mdp.parents.add(mappairignite);
		root.mdsroots.add(this.root);
		root.mdsroots.add(mappairignite.root);
		return mdp;
	}

	/**
	 * IgnitePipeline accepts the inner join function.
	 * @param mappairignite
	 * @param innerjoin
	 * @return IgnitePipeline object.
	 * @throws PipelineException
	 */
	public <I2> IgnitePipeline<Tuple2<I1,I2>> join(IgnitePipeline<I2> mappairignite, JoinPredicate<I1, I2> innerjoin) throws PipelineException {
		if (Objects.isNull(mappairignite)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		if (Objects.isNull(innerjoin)) {
			throw new PipelineException(PipelineConstants.INNERJOINCONDITION);
		}
		var mdp = new IgnitePipeline(root, innerjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappairignite.childs.add(mdp);
		mdp.parents.add(mappairignite);
		root.mdsroots.add(this.root);
		root.mdsroots.add(mappairignite.root);
		return mdp;
	}

	/**
	 * IgnitePipeline constructor for FlatMap function.
	 * @param <T>
	 * @param root
	 * @param fmf
	 */
	
	private <T> IgnitePipeline(AbstractPipeline root,
			FlatMapFunction<I1, ? extends T> fmf) {
		this.task = fmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline accepts the FlatMap function.
	 * @param <T>
	 * @param fmf
	 * @return IgnitePipeline object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public <T> IgnitePipeline<T> flatMap(FlatMapFunction<I1, ? extends T> fmf) throws PipelineException {
		if (Objects.isNull(fmf)) {
			throw new PipelineException(PipelineConstants.FLATMAPNULL);
		}
		var mdp = new IgnitePipeline(root, fmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * IgnitePipeline accepts the TupleFlatMap function.
	 * @param <I3>
	 * @param <I4>
	 * @param fmt
	 * @return MapPairIgnite object. 
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public <I3, I4> MapPairIgnite<I3, I4> flatMapToTuple2(TupleFlatMapFunction<? super I1, ? extends Tuple2<I3, I4>> fmt) throws PipelineException {
		if (Objects.isNull(fmt)) {
			throw new PipelineException(PipelineConstants.FLATMAPPAIRNULL);
		}
		var mdp = new MapPairIgnite(root, fmt);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * IgnitePipeline accepts the TupleFlatMap function.
	 * @param fmt
	 * @return IgnitePipeline object. 
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public IgnitePipeline<Tuple> flatMapToTuple(TupleFlatMapFunction<? super I1, ? extends Tuple> fmt) throws PipelineException {
		if (Objects.isNull(fmt)) {
			throw new PipelineException(PipelineConstants.FLATMAPPAIRNULL);
		}
		var mdp = new IgnitePipeline(root, fmt);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * IgnitePipeline constructor for TupleFlatMap function.
	 * @param root
	 * @param lfmf
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	private IgnitePipeline(AbstractPipeline root,
			TupleFlatMapFunction lfmf) {
		this.task = lfmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline constructor for LongFlatMap function.
	 * @param root
	 * @param lfmf
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	private IgnitePipeline(AbstractPipeline root,
			LongFlatMapFunction lfmf) {
		this.task = lfmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline accepts the LongFlatMap function.
	 * @param lfmf
	 * @return IgnitePipeline object.
	 * @throws PipelineException
	 */
	public IgnitePipeline<Long> flatMapToLong(LongFlatMapFunction<I1> lfmf) throws PipelineException {
		if (Objects.isNull(lfmf)) {
			throw new PipelineException(PipelineConstants.LONGFLATMAPNULL);
		}
		var mdp = new IgnitePipeline<Long>(root, lfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * IgnitePipeline constructor for DoubleFlatMap function.
	 * @param root
	 * @param dfmf
	 */
	
	private IgnitePipeline(AbstractPipeline root,
			DoubleFlatMapFunction<I1> dfmf) {
		this.task = dfmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline accepts the DoubleFlatMap function.
	 * @param dfmf
	 * @return IgnitePipeline object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public IgnitePipeline<Double> flatMapToDouble(DoubleFlatMapFunction<I1> dfmf) throws PipelineException {
		if (Objects.isNull(dfmf)) {
			throw new PipelineException(PipelineConstants.DOUBLEFLATMAPNULL);
		}
		var mdp = new IgnitePipeline(root, dfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * IgnitePipeline accepts the peek function.
	 * @param consumer
	 * @return IgnitePipeline object.
	 * @throws PipelineException
	 */
	public IgnitePipeline<I1> peek(PeekConsumer<I1> consumer) throws PipelineException  {
		if (Objects.isNull(consumer)) {
			throw new PipelineException(PipelineConstants.PEEKNULL);
		}
		var map = new IgnitePipeline<>(root, consumer);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}

	/**
	 * IgnitePipeline constructor for sorting function.
	 * @param root
	 * @param sortedcomparator
	 */
	
	private IgnitePipeline(AbstractPipeline root,
			SortedComparator<I1> sortedcomparator) {
		this.task = sortedcomparator;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline constructor for RightOuterJoin function.
	 * @param mdp
	 * @param rightouterjoinpredicate
	 */
	@SuppressWarnings("unchecked")
	protected IgnitePipeline(IgnitePipeline<I1> mdp,
			RightOuterJoinPredicate<I1, I1> rightouterjoinpredicate) {
		this.task = rightouterjoinpredicate;
		this.root = mdp.root;
		mdsroots.add(mdp.root);
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline constructor for LeftOuterJoin function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param mdp
	 * @param leftouterjoinpredicate
	 */
	
	protected <T, O1, O2> IgnitePipeline(IgnitePipeline<I1> mdp,
			LeftOuterJoinPredicate<I1, I1> leftouterjoinpredicate) {
		this.task = leftouterjoinpredicate;
		this.root = mdp.root;
		mdsroots.add(mdp.root);
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline constructor for RightOuterJoin function.
	 * @param root
	 * @param conditionrightouterjoin
	 */
	
	private IgnitePipeline(AbstractPipeline root,
			RightOuterJoinPredicate<? super I1, ? super I1> conditionrightouterjoin) {
		this.task = conditionrightouterjoin;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline constructor for LeftOuterJoin function.
	 * @param root
	 * @param conditionleftouterjoin
	 */
	@SuppressWarnings("unchecked")
	private IgnitePipeline(AbstractPipeline root,
			LeftOuterJoinPredicate<? super I1, ? super I1> conditionleftouterjoin) {
		this.task = conditionleftouterjoin;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline constructor for InnerJoin function.
	 * @param root
	 * @param join
	 */
	@SuppressWarnings("unchecked")
	private IgnitePipeline(AbstractPipeline root,
			JoinPredicate<? super I1, ? super I1> join) {
		this.task = join;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline accepts the sorting function.
	 * @param sortedcomparator
	 * @return IgnitePipeline object.
	 * @throws PipelineException
	 */
	public IgnitePipeline<I1> sorted(SortedComparator<I1> sortedcomparator) throws PipelineException  {
		if (Objects.isNull(sortedcomparator)) {
			throw new PipelineException(PipelineConstants.SORTEDNULL);
		}
		var map = new IgnitePipeline<>(root, sortedcomparator);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}

	/**
	 * IgnitePipeline constructor for Distinct.
	 * @param root
	 * @param distinct
	 */
	
	private IgnitePipeline(AbstractPipeline root,
			Distinct distinct) {
		this.task = distinct;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline accepts the distinct.
	 * @return IgnitePipeline object.
	 */
	public IgnitePipeline<I1> distinct()  {
		var distinct = new Distinct();
		var map = new IgnitePipeline(root, distinct);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}

	/**
	 * IgnitePipeline constructor for ToInt function.
	 * @param root
	 * @param tointfunction
	 */
	
	protected IgnitePipeline(AbstractPipeline root,
			ToIntFunction<I1> tointfunction) {
		this.task = tointfunction;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline accepts the SToInt function.
	 * @param tointfunction
	 * @return PipelineIntStream object.
	 * @throws PipelineException
	 */
	public PipelineIntStream<I1> mapToInt(SToIntFunction<I1> tointfunction) throws PipelineException  {
		if (Objects.isNull(tointfunction)) {
			throw new PipelineException(PipelineConstants.MAPTOINTNULL);
		}
		var map = new PipelineIntStream<>(root, tointfunction);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}

	/**
	 * IgnitePipeline constructor for KeyBy function.
	 * @param root
	 * @param keybyfunction
	 */
	
	protected IgnitePipeline(AbstractPipeline root,
			KeyByFunction<I1, I1> keybyfunction) {
		this.task = keybyfunction;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * IgnitePipeline accepts the KeyBy function.
	 * @param <O>
	 * @param keybyfunction
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	public <O> MapPair<O, I1> keyBy(KeyByFunction<I1, O> keybyfunction) throws PipelineException  {
		if (Objects.isNull(keybyfunction)) {
			throw new PipelineException(PipelineConstants.KEYBYNULL);
		}
		var mt = new MapPair(root, keybyfunction);
		mt.parents.add(this);
		this.childs.add(mt);
		return mt;
	}
	
	/**
	 * IgnitePipeline constructor for the Coalesce function.
	 * @param <O1>
	 * @param root
	 * @param cf
	 */
	private <O1> IgnitePipeline(AbstractPipeline root,
			Coalesce<I1> cf)  {
		this.task = cf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * IgnitePipeline accepts the coalesce function.
	 * @param partition
	 * @param cf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public IgnitePipeline<I1> coalesce(int partition,PipelineCoalesceFunction<I1> cf) throws PipelineException  {
		if(Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.COALESCENULL);
		}
		var IgnitePipelinecoalesce = new IgnitePipeline<I1>(root, new Coalesce(partition, cf));
		this.childs.add(IgnitePipelinecoalesce);
		IgnitePipelinecoalesce.parents.add(this);
		return IgnitePipelinecoalesce;
	}
	
	/**
	 * Private interface which accepts groupby function. 
	 * @param <I2>
	 * @param root
	 * @param cf
	 */
	private <I2> IgnitePipeline(AbstractPipeline root,
			GroupByFunction<I1,I2> gbf)  {
		this.task = gbf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * 
	 * @param <T>
	 * @param mf
	 * @return
	 * @throws PipelineException
	 */
	public <T1> IgnitePipeline<Tuple2<T1,List<I1>>> groupBy(GroupByFunction<I1,T1> gbf) throws PipelineException{
		if(Objects.isNull(gbf)) {
			throw new PipelineException(PipelineConstants.GROUPBYNULL);
		}
		var sp = new IgnitePipeline<>(root,gbf);
		sp.parents.add(this);
		this.childs.add(sp);
		return (IgnitePipeline<Tuple2<T1, List<I1>>>) sp;
	}

	/**
	 * Terminal operation save as file.
	 * @param path
	 * @throws Exception 
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void saveAsTextFile(URI uri, String path) throws Exception  {
		log.debug("Caching...");
		var mdp = (IgnitePipeline) root;
		Utils.writeToOstream(mdp.pipelineconfig.getOutput(), "Caching...");
		var mdscollect = (IgnitePipeline) root;
		mdscollect.finaltasks.clear();
		mdscollect.finaltasks.add(mdscollect.finaltask);
		mdscollect.mdsroots.add(root);
		mdscollect.cacheInternal(true, uri, path);
		log.debug("Cached....");
		Utils.writeToOstream(mdp.pipelineconfig.getOutput(), "Cached...");
	}


	/**
	 * Collect result or just computes stages alone by passing the 
	 * toexecute parameter. 
	 * @param toexecute
	 * @param supplier
	 * @return
	 * @throws PipelineException 
	 * @
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public IgnitePipeline<I1> cache(boolean isresults) throws PipelineException  {
		try {
			log.debug("Caching...");
			var mdp = (IgnitePipeline) root;
			Utils.writeToOstream(mdp.pipelineconfig.getOutput(), "Caching...");
			var mdscollect = (IgnitePipeline) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdscollect.finaltask);
			mdscollect.mdsroots.add(root);
			var job = mdscollect.cacheInternal(isresults, null, null);
			log.debug("Cached....");
			Utils.writeToOstream(mdp.pipelineconfig.getOutput(), "Cached...");
			var mdpcached = new IgnitePipeline();
			mdpcached.job = job;
			mdpcached.pipelineconfig = mdp.pipelineconfig;
			return mdpcached;
		}
		catch (Exception ex) {
			log.error(PipelineConstants.PIPELINECOLLECTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOLLECTERROR, ex);
		}
	}

	/**
	 * Collect result or just computes stages alone by passing the 
	 * toexecute parameter. 
	 * @param toexecute
	 * @param supplier
	 * @return list
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes"})
	public List collect(boolean toexecute, IntSupplier supplier) throws PipelineException {
		try {
			log.debug("Caching...");
			var mdp = (IgnitePipeline) root;
			Utils.writeToOstream(mdp.pipelineconfig.getOutput(), "Caching...");
			var mdscollect = (IgnitePipeline) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdscollect.finaltask);
			mdscollect.mdsroots.add(root);
			var job = mdscollect.cacheInternal(true, null, null);
			return (List) job.getResults();
		} catch (Exception ex) {
			log.error(PipelineConstants.PIPELINECOLLECTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOLLECTERROR, ex);
		}
	}

	/**
	 * The function count return the results of count.
	 * @param supplier
	 * @return results of count.
	 * @throws PipelineException
	 */
	public Object count(NumPartitions supplier) throws PipelineException  {
		try {
			var mdp = new IgnitePipeline(root, new CalculateCount());
			mdp.parents.add(this);
			this.childs.add(mdp);
			var mdscollect = (IgnitePipeline) root;

			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdp.task);
			mdscollect.mdsroots.add(root);
			var job = mdscollect.cacheInternal(true, null, null);
			return (List) job.getResults();
		}
		catch (Exception ex) {
			log.error(PipelineConstants.PIPELINECOUNTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOUNTERROR, ex);
		}
	}

	/**
	 * This function executes the forEach tasks.
	 * @param consumer
	 * @param supplier
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void forEach(Consumer<?> consumer, IntSupplier supplier) throws PipelineException  {
		try {
			var mdscollect = (IgnitePipeline) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdscollect.finaltask);

			mdscollect.mdsroots.add(root);
			var job = mdscollect.cacheInternal(true, null, null);
			var results = (List<?>) job.getResults();
			results.stream().forEach((Consumer) consumer);
		}
		catch (Exception ex) {
			log.error(PipelineConstants.PIPELINEFOREACHERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINEFOREACHERROR, ex);
		}
	}

	public String getHdfspath() {
		return hdfspath;
	}

	public String getFolder() {
		return folder;
	}

	@Override
	public String toString() {
		return "IgnitePipeline [task=" + task + "]";
	}

}
