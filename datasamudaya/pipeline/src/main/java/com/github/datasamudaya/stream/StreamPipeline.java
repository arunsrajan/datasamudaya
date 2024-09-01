package com.github.datasamudaya.stream;

import static java.util.Objects.nonNull;

import java.awt.TrayIcon.MessageType;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.IgniteCache;
import org.apache.log4j.Logger;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.AllDirectedPaths;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.io.ComponentNameProvider;
import org.jgrapht.io.DOTExporter;
import org.jgrapht.io.ExportException;
import org.jgrapht.io.GraphExporter;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.JSONObject;

import com.esotericsoftware.kryo.io.Input;
import com.github.datasamudaya.common.ByteBufferInputStream;
import com.github.datasamudaya.common.ByteBufferOutputStream;
import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.DataSamudayaIgniteClient;
import com.github.datasamudaya.common.DataSamudayaJobMetrics;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.FileSystemSupport;
import com.github.datasamudaya.common.GlobalContainerLaunchers;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.Job.JOBTYPE;
import com.github.datasamudaya.common.JobMetrics;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.RemoteDataFetch;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.Stage;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.AggregateFunction;
import com.github.datasamudaya.common.functions.AggregateReduceFunction;
import com.github.datasamudaya.common.functions.CalculateCount;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.functions.CountByKeyFunction;
import com.github.datasamudaya.common.functions.CountByValueFunction;
import com.github.datasamudaya.common.functions.Distinct;
import com.github.datasamudaya.common.functions.DistributedDistinct;
import com.github.datasamudaya.common.functions.DistributedSort;
import com.github.datasamudaya.common.functions.DoubleFlatMapFunction;
import com.github.datasamudaya.common.functions.FlatMapFunction;
import com.github.datasamudaya.common.functions.FoldByKey;
import com.github.datasamudaya.common.functions.FullOuterJoin;
import com.github.datasamudaya.common.functions.GroupByFunction;
import com.github.datasamudaya.common.functions.GroupByKeyFunction;
import com.github.datasamudaya.common.functions.HashPartitioner;
import com.github.datasamudaya.common.functions.IntersectionFunction;
import com.github.datasamudaya.common.functions.Join;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.KeyByFunction;
import com.github.datasamudaya.common.functions.LeftJoin;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.LongFlatMapFunction;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.MapToPairFunction;
import com.github.datasamudaya.common.functions.PeekConsumer;
import com.github.datasamudaya.common.functions.PipelineCoalesceFunction;
import com.github.datasamudaya.common.functions.PredicateSerializable;
import com.github.datasamudaya.common.functions.ReduceByKeyFunction;
import com.github.datasamudaya.common.functions.ReduceByKeyFunctionValues;
import com.github.datasamudaya.common.functions.ReduceFunction;
import com.github.datasamudaya.common.functions.RightJoin;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.SToIntFunction;
import com.github.datasamudaya.common.functions.SortedComparator;
import com.github.datasamudaya.common.functions.TupleFlatMapFunction;
import com.github.datasamudaya.common.functions.UnionFunction;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.scheduler.RemoteJobScheduler;
import com.github.datasamudaya.stream.scheduler.StreamJobScheduler;
import com.github.datasamudaya.stream.scheduler.StreamPipelineTaskSubmitter;
import com.github.datasamudaya.stream.utils.FileBlocksPartitionerHDFS;
import com.github.datasamudaya.stream.utils.PipelineConfigValidator;

/**
 * 
 * @author arun
 * The class StreamPipeline is the class for the core DataPipeline .
 * @param <I1>
 */
public sealed class StreamPipeline<I1> extends AbstractPipeline permits CsvStream,JsonStream, PipelineIntStream {
	private List<Path> filepaths = new ArrayList<>();
	protected String protocol;
	IntSupplier supplier;
	private String hdfspath;
	private String folder;

	private static Logger log = Logger.getLogger(StreamPipeline.class);

	protected StreamPipeline() {

	}

	/**
	 * private Constructor for StreamPipeline 
	 * @param hdfspath
	 * @param folder
	 * @param pipelineconfig
	 * @throws PipelineException
	 */
	private StreamPipeline(String hdfspath, String folder, PipelineConfig pipelineconfig) throws PipelineException {
		var validator = new PipelineConfigValidator();
		var errormessages = validator.validate(pipelineconfig);
		if (!errormessages.isEmpty()) {
			var errors = new StringBuilder();
			errormessages.stream().forEach(error -> errors.append(error + DataSamudayaConstants.NEWLINE));
			throw new PipelineException(errors.toString());
		}
		this.pipelineconfig = pipelineconfig;
		this.hdfspath = hdfspath;
		this.folder = folder;
		this.protocol = FileSystemSupport.HDFS;
	}

	/**
	 * The function newStreamHDFS creates Data Pipeline
	 * accepts the three params hdfs path, folder in HDFS and
	 * config object.
	 * @param hdfspath
	 * @param folder
	 * @param pipelineconfig
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	public static StreamPipeline<String> newStreamHDFS(String hdfspath, String folder, PipelineConfig pipelineconfig) throws PipelineException {
		StreamPipeline<String> pipeline = new StreamPipeline<String>(hdfspath, folder, pipelineconfig);
		pipeline.tasks.add(new Dummy());
		return pipeline;

	}


	public static CsvStream<CSVRecord> newCsvStreamHDFS(String hdfspath, String folder, PipelineConfig pipelineconfig, String[] header) throws PipelineException {
		StreamPipeline<String> pipeline = new StreamPipeline<String>(hdfspath, folder, pipelineconfig);
		CsvStream<CSVRecord> csvpipeline = pipeline.csvWithHeader(header);
		pipeline.tasks.add(new Dummy());
		return csvpipeline;

	}

	public static CsvStream<Map<String, Object>> newCsvStreamHDFSSQL(String hdfspath, String folder, PipelineConfig pipelineconfig, String[] header, List<SqlTypeName> types, List<String> requiredcolumns) throws PipelineException {
		pipelineconfig.setStorage(STORAGE.COLUMNARSQL);
		StreamPipeline<String> pipeline = new StreamPipeline<String>(hdfspath, folder, pipelineconfig);
		pipeline.tasks.add(new Dummy());
		return pipeline.csvWithHeader(header, types, requiredcolumns);

	}

	public static JsonStream<JSONObject> newJsonStreamHDFS(String hdfspath, String folder, PipelineConfig pipelineconfig) throws PipelineException {
		StreamPipeline<String> pipeline = new StreamPipeline<String>(hdfspath, folder, pipelineconfig);
		pipeline.tasks.add(new Dummy());
		return pipeline.toJson();

	}

	public static JsonStream<Map<String, Object>> newJsonStreamHDFSSQL(String hdfspath, String folder, PipelineConfig pipelineconfig, String[] header, List<SqlTypeName> types, List<String> requiredcolumns) throws PipelineException {
		pipelineconfig.setStorage(STORAGE.COLUMNARSQL);
		StreamPipeline<String> pipeline = new StreamPipeline<String>(hdfspath, folder, pipelineconfig);
		pipeline.tasks.add(new Dummy());
		return pipeline.toJson(header, types, requiredcolumns);

	}

	public static StreamPipeline<String> newStream(String filepathwithscheme, PipelineConfig pipelineconfig) throws PipelineException {
		StreamPipeline<String> sp = null;
		URL url;
		try {
			url = new URL(filepathwithscheme);
			if (url.getProtocol().equals(FileSystemSupport.HDFS)) {
				sp = newStreamHDFS(url.getProtocol() + DataSamudayaConstants.COLON + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.FORWARD_SLASH + url.getHost() + DataSamudayaConstants.COLON + url.getPort(), url.getPath(), pipelineconfig);
			}
			return sp;
		}
		catch (MalformedURLException use) {
			throw new PipelineException(PipelineConstants.URISYNTAXNOTPROPER, use);
		}
	}

	/**
	 * Creates csv stream object
	 * @param header
	 * @return CsvStream object.
	 */
	private CsvStream<CSVRecord> csvWithHeader(String[] header) {
		return new CsvStream<>(this, new CsvOptions(header));
	}

	/**
	 * Creates csv stream object
	 * @param header
	 * @return CsvStream object.
	 */
	private CsvStream<Map<String, Object>> csvWithHeader(String[] header, List<SqlTypeName> columntypes, List<String> columns) {
		return new CsvStream<>(this, new CsvOptionsSQL(header, columntypes, columns));
	}

	/**
	 * Creates Json stream object.
	 * @return JsonStream object
	 */
	private JsonStream<JSONObject> toJson() {
		return new JsonStream<>(this);
	}

	/**
	 * Creates Json stream object.
	 * @param header
	 * @param columntypes
	 * @param columns
	 * @return json stream object
	 */
	private JsonStream<Map<String, Object>> toJson(String[] header, List<SqlTypeName> columntypes, List<String> columns) {
		return new JsonStream<>(this, new JsonSQL(header, columntypes, columns));
	}

	/**
	 * StreamPipeline accepts the MapFunction.
	 * @param <T>
	 * @param map
	 * @return StreamPipeline object
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public <T> StreamPipeline<T> map(MapFunction<I1 , ? extends T> map) throws PipelineException {
		if (Objects.isNull(map)) {
			throw new PipelineException(PipelineConstants.MAPFUNCTIONNULL);
		}
		var mapobj = new StreamPipeline(this, map);
		return mapobj;
	}

	public List<Path> getFilepaths() {
		return filepaths;
	}

	public void setFilepaths(List<Path> filepaths) {
		this.filepaths = filepaths;
	}

	/**
	 * Add child task to parent 
	 * @param parent
	 * @param task
	 */
	protected StreamPipeline(AbstractPipeline parent, Object task) {
		this.parents.add(parent);
		parent.childs.add(this);
		this.tasks.add(task);
		this.pipelineconfig = parent.pipelineconfig;
		this.csvoptions = parent.csvoptions;
		this.json = parent.json;
	}

	/**
	 * Parent And Child Relationship for Union, Intersection etc.
	 * @param parent1
	 * @param parent2
	 * @param task
	 */
	private StreamPipeline(StreamPipeline<I1> parent1, StreamPipeline<I1> parent2, Object task) {
		parent1.childs.add(this);
		this.parents.add(parent1);
		parent2.childs.add(this);
		this.parents.add(parent2);
		this.tasks.add(task);
		this.pipelineconfig = parent1.pipelineconfig;
	}


	/**
	 * StreamPipeline accepts the filter i.e predicate function.
	 * @param predicate
	 * @return StreamPipeline object
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> filter(PredicateSerializable<I1> predicate) throws PipelineException {
		if (Objects.isNull(predicate)) {
			throw new PipelineException(PipelineConstants.PREDICATENULL);
		}
		var filter = new StreamPipeline<I1>(this, predicate);
		return filter;
	}

	/**
	 * StreamPipeline accepts the union function.
	 * @param union
	 * @return StreamPipeline object
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> union(StreamPipeline<I1> union) throws PipelineException {
		if (Objects.isNull(union)) {
			throw new PipelineException(PipelineConstants.UNIONNULL);
		}
		var unionfunction = new UnionFunction();
		var unionchild = new  StreamPipeline(this, union, unionfunction);
		return unionchild;
	}


	/**
	 * StreamPipeline accepts the intersection function.
	 * @param intersection
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> intersection(StreamPipeline<I1> intersection) throws PipelineException {
		if (Objects.isNull(intersection)) {
			throw new PipelineException(PipelineConstants.INTERSECTIONNULL);
		}
		var intersectionfunction = new IntersectionFunction();
		var intersectionchild = new  StreamPipeline(this, intersection, intersectionfunction);
		return intersectionchild;
	}

	/**
	 * StreamPipeline accepts the MapPair function.
	 * @param <I3>
	 * @param <I4>
	 * @param pf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public <I3, I4> MapPair<I3, I4> mapToPair(MapToPairFunction<? super I1, ? extends Tuple2<I3, I4>> pf) throws PipelineException {
		if (Objects.isNull(pf)) {
			throw new PipelineException(PipelineConstants.MAPPAIRNULL);
		}
		var mappair = new MapPair(this, pf);
		return mappair;
	}

	/**
	 * StreamPipeline accepts the sample function.
	 * @param numsample
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> sample(Integer numsample) throws PipelineException {
		if (Objects.isNull(numsample)) {
			throw new PipelineException(PipelineConstants.SAMPLENULL);
		}
		var sampleintegersupplier = new SampleSupplierInteger(numsample);
		var samplesupplier = new StreamPipeline(this, sampleintegersupplier);
		return samplesupplier;
	}

	/**
	 * StreamPipeline accepts the right outer join function.
	 * @param mappair
	 * @param conditionrightouterjoin
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	public <I2> StreamPipeline<Tuple2<I1, I2>> rightOuterjoin(StreamPipeline<? extends I2> mappair, RightOuterJoinPredicate<? super I1, ? super I2> conditionrightouterjoin) throws PipelineException {
		if (Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOIN);
		}
		if (Objects.isNull(conditionrightouterjoin)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOINCONDITION);
		}
		var sp = new StreamPipeline(this, mappair, conditionrightouterjoin);
		return sp;
	}

	/**
	 * StreamPipeline accepts the left outer join function.
	 * @param mappair
	 * @param conditionleftouterjoin
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	public <I2> StreamPipeline<Tuple2<I1, I2>> leftOuterjoin(StreamPipeline<I2> mappair,
			LeftOuterJoinPredicate<I1, I2> conditionleftouterjoin) throws PipelineException {
		if (Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOIN);
		}
		if (Objects.isNull(conditionleftouterjoin)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOINCONDITION);
		}
		StreamPipeline<Tuple2<I1, I2>> sp = new StreamPipeline(this, mappair, conditionleftouterjoin);
		return sp;
	}

	/**
	 * StreamPipeline accepts the inner join function.
	 * @param mappair
	 * @param innerjoin
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	public <I2> StreamPipeline<Tuple2<I1, I2>> join(StreamPipeline<I2> mappair, JoinPredicate<I1, I2> innerjoin) throws PipelineException {
		if (Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		if (Objects.isNull(innerjoin)) {
			throw new PipelineException(PipelineConstants.INNERJOINCONDITION);
		}
		StreamPipeline<Tuple2<I1, I2>> sp = new StreamPipeline(this, mappair, innerjoin);
		return sp;
	}

	/**
	 * StreamPipeline accepts the outer join function.
	 * @param <I2>
	 * @param mappair
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	public <I2> StreamPipeline<Tuple2<I1, I2>> fullJoin(StreamPipeline<I2> mappair) throws PipelineException {
		if (Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.OUTERJOIN);
		}		
		StreamPipeline<Tuple2<I1, I2>> sp = new StreamPipeline(this, mappair, new FullOuterJoin());
		return sp;
	}
	
	/**
	 * StreamPipeline accepts the FlatMap function.
	 * @param <T>
	 * @param fmf
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public <T> StreamPipeline<T> flatMap(FlatMapFunction<I1, ? extends T> fmf) throws PipelineException {
		if (Objects.isNull(fmf)) {
			throw new PipelineException(PipelineConstants.FLATMAPNULL);
		}
		var sp = new StreamPipeline(this, fmf);
		return sp;
	}

	/**
	 * StreamPipeline accepts the TupleFlatMap function.
	 * @param <I3>
	 * @param <I4>
	 * @param fmt
	 * @return MapPair object. 
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public <I3, I4> MapPair<I3, I4> flatMapToTuple2(TupleFlatMapFunction<? super I1, ? extends Tuple2<I3, I4>> fmt) throws PipelineException {
		if (Objects.isNull(fmt)) {
			throw new PipelineException(PipelineConstants.FLATMAPPAIRNULL);
		}
		var sp = new MapPair(this, fmt);
		return sp;
	}

	/**
	 * StreamPipeline accepts the TupleFlatMap function.
	 * @param fmt
	 * @return StreamPipeline object. 
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public StreamPipeline<Tuple> flatMapToTuple(TupleFlatMapFunction<? super I1, ? extends Tuple> fmt) throws PipelineException {
		if (Objects.isNull(fmt)) {
			throw new PipelineException(PipelineConstants.FLATMAPPAIRNULL);
		}
		var sp = new StreamPipeline(this, fmt);
		return sp;
	}

	/**
	 * StreamPipeline accepts the LongFlatMap function.
	 * @param lfmf
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<Long> flatMapToLong(LongFlatMapFunction<I1> lfmf) throws PipelineException {
		if (Objects.isNull(lfmf)) {
			throw new PipelineException(PipelineConstants.LONGFLATMAPNULL);
		}
		var sp = new StreamPipeline<Long>(this, lfmf);
		return sp;
	}

	/**
	 * StreamPipeline accepts the DoubleFlatMap function.
	 * @param dfmf
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public StreamPipeline<Double> flatMapToDouble(DoubleFlatMapFunction<I1> dfmf) throws PipelineException {
		if (Objects.isNull(dfmf)) {
			throw new PipelineException(PipelineConstants.DOUBLEFLATMAPNULL);
		}
		var sp = new StreamPipeline(this, dfmf);
		return sp;
	}

	/**
	 * StreamPipeline accepts the peek function.
	 * @param consumer
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> peek(PeekConsumer<I1> consumer) throws PipelineException {
		if (Objects.isNull(consumer)) {
			throw new PipelineException(PipelineConstants.PEEKNULL);
		}
		var peek = new StreamPipeline<I1>(this, consumer);
		return peek;
	}

	/**
	 * StreamPipeline accepts the sorting function.
	 * @param sortedcomparator
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> sorted(SortedComparator<I1> sortedcomparator) throws PipelineException {
		if (Objects.isNull(sortedcomparator)) {
			throw new PipelineException(PipelineConstants.SORTEDNULL);
		}
		var sort = new StreamPipeline<I1>(this, sortedcomparator);
		return sort;
	}

	/**
	 * StreamPipeline accepts the distinct.
	 * @return StreamPipeline object.
	 */
	public StreamPipeline<I1> distinct() {
		Distinct distinct = new Distinct();
		var map = new StreamPipeline(this, distinct);
		return map;
	}

	/**
	 * StreamPipeline accepts the SToInt function.
	 * @param tointfunction
	 * @return PipelineIntStream object.
	 * @throws PipelineException
	 */
	public PipelineIntStream<I1> mapToInt(SToIntFunction<I1> tointfunction) throws PipelineException {
		if (Objects.isNull(tointfunction)) {
			throw new PipelineException(PipelineConstants.MAPTOINTNULL);
		}
		var map = new PipelineIntStream<I1>(this, tointfunction);
		return map;
	}


	/**
	 * StreamPipeline accepts the KeyBy function.
	 * @param <O>
	 * @param keybyfunction
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	public <O> MapPair<O, I1> keyBy(KeyByFunction<I1, O> keybyfunction) throws PipelineException {
		if (Objects.isNull(keybyfunction)) {
			throw new PipelineException(PipelineConstants.KEYBYNULL);
		}
		var mt = new MapPair(this, keybyfunction);
		return mt;
	}

	/**
	 * StreamPipeline accepts the Reduce function.
	 * @param reduce
	 * @return StreamPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> reduce(ReduceFunction<I1> reduce) throws PipelineException {
		if (Objects.isNull(reduce)) {
			throw new PipelineException(PipelineConstants.KEYBYNULL);
		}
		var sp = new StreamPipeline<I1>(this, reduce);
		return sp;
	}

	/**
	 * StreamPipeline accepts the coalesce function.
	 * @param partition
	 * @param cf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public StreamPipeline<I1> coalesce(int partition, PipelineCoalesceFunction<I1> cf) throws PipelineException {
		if (Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.COALESCENULL);
		}
		var streampipelinecoalesce = new StreamPipeline<I1>(this, new Coalesce(partition, cf));
		return streampipelinecoalesce;
	}

	/**
	 * 
	 * @param <T>
	 * @param mf
	 * @return
	 * @throws PipelineException
	 */
	public <T1> StreamPipeline<Tuple2<T1, List<I1>>> groupBy(GroupByFunction<I1, T1> gbf) throws PipelineException {
		if (Objects.isNull(gbf)) {
			throw new PipelineException(PipelineConstants.GROUPBYNULL);
		}
		var sp = new StreamPipeline<Tuple2<T1, List<I1>>>(this, gbf);
		return sp;
	}

	/**
	 * Loads the data from hdfs and cached
	 * @return
	 * @throws Exception
	 */
	public StreamPipeline<I1> load() throws Exception {
		this.childs.clear();
		return this;
	}

	public StreamPipeline<I1> cache() throws Exception {
		var job = this.createJob();
		job.setJobtype(JOBTYPE.PIG);
		this.pipelineconfig.setJobid(job.getId());
		job.getJm().setSqlpigquery(this.pipelineconfig.getSqlpigquery());
		if (this.pipelineconfig.getIsremotescheduler()) {
			RemoteJobScheduler rjs = new RemoteJobScheduler();
			return new StreamPipeline<I1>(this, rjs.scheduleJob(job));
		} else {
			StreamJobScheduler js = new StreamJobScheduler();
			job.setPipelineconfig(this.pipelineconfig);
			var csp = new StreamPipeline<I1>(new Dummy());
			csp.pigtasks = (Set<Task>) js.schedule(job);
			this.clearChild();
			csp.csvoptions = ((StreamPipeline<?>) this).getCsvOptions();
			csp.json = ((StreamPipeline<?>) this).getJson();
			csp.pipelineconfig = this.pipelineconfig;
			csp.graph = new DirectedAcyclicGraph<>(DAGEdge.class);
			return csp;
		}
	}

	/**
	 * No Parent and child with dummy task
	 * @param task
	 */
	protected StreamPipeline(Object task) {
		tasks.add(task);
	}

	private Set<Task> pigtasks;
	protected DirectedAcyclicGraph<AbstractPipeline, DAGEdge> graph = new DirectedAcyclicGraph<>(DAGEdge.class);

	boolean reexecutealltasks;
	private Job job;
	Job jobCreated;

	/**
	 * Create Job and get DAG
	 * @return
	 * @throws PipelineException 
	 * @throws ExportException 
	 * @throws URISyntaxException 
	 * @throws IOException 
	 * @ 
	 */
	protected Job createJob() throws PipelineException, ExportException, IOException, URISyntaxException {
		if (this.job != null) {
			jobCreated = this.job;
		} else {
			jobCreated = new Job();
			jobCreated.setJm(new JobMetrics());
			jobCreated.getJm().setJobstarttime(System.currentTimeMillis());
			jobCreated.setPipelineconfig(pipelineconfig);
			if (pipelineconfig.getUseglobaltaskexecutors()) {
				if(nonNull(pipelineconfig.getJobid())) {
					jobCreated.setId(pipelineconfig.getJobid());
				} else {
					pipelineconfig.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
					jobCreated.setId(pipelineconfig.getJobid());
				}
			} else {
				jobCreated.setId(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
			}
			jobCreated.getJm().setJobid(jobCreated.getId());
			jobCreated.getJm().setMode(Boolean.parseBoolean(pipelineconfig.getYarn()) ? DataSamudayaConstants.YARN : Boolean.parseBoolean(pipelineconfig.getMesos()) ? DataSamudayaConstants.MESOS : Boolean.parseBoolean(pipelineconfig.getJgroups()) ? DataSamudayaConstants.JGROUPS : Boolean.parseBoolean(pipelineconfig.getLocal()) ? DataSamudayaConstants.LOCAL : DataSamudayaConstants.EXECMODE_DEFAULT);
			jobCreated.getJm().setJobname(pipelineconfig.getJobname());
			DataSamudayaJobMetrics.put(jobCreated.getJm());
		}

		getDAG(jobCreated);
		return jobCreated;
	}
	int tmptaskid;

	/**
	 * Form nodes and edges and get Directed Acyclic graph 
	 * @param root
	 * @param absfunction
	 */
	protected void formDAGAbstractFunction(AbstractPipeline root, Collection<AbstractPipeline> absfunction) {
		for (var func : absfunction) {
			//Add the verted to graph. 
			graph.addVertex(func);
			//If root not null add edges between root and child nodes.
			if (root != null) {
				graph.addEdge(root, func);
			}
			log.info(PipelineUtils.getFunctions(func.tasks));
			//recursively form edges for root and child nodes.
			formDAGAbstractFunction(func, func.childs);
		}
	}
	private int stageid = 1;

	private String printTasks(List<AbstractPipeline> functions) {
		var tasksnames = functions.stream().map(absfunc -> absfunc.tasks).collect(Collectors.toList());
		return tasksnames.toString();
	}

	private String printStages(Set<Stage> stages) {
		var stagenames = stages.stream().map(sta -> sta.getId()).collect(Collectors.toList());
		return stagenames.toString();
	}
	private Set<Stage> finalstages = new LinkedHashSet<>();
	private Set<Stage> rootstages = new LinkedHashSet<>();


	public void getParents(AbstractPipeline parent, List<AbstractPipeline> parents) {
		if (CollectionUtils.isEmpty(parent.parents)) {
			parents.add(parent);
			return;
		}
		for (AbstractPipeline parentroot : parent.parents) {
			getParents(parentroot, parents);
		}
	}

	/**
	 * Get Directed Acyclic graph for Map reduce API from functions graph 
	 * to stages graph.
	 * @param job
	 * @throws PipelineException 
	 * @throws ExportException 
	 * @throws URISyntaxException 
	 * @throws IOException 
	 * @ 
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	protected void getDAG(Job job) throws PipelineException {
		try {
			log.debug("Induce of DAG started...");
			DirectedAcyclicGraph<Stage, DAGEdge> graphstages = null;
			Map<Object, Stage> taskstagemap = null;
			tmptaskid = 0;
			List<AbstractPipeline> mdsroots = new ArrayList<>();
			getParents(this, mdsroots);
			formDAGAbstractFunction(null, mdsroots);
			var absfunctions = graph.vertexSet();
			for (var absfunction : absfunctions) {
				log.debug("\n\nTasks " + absfunction);
				log.debug("[Parent] [Child]");
				log.debug(printTasks(absfunction.parents) + " , " + printTasks(absfunction.childs));
				log.debug("Task");
				log.debug(PipelineUtils.getFunctions(absfunction.tasks));
			}
			taskstagemap = new HashMap<>();

			graphstages = new DirectedAcyclicGraph<>(DAGEdge.class);
			rootstages.clear();
			var topoaf = new TopologicalOrderIterator<>(graph);
			while (topoaf.hasNext()) {
				var af = topoaf.next();
				log.info(PipelineUtils.getFunctions(af.tasks) + ", task=" + af.tasks);
				// If AbstractFunction is mds then create a new stage object
				// parent and
				// child stage and form the edge between stages.
				if ((af instanceof StreamPipeline) && af.tasks.get(0) instanceof Dummy) {
					var parentstage = new Stage();
					parentstage.setId(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + job.getStageidgenerator().getAndIncrement());
					rootstages.add(parentstage);
					graphstages.addVertex(parentstage);
					taskstagemap.put(af.tasks.get(0), parentstage);
				} else if (af.parents.size() >= 2) {
					if (af.tasks.get(0) instanceof IntersectionFunction && pipelineconfig.getStorage() == STORAGE.COLUMNARSQL
							&& pipelineconfig.getMode().equals(DataSamudayaConstants.MODE_NORMAL) ) {
						List<Stage> stages = new ArrayList<>();
						for (var afparent : af.parents) {
							if(nonNull(afparent.csvoptions)) {
								var parentstage = taskstagemap.get(afparent.tasks.get(0));
								var childstagedisdistinct = new Stage();
								childstagedisdistinct.setId(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + job.getStageidgenerator().getAndIncrement());
								childstagedisdistinct.tasks.add(new DistributedDistinct());
								graphstages.addVertex(parentstage);
								graphstages.addVertex(childstagedisdistinct);
								graphstages.addEdge(parentstage, childstagedisdistinct);
								childstagedisdistinct.parent.add(parentstage);
								parentstage.child.add(childstagedisdistinct);
								taskstagemap.put(childstagedisdistinct.tasks.get(0), childstagedisdistinct);
								stages.add(childstagedisdistinct);
							} else {
								Stage parentstage = taskstagemap.get(afparent.tasks.get(0));
								stages.add(parentstage);
							}
						}
						var childstage = new Stage();
						childstage.setId(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + job.getStageidgenerator().getAndIncrement());
						graphstages.addVertex(childstage);
						childstage.tasks.add(af.tasks.get(0));
						taskstagemap.put(af.tasks.get(0), childstage);
						for (var stage : stages) {
							childstage.parent.add(stage);
							stage.child.add(childstage);
							graphstages.addEdge(stage, childstage);
						}
					} else {
						var childstage = new Stage();
						childstage.setId(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + job.getStageidgenerator().getAndIncrement());
						for (var afparent : af.parents) {
							Stage parentstage = taskstagemap.get(afparent.tasks.get(0));
							graphstages.addVertex(parentstage);
							graphstages.addVertex(childstage);
							graphstages.addEdge(parentstage, childstage);
							childstage.parent.add(parentstage);
							parentstage.child.add(childstage);
						}
						childstage.tasks.add(af.tasks.get(0));
						taskstagemap.put(af.tasks.get(0), childstage);
					}
				} else if (af.parents.size() == 1) {
					// create a new stage and add the abstract function to
					// new stage created and form the edges between last stage
					// and new stage.
					// and pushed to stack.
					if (af.parents.get(0).childs.size() >= 2) {
						var childstage = new Stage();
						childstage.setId(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + job.getStageidgenerator().getAndIncrement());
						for (var afparent : af.parents) {
							var parentstage = taskstagemap.get(afparent.tasks.get(0));
							graphstages.addVertex(parentstage);
							graphstages.addVertex(childstage);
							graphstages.addEdge(parentstage, childstage);
							childstage.parent.add(parentstage);
							parentstage.child.add(childstage);
						}
						childstage.tasks.add(af.tasks.get(0));
						taskstagemap.put(af.tasks.get(0), childstage);
					} else if (!Objects.isNull(af.tasks.get(0)) && (af.tasks.get(0) instanceof Coalesce
							|| af.tasks.get(0) instanceof GroupByKeyFunction
							|| af.tasks.get(0) instanceof GroupByFunction
							|| af.tasks.get(0) instanceof HashPartitioner
							|| af.tasks.get(0) instanceof CountByKeyFunction
							|| af.tasks.get(0) instanceof CountByValueFunction
							|| af.tasks.get(0) instanceof JoinPredicate
							|| af.tasks.get(0) instanceof Join
							|| af.tasks.get(0) instanceof LeftJoin
							|| af.tasks.get(0) instanceof RightJoin
							|| af.tasks.get(0) instanceof LeftOuterJoinPredicate
							|| af.tasks.get(0) instanceof RightOuterJoinPredicate
							|| af.tasks.get(0) instanceof AggregateFunction
							|| af.tasks.get(0) instanceof AggregateReduceFunction
							|| af.tasks.get(0) instanceof SampleSupplierInteger
							|| af.tasks.get(0) instanceof SampleSupplierPartition
							|| af.tasks.get(0) instanceof UnionFunction
							|| af.tasks.get(0) instanceof FoldByKey
							|| af.tasks.get(0) instanceof IntersectionFunction
							|| ((af.tasks.get(0) instanceof ReduceByKeyFunction
							|| af.tasks.get(0) instanceof ReduceByKeyFunctionValues
							|| af.tasks.get(0) instanceof ReduceFunction
							|| af.tasks.get(0) instanceof SortedComparator
					) && pipelineconfig.getStorage() == STORAGE.COLUMNARSQL
							&& pipelineconfig.getMode().equals(DataSamudayaConstants.MODE_NORMAL)
					))
					) {
						stageCreator(graphstages, taskstagemap, af, job);
					} else if (!Objects.isNull(af.parents.get(0).tasks.get(0))
							&& !(af.parents.get(0).tasks.get(0) instanceof Coalesce
							|| af.parents.get(0).tasks.get(0) instanceof GroupByKeyFunction
							|| af.parents.get(0).tasks.get(0) instanceof GroupByFunction
							|| af.parents.get(0).tasks.get(0) instanceof HashPartitioner
							|| af.parents.get(0).tasks.get(0) instanceof CountByKeyFunction
							|| af.parents.get(0).tasks.get(0) instanceof CountByValueFunction
							|| af.parents.get(0).tasks.get(0) instanceof JoinPredicate
							|| af.parents.get(0).tasks.get(0) instanceof LeftOuterJoinPredicate
							|| af.parents.get(0).tasks.get(0) instanceof RightOuterJoinPredicate
							|| af.parents.get(0).tasks.get(0) instanceof AggregateFunction
							|| af.parents.get(0).tasks.get(0) instanceof AggregateReduceFunction
							|| af.parents.get(0).tasks.get(0) instanceof SampleSupplierInteger
							|| af.parents.get(0).tasks.get(0) instanceof SampleSupplierPartition
							|| af.parents.get(0).tasks.get(0) instanceof UnionFunction
							|| af.parents.get(0).tasks.get(0) instanceof FoldByKey
							|| af.parents.get(0).tasks.get(0) instanceof IntersectionFunction
							|| af.parents.get(0).tasks.get(0) instanceof Join
							|| af.parents.get(0).tasks.get(0) instanceof LeftJoin
							|| af.parents.get(0).tasks.get(0) instanceof RightJoin
							|| ((af.parents.get(0).tasks.get(0) instanceof ReduceByKeyFunction
							|| af.parents.get(0).tasks.get(0) instanceof ReduceByKeyFunctionValues
							|| af.parents.get(0).tasks.get(0) instanceof ReduceFunction
							|| af.parents.get(0).tasks.get(0) instanceof SortedComparator							
					) && pipelineconfig.getStorage() == STORAGE.COLUMNARSQL
							&& pipelineconfig.getMode().equals(DataSamudayaConstants.MODE_NORMAL)
					))) {
						var parentstage = taskstagemap.get(af.parents.get(0).tasks.get(0));
						parentstage.tasks.add(af.tasks.get(0));
						taskstagemap.put(af.tasks.get(0), parentstage);
					} else {
						stageCreator(graphstages, taskstagemap, af, job);
					}
				}
			}
			log.debug("Stages----------------------------------------");
			var stagesprocessed = graphstages.vertexSet();
			job.getJm().setStageGraphs(graphstages);
			for (var stagetoprint : stagesprocessed) {
				log.info("\n\nStage " + stagetoprint.getId());
				log.info("[Parent] [Child]");
				log.info(printStages(stagetoprint.parent) + " , " + printStages(stagetoprint.child));
				log.info("Tasks");
				for (var task : stagetoprint.tasks) {
					log.info(PipelineUtils.getFunctions(task));
				}
			}

			finalstages.clear();
			finalstages.add(taskstagemap.get(this.tasks.get(0)));
			var stages = new LinkedHashSet<Stage>();
			if (rootstages.size() == 1 && finalstages.size() == 1 && rootstages.containsAll(finalstages)) {
				stages.addAll(rootstages);
			} else {
				// Directed paths
				var adp = new AllDirectedPaths<>(graphstages);

				// Get graph paths between root stage and final stage.
				List<GraphPath<Stage, DAGEdge>> graphPaths = adp.getAllPaths(rootstages, finalstages, true,
						Integer.MAX_VALUE);
				// Collect the graph paths by getting source and target stages.
				for (var graphpath : graphPaths) {
					var dagedges = graphpath.getEdgeList();
					for (var dagedge : dagedges) {
						stages.add((Stage) dagedge.getSource());
						stages.add((Stage) dagedge.getTarget());
					}
				}
			}
			// Topological ordering of graph stages been computed so that
			// Stage of child will not be excuted not till all the parent stages
			// result been computed.
			Iterator<Stage> topostages = new TopologicalOrderIterator(graphstages);
			while (topostages.hasNext()) {
				job.getTopostages().add(topostages.next());
			}
			job.getTopostages().retainAll(stages);
			var abspipeline = new ArrayList<AbstractPipeline>();
			Set<Stage> stagesblocks = new LinkedHashSet<>();
			boolean allEmptyPigTasks = isAllEmptyPigTasks(mdsroots);
			if (!allEmptyPigTasks) {
				ConcurrentMap<Stage, Object> stageoutputmap = new ConcurrentHashMap<>();
				Iterator<AbstractPipeline> sps = mdsroots.iterator();
				for (Stage stage :rootstages) {
					StreamPipeline sp = (StreamPipeline) sps.next();
					if (nonNull(sp.pigtasks)) {
						stageoutputmap.put(stage, new ArrayList<>(sp.pigtasks));
					} else {
						stagesblocks.add(stage);
						abspipeline.add(sp);
					}
				}
				job.setStageoutputmap(stageoutputmap);
				if (CollectionUtils.isNotEmpty(abspipeline)) {
					var dbPartitioner = new FileBlocksPartitionerHDFS();
					dbPartitioner.getJobStageBlocks(job, supplier, "hdfs", stagesblocks, abspipeline, ((StreamPipeline) this).pipelineconfig);
				}
				PipelineConfig pipeconf = ((StreamPipeline) this).pipelineconfig;
				if (pipeconf.getMode().equalsIgnoreCase(DataSamudayaConstants.MODE_DEFAULT)) {
					var ignite = DataSamudayaIgniteClient.instance(this.pipelineconfig);
					IgniteCache<Object, byte[]> ignitecache = ignite.getOrCreateCache(DataSamudayaConstants.DATASAMUDAYACACHE);
					job.setIgcache(ignitecache);
					job.setIgnite(ignite);
				} else if (pipeconf.getMode().equalsIgnoreCase(DataSamudayaConstants.MODE_NORMAL)
						&& "false".equalsIgnoreCase(pipeconf.getLocal())
						&& "false".equalsIgnoreCase(pipeconf.getYarn())) {
					job.setLcs(GlobalContainerLaunchers.get(pipelineconfig.getUser(), pipelineconfig.getTejobid()));
					List<String> containers = job.getLcs().stream().flatMap(lc -> {
						var host = lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE);
						return lc.getCla().getCr().stream().map(cr -> {
							return host[0] + DataSamudayaConstants.UNDERSCORE + cr.getPort();
						}).collect(Collectors.toList()).stream();
					}).collect(Collectors.toList());
					job.setTaskexecutors(containers);
					job.getJm().setContainersallocated(new ConcurrentHashMap<>());
					// Get nodes
					job.setNodes(job.getLcs().stream().map(lc -> lc.getNodehostport()).collect(Collectors.toSet()));
				}
			} else {
				var dbPartitioner = new FileBlocksPartitionerHDFS();
				dbPartitioner.getJobStageBlocks(job, supplier, "hdfs", rootstages, mdsroots, this.pipelineconfig);
			}
			var writer = new StringWriter();
			if (Boolean.parseBoolean((String) DataSamudayaProperties.get().get(DataSamudayaConstants.GRAPHSTOREENABLE))) {
				Utils.renderGraphStage(graphstages, writer);
			}

			if (Boolean.parseBoolean((String) DataSamudayaProperties.get().get(DataSamudayaConstants.GRAPHSTOREENABLE))) {
				writer = new StringWriter();
				renderGraph(graph, writer);
			}

			stages.clear();
			stages = null;
			log.debug("Induce of DAG ended.");
		} catch (Exception ex) {
			log.error(PipelineConstants.DAGERROR, ex);
			throw new PipelineException(PipelineConstants.DAGERROR, ex);
		}
	}

	/**
	 * Check whether the pig tasks are available in all root
	 * @param abspipelines
	 * @return
	 */
	protected boolean isAllEmptyPigTasks(Collection<AbstractPipeline> abspipelines) {
		Iterator<AbstractPipeline> iteratorpipelines = abspipelines.iterator();
		while (iteratorpipelines.hasNext()) {
			StreamPipeline<?> sp = (StreamPipeline<?>) iteratorpipelines.next();
			if (CollectionUtils.isNotEmpty(sp.pigtasks)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * The method stageCreator creates stage object and forms graph nodes
	 * and edges
	 * @param graphstages
	 * @param taskstagemap
	 * @param af
	 */
	private void stageCreator(DirectedAcyclicGraph<Stage, DAGEdge> graphstages,
			Map<Object, Stage> taskstagemap, AbstractPipeline af, Job job) {
		var parentstage = taskstagemap.get(af.parents.get(0).tasks.get(0));
		if (af.tasks.get(0) instanceof ReduceByKeyFunction || af.tasks.get(0) instanceof ReduceByKeyFunctionValues
				|| af.tasks.get(0) instanceof ReduceFunction && pipelineconfig.getStorage() == STORAGE.COLUMNARSQL
				&& pipelineconfig.getMode().equals(DataSamudayaConstants.MODE_NORMAL)) {
			parentstage.tasks.add(af.tasks.get(0));
		}		
		if (af.tasks.get(0) instanceof SortedComparator && pipelineconfig.getStorage() == STORAGE.COLUMNARSQL
				&& pipelineconfig.getMode().equals(DataSamudayaConstants.MODE_NORMAL)) {
			parentstage.tasks.add(af.tasks.get(0));
			var childstage = new Stage();
			childstage.setId(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + job.getStageidgenerator().getAndIncrement());
			childstage.tasks.add(new DistributedSort());
			graphstages.addVertex(parentstage);
			graphstages.addVertex(childstage);
			graphstages.addEdge(parentstage, childstage);
			childstage.parent.add(parentstage);
			parentstage.child.add(childstage);
			taskstagemap.put(childstage.tasks.get(0), childstage);
			taskstagemap.put(af.tasks.get(0), childstage);
			return;
		}		
		if(nonNull(parentstage)) {
			var childstage = new Stage();
			childstage.setId(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + job.getStageidgenerator().getAndIncrement());
			childstage.tasks.add(af.tasks.get(0));
			graphstages.addVertex(parentstage);
			graphstages.addVertex(childstage);
			graphstages.addEdge(parentstage, childstage);
			childstage.parent.add(parentstage);
			parentstage.child.add(childstage);
			taskstagemap.put(af.tasks.get(0), childstage);
		}
	}


	/**
	 * The method renderGraph writes the graph information to files.
	 * @param graph
	 * @param writer
	 * @throws ExportException
	 */
	private static void renderGraph(Graph<AbstractPipeline, DAGEdge> graph, Writer writer) throws ExportException {
		ComponentNameProvider<AbstractPipeline> vertexIdProvider = task -> {

			try {
				Thread.sleep(500);
			} catch (Exception ex) {
				log.error("Delay Error, see cause below \n", ex);
			}
			return "" + System.currentTimeMillis();

		};
		ComponentNameProvider<AbstractPipeline> vertexLabelProvider = AbstractPipeline::toString;
		GraphExporter<AbstractPipeline, DAGEdge> exporter = new DOTExporter<>(vertexIdProvider, vertexLabelProvider, null);
		exporter.exportGraph(graph, writer);
		var path = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GRAPDIRPATH);
		new File(path).mkdirs();
		try (var stagegraphfile = new FileWriter(path + DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GRAPHTASKFILENAME) + System.currentTimeMillis());) {
			stagegraphfile.write(writer.toString());
		} catch (Exception e) {
			log.error("File Write Error, see cause below \n", e);
		}
	}


	/**
	 * Terminal operation save as file.
	 * @param path
	 * @throws Throwable 
	 * @
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void saveAsTextFile(URI uri, String path) throws PipelineException,Exception {
		var jobcreated = createJob();
		jobcreated.setTrigger(Job.TRIGGER.SAVERESULTSTOFILE);
		jobcreated.setIsresultrequired(true);
		jobcreated.setUri(uri.toString());
		jobcreated.setSavepath(path);
		submitJob(jobcreated);
	}

	/**
	 * 
	 * @param uri
	 * @param path
	 * @throws PipelineException
	 * @throws Exception
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void saveAsTextFilePig(URI uri, String path) throws PipelineException,Exception {
		var mdscollect = (StreamPipeline) this;
		var jobcreated = mdscollect.createJob();
		jobcreated.setTrigger(Job.TRIGGER.SAVERESULTSTOFILE);
		jobcreated.setUri(uri.toString());
		jobcreated.setSavepath(path);
		mdscollect.submitJob(jobcreated);
		mdscollect.graph = new DirectedAcyclicGraph<>(DAGEdge.class);
		mdscollect.childs.clear();
		mdscollect.tasks = new ArrayList<>();
		mdscollect.tasks.add(new Dummy());
	}

	/**
	 * Submit the job to job scheduler.
	 * @param job
	 * @return
	 * @throws Throwable 
	 * @throws Exception 
	 * @
	 */
	private Object submitJob(Job job) throws Exception {
		try {
			boolean ismesos = Boolean.parseBoolean(pipelineconfig.getMesos());
			// If scheduler is yarn?
			boolean isyarn = Boolean.parseBoolean(pipelineconfig.getYarn());
			// If scheduler is local?
			boolean islocal = Boolean.parseBoolean(pipelineconfig.getLocal());
			// If ignite mode is choosen
			boolean isignite = Objects.isNull(pipelineconfig.getMode()) ? false
					: pipelineconfig.getMode().equals(DataSamudayaConstants.MODE_DEFAULT) ? true : false;
			boolean isjgroups = Boolean.parseBoolean(pipelineconfig.getJgroups());
			if (nonNull(pipelineconfig.getIsremotescheduler())
					&& pipelineconfig.getIsremotescheduler()
					&& pipelineconfig.getCpudriver()>0
					&& pipelineconfig.getMemorydriver()>0
					&& ((Boolean.FALSE.equals(ismesos) 
					&& Boolean.FALSE.equals(isyarn) 
					&& Boolean.FALSE.equals(islocal)) || isjgroups)
					&& !isignite) {
				RemoteJobScheduler rjs = new RemoteJobScheduler();
				return rjs.scheduleJob(job);
			} else {
				StreamJobScheduler js = new StreamJobScheduler();
				job.setPipelineconfig(pipelineconfig);
				return js.schedule(job);
			}
		} finally {
			if(pipelineconfig.isWindowspushnotification()) {
				Utils.showNotification(DataSamudayaConstants.DATASAMUDAYA+" Job Completed", Utils.getJobInfo(job), MessageType.INFO);
			}
		}
	}

	/**
	 * Collect the result which is terminal operation.
	 * @param toexecute
	 * @return
	 * @throws PipelineException 
	 * @
	 */
	@SuppressWarnings({"rawtypes"})
	private List collect(boolean toexecute, Job.TRIGGER jobtrigger) throws PipelineException {
		try {
			var job = createJob();
			job.getJm().setSqlpigquery(pipelineconfig.getSqlpigquery());
			if (jobtrigger == Job.TRIGGER.PIGDUMP) {
				job.setJobtype(JOBTYPE.PIG);
			}
			job.setTrigger(jobtrigger);
			var results = new ArrayList();
			if (toexecute && jobtrigger != Job.TRIGGER.PIGDUMP) {
				job.setIsresultrequired(true);
				results = (ArrayList) submitJob(job);
			} else if (toexecute && jobtrigger == Job.TRIGGER.PIGDUMP) {
				job.setIsresultrequired(true);
				if(pipelineconfig.getIsremotescheduler()) {
					results = (ArrayList) submitJob(job);
					for(List result:(List<List>)results) {
						Utils.printTableOrError((List) result, pipelineconfig.getWriter(), JOBTYPE.PIG);
					}
				} else {
					submitJob(job);
				}
			}
			return (List) results;
		}
		catch (Exception ex) {
			log.error(PipelineConstants.CREATEOREXECUTEJOBERROR, ex);
			throw new PipelineException(PipelineConstants.CREATEOREXECUTEJOBERROR, (Exception) ex);
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
	@SuppressWarnings({"rawtypes", "unchecked"})
	public List collect(boolean toexecute, IntSupplier supplier) throws PipelineException {
		try {
			log.debug("Collect task begin...");
			var mdscollect = (StreamPipeline) this;
			Utils.writeToOstream(pipelineconfig.getOutput(), "Collect task begin...");
			if (mdscollect.supplier != null && supplier != null) {
				if (mdscollect.supplier.getAsInt() != supplier.getAsInt()) {
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
			var result = mdscollect.collect(toexecute, Job.TRIGGER.COLLECT);
			log.debug("Collect task ended.");
			Utils.writeToOstream(pipelineconfig.getOutput(), "Collect task ended.");
			return result;
		}
		catch (Exception ex) {
			log.error(PipelineConstants.PIPELINECOLLECTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOLLECTERROR, ex);
		}
	}

	/**
	 * Dumps or prints PIG results
	 * @param toexecute
	 * @param supplier
	 * @throws PipelineException
	 */
	public void dumpPigResults(boolean toexecute, IntSupplier supplier) throws PipelineException {
		try {
			log.debug("Dump task begin...");
			Utils.writeToOstream(pipelineconfig.getOutput(), "Dump task begin...");
			var mdscollect = (StreamPipeline) this;
			if (mdscollect.supplier != null && supplier != null) {
				if (mdscollect.supplier.getAsInt() != supplier.getAsInt()) {
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
			var result = mdscollect.collect(toexecute, Job.TRIGGER.PIGDUMP);
			log.debug("Dump task ended.");
			Utils.writeToOstream(pipelineconfig.getOutput(), "Dump task ended.");
		}
		catch (Exception ex) {
			log.error(PipelineConstants.PIPELINECOLLECTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOLLECTERROR, ex);
		}
	}

	/**
	 * The function count return the results of count.
	 * @param supplier
	 * @return result of count.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public Object count(NumPartitions supplier) throws PipelineException {
		try {
			var sp = new StreamPipeline(this, new CalculateCount());
			var mdscollect = (StreamPipeline) sp;
			if (mdscollect.supplier != null && supplier != null) {
				if (mdscollect.supplier.getAsInt() != supplier.getAsInt()) {
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
			return mdscollect.collect(true, Job.TRIGGER.COUNT);
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
	public void forEach(Consumer<?> consumer, IntSupplier supplier) throws PipelineException {
		try {
			var mdscollect = (StreamPipeline) this;
			if (mdscollect.supplier != null && supplier != null) {
				if (mdscollect.supplier.getAsInt() != supplier.getAsInt()) {
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
			List<StreamPipelineTaskSubmitter> results = mdscollect.collect(true, Job.TRIGGER.FOREACH);
			results.parallelStream().map(spts -> {
				try {
					Task task = spts.getTask();
					if (task.hostport != null) {
						RemoteDataFetch rdf = new RemoteDataFetch();
						rdf.setHp(task.hostport);
						rdf.setJobid(task.jobid);
						rdf.setStageid(task.stageid);
						rdf.setTaskid(task.taskid);
						rdf.setTejobid(task.jobid);
						boolean isJGroups = Boolean.parseBoolean(spts.getPc().getJgroups());
						rdf.setMode(isJGroups ? DataSamudayaConstants.JGROUPS : DataSamudayaConstants.STANDALONE);
						RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
						try (var input = new Input(new ByteArrayInputStream(rdf.getData()));) {
							return Utils.getKryo().readClassAndObject(input);
						}
					} else if (spts.getPc().getLocal().equals(Boolean.TRUE.toString())) {
						try (var fsstream = mdscollect.jobCreated.getResultstream().get(Utils.getIntermediateResultFS(task));
				                ByteBufferInputStream bbis =
										new ByteBufferInputStream(((ByteBufferOutputStream) fsstream).get());
				                var input = new Input(bbis);) {
							return Utils.getKryo().readClassAndObject(input);
						}
					} else if (spts.getPc().getMode().equals(DataSamudayaConstants.MODE_DEFAULT)) {
						try (var sis = new ByteArrayInputStream(
								((StreamPipeline) this).jobCreated.getIgcache().get(task.jobid + task.stageid + task.taskid));
								var input = new Input(sis);) {
							return Utils.getKryo().readClassAndObject(input);
						}
					}
				} catch (Exception ex) {
					log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
				}
				return Arrays.asList();
			}).forEach((Consumer) consumer);
			Utils.destroyTaskExecutors(mdscollect.jobCreated);
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
		return "StreamPipeline [task=" + tasks + "]";
	}

	public Set<Task> getPigtasks() {
		return pigtasks;
	}

	public void clearChild() {
		this.childs.clear();
	}

	public CsvOptions getCsvOptions() {
		return this.csvoptions;
	}

	public Json getJson() {
		return this.json;
	}

}
