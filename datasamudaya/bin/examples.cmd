call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceJGroupsDivided  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemory hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryImplicit hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 1 11 18432

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalJGroupsImplicit hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 1 11 18432

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemorySortByDelayResourceDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemorySortByAirlinesResourceDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesdatasamudaya sa

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesdatasamudaya local

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesdatasamudaya yarn

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesdatasamudaya jgroups

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduceIgnite hdfs://127.0.0.1:9000 /github /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.IgnitePipelineFile C:/DEVELOPMENT/dataset/airline/1987/

REM Stream Reduce LOJ

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinJGroupsDivided  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinNormal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinNormalDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 18432 3 128

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 18432 3 128


REM Stream Reduce ROJ


call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinJGroupsDivided  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinNormal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinNormalDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 18432 3 128

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 18432 3 128

REM Stream Reduce Transformations

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceSample hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalesceOne hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalescePartition hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceUnion hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceIntersection hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCachedIgnite hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoin hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoin hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoin hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoinCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoinCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoinCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairMultipleJoinsCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceSampleLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalesceOneLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalescePartitionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceUnionLocal hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceIntersectionLocal hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCachedIgniteLocal hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoinLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoinLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoinLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoinCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoinCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoinCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairMultipleJoinsCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

REM Stream Reduce SQL

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.StreamingSqlExamples hdfs://127.0.0.1:9000 /airline1989 /sqlexamplestandaloneoutput standalone true arun 2 4096

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.StreamingSqlExamples hdfs://127.0.0.1:9000 /airline1989 /sqlexamplejgroupsoutput jgroups true arun 2 4096

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.StreamingSqlExamples hdfs://127.0.0.1:9000 /airline1989 /sqlexampleyarnoutput yarn false

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumLocal hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumSAInMemory hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumSAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumSADisk hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlCountLocal hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelayLocal hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySADisk hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemory hdfs://127.0.0.1:9000 /airline1989 128 11


call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelayLocal hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySADisk hdfs://127.0.0.1:9000 /airline1989 128 11 18432 1

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemory hdfs://127.0.0.1:9000 /airline1989 128 11 18432 1

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 128 11 18432 1

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelayLocal hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelaySaveLocal hdfs://127.0.0.1:9000 /airline1989 128 11 /examplesdatasamudaya

Stream Reduce Aggregate
-----------------------

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayDisk hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayDiskDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 3

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayIgnite hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemory hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 3

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 3

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayCoalesceInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayCoalesceInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 3

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayJGroups hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayJGroupsDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 3

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayLocal hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayYARN hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1


call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamCoalesceNormalInMemoryDiskContainerDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1

REM Filter Operation Streaming

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamFilterFilterCollectArrDelayInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1 32


REM Running MR Job examples

call mapreducejobsubmitter.cmd -jar ../examples/examples-2.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayNormal /airline1989 /carriers /examplesdatasamudaya 128 10"

call mapreducejobsubmitter.cmd -jar ../examples/examples-2.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayYARN /airline1989 /carriers /examplesdatasamudaya 3 18432" 

call mapreducejobsubmitter.cmd -jar ../examples/examples-2.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayIGNITE /airline1989 /carriers /examplesdatasamudaya"


pause