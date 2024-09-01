call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceIgnite  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceJGroups  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceJGroupsDivided  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceNormalInMemory -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryImplicit -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 1 11 18432"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceNormalJGroupsImplicit -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 1 11 18432"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDiskDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceNormalDisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceNormalDiskDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceYARN -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceNormalInMemorySortByDelayResourceDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceNormalInMemorySortByAirlinesResourceDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /github /examplesdatasamudaya sa"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /github /examplesdatasamudaya local"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /github /examplesdatasamudaya yarn"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /github /examplesdatasamudaya jgroups"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduceIgnite -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /github /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.IgnitePipelineFile C:/DEVELOPMENT/dataset/airline/1987/

REM Stream Reduce LOJ

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinIgnite  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinJGroups  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinJGroupsDivided  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinNormal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinNormalDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinYARN -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinInMemoryDisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 18432 3 128"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinInMemoryDiskDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 18432 3 128"


REM Stream Reduce ROJ


call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinIgnite  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinJGroups  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinJGroupsDivided  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinNormal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinNormalDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinYARN -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinInMemoryDisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 18432 3 128"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinInMemoryDiskDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 18432 3 128"

REM Stream Reduce Transformations

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceSample -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalesceOne -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalescePartition -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceUnion -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceIntersection -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceCachedIgnite -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoin -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoin -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoin -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoinCoalesceReduction -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoinCoalesceReduction -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoinCoalesceReduction -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairMultipleJoinsCoalesceReduction -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceSampleLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalesceOneLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalescePartitionLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceUnionLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceIntersectionLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceCachedIgniteLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoinLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoinLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoinLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoinCoalesceReductionLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoinCoalesceReductionLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoinCoalesceReductionLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReducePairMultipleJoinsCoalesceReductionLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

REM Stream Reduce SQL

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.StreamingSqlExamples -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /sqlexamplestandaloneoutput standalone true arun 2 4096"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.StreamingSqlExamples -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /sqlexamplejgroupsoutput jgroups true arun 2 4096"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.StreamingSqlExamples -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /sqlexampleyarnoutput yarn false"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlSumLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlSumSAInMemory -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlSumSAInMemoryDisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlSumSADisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlCountLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 128 11"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelayLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 128 11"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySADisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 128 11"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemory -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 128 11"


call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemoryDisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 128 11"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelayLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 128 11"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySADisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 128 11 18432 1"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemory -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 128 11 18432 1"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemoryDisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 128 11 18432 1"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelayLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 128 11"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelaySaveLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 128 11 /examplesdatasamudaya"

Stream Reduce Aggregate
-----------------------

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayDisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayDiskDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 3"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayIgnite -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemory -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 3"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDiskDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 3"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayCoalesceInMemoryDisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayCoalesceInMemoryDiskDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 3"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayJGroups -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayJGroupsDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 3"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1"

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayYARN -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1"


call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamCoalesceNormalInMemoryDiskContainerDivided -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1"

REM Filter Operation Streaming

call streamjobsubmitter.cmd -jar ../examples/examples-3.0.jar -class com.github.datasamudaya.stream.examples.StreamFilterFilterCollectArrDelayInMemoryDisk -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1 32"


REM Running MR Job examples

call mapreducejobsubmitter.cmd -jar -jar ../examples/examples-3.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayNormal /airline1989 /carriers /examplesdatasamudaya 128 10"

call mapreducejobsubmitter.cmd -jar -jar ../examples/examples-3.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayYARN /airline1989 /carriers /examplesdatasamudaya 3 18432" 

call mapreducejobsubmitter.cmd -jar -jar ../examples/examples-3.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayIGNITE /airline1989 /carriers /examplesdatasamudaya"


pause