call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceIgnite  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayIgnite -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduceIgnite -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /github /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.IgnitePipelineFile C:/DEVELOPMENT/dataset/airline/1987/

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinIgnite  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinIgnite  -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceCachedIgnite -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.transformation.examples.StreamReduceCachedIgniteLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayIgnite -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 18432 1"

call mapreducejobsubmitter.cmd -jar -jar ../examples/examples-4.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayIGNITE /airline1989 /carriers /examplesdatasamudaya"