call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceYARN -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /github /examplesdatasamudaya yarn"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinYARN -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinYARN -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.sql.examples.StreamingSqlExamples -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /sqlexampleyarnoutput yarn false"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.sql.examples.StreamingSqlExamples -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /sqlexampleyarnoutput yarn false"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayLocal -user arun -numbercontainers 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -arguments "hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 1"

call mapreducejobsubmitter.cmd -jar -jar ../examples/examples-4.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayYARN /airline1989 /carriers /examplesdatasamudaya 3 1024"

call streamjobsubmitter.cmd -jar ../examples/examples-4.0.jar -class com.github.datasamudaya.stream.sql.dataframe.DataFrameAggregate -user arun -numbercontainers 2 -containercpu 2 -containermemory 3072 -drivercpu 3 -drivermemory 4096 -isdriverrequired false -arguments "/flight hdfs://127.0.0.1:9000 2048 yarn"