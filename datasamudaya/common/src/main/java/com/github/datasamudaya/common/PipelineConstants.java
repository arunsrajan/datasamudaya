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
package com.github.datasamudaya.common;


/**
 * 
 * @author arun
 * This class holds the information of the error messages for the pipeline tasks errors.
 */
public class PipelineConstants {
	private PipelineConstants() {
	}
	//Exception Messages
	public static final String URISYNTAXNOTPROPER = "URI syntax not proper";
	public static final String FILEIOERROR = "File IO Error";
	public static final String CREATEOREXECUTEJOBERROR = "Create or Execute Job Error";
	public static final String DELETEINTERMEDIATEPHASEOUTPUTERROR = "Deleting intermediate stage output error, See Cause Below";

	public static final String UNKNOWNERROR = "Unknown Error";
	public static final String PROCESSHDFSERROR = "Process map task execution error";
	public static final String PROCESSHDFSINTERSECTION = "Process intersection task execution error";
	public static final String PROCESSHDFSUNION = "Process union task execution error";
	public static final String PROCESSJOIN = "Process Join task execution error";
	public static final String PROCESSSAMPLE = "Process Sample task execution error";
	public static final String PROCESSLEFTOUTERJOIN = "Process Left Outer Join task execution error";
	public static final String PROCESSRIGHTOUTERJOIN = "Process Right Outer Join task execution error";
	public static final String PROCESSGROUPBYKEY = "Process Group by key task execution error";
	public static final String PROCESSCOUNTBYKEY = "Process Count by key task execution error";
	public static final String PROCESSCOUNTBYVALUE = "Process Count by value task execution error";
	public static final String PROCESSCOALESCE = "Process Coaslesce task execution error";
	public static final String PROCESSHASHPARTITION = "Process HashPartition task execution error";
	public static final String PROCESSFOLDBYKEY = "Process Fold by key task execution error";
	public static final String RESOURCESUNAVAILABLE = "Resource Unavailable To Process Tasks";
	public static final String DAGERROR = "DAG Formation Error";
	public static final String MAPFUNCTIONNULL = "MapFunction cannot be null";
	public static final String PREDICATENULL = "Predicate cannot be null";
	public static final String UNIONNULL = "Union cannot be null";
	public static final String INTERSECTIONNULL = "Intersection cannot be null";
	public static final String MAPPAIRNULL = "MapPair cannot be null";
	public static final String SAMPLENULL = "Sample cannot be null";
	public static final String RIGHTOUTERJOIN = "Right Outer Join cannot be null";
	public static final String RIGHTOUTERJOINCONDITION = "Right Outer Join condition cannot be null";
	public static final String LEFTOUTERJOIN = "Left Outer Join cannot be null";
	public static final String LEFTOUTERJOINCONDITION = "Left Outer Join condition cannot be null";
	public static final String INNERJOIN = "Inner Join cannot be null";
	public static final String INNERJOINCONDITION = "Inner Join condition cannot be null";
	public static final String OUTERJOIN = "Full Outer Join cannot be null";
	public static final String FLATMAPNULL = "FlatMap cannt be null";
	public static final String FLATMAPPAIRNULL = "FlatMap Pair cannot be null";
	public static final String LONGFLATMAPNULL = "Long FlatMap cannt be null";
	public static final String DOUBLEFLATMAPNULL = "Double FlatMap cannot be null";
	public static final String PEEKNULL = "Peek cannot be null";
	public static final String SORTEDNULL = "Sort cannot be null";
	public static final String MAPTOINTNULL = "MapToInt cannot be null";
	public static final String KEYBYNULL = "KeyBy cannot be null";
	public static final String MAPVALUESNULL = "MapValues cannot be null";
	public static final String COALESCENULL = "Coalesce cannot be null";
	public static final String GROUPBYNULL = "GroupBy Function cannot be null";
	public static final String REDUCENULL = "Reduce cannot be null";
	public static final String FOLDLEFTREDUCENULL = "FoldLeft Reduce cannot be null";
	public static final String FOLDLEFTCOALESCENULL = "FoldLeft Coalesce cannot be null";
	public static final String FOLDRIGHTREDUCENULL = "FoldRight Reduce cannot be null";
	public static final String FOLDRIGHTCOALESCENULL = "FoldRight Coalesce cannot be null";
	public static final String PIPELINECOLLECTERROR = "Exception in pipeline collect execution error";
	public static final String PIPELINECOUNTERROR = "Exception in pipeline count execution error";
	public static final String PIPELINEFOREACHERROR = "Exception in pipeline foreach execution error";
	public static final String JOBSCHEDULERERROR = "Exception in stream job scheduler";
	public static final String JOBSCHEDULEPARALLELEXECUTIONLOCALMODEERROR = "Exception in stream job scheduler parallel execution local mode error";
	public static final String JOBSCHEDULEPARALLELEXECUTIONERROR = "Exception in stream job scheduler parallel execution error";
	public static final String JOBSCHEDULERPHYSICALEXECUTIONPLANERROR = "Physical task execution plan error";
	public static final String JOBSCHEDULERFINALSTAGERESULTSERROR = "Acquiring final stage results error";
	public static final String JOBSCHEDULERCREATINGSTREAMSCHEDULERTHREAD = "Creating stream task scheduler thread error";
	public static final String JOBSCHEDULERGETTINGTASKEXECUTORLOADBALANCEDERROR = "Acquiring task executor load balaned error";
	public static final String JOBSCHEDULERINMEMORYDATAFETCHERROR = "Acquiring in memory data fetch error";
	public static final String JOBSCHEDULERHDFSDATAFETCHERROR = "Acquiring data fetch from hdfs filesystem error";
	public static final String JOBSCHEDULERCONTAINERERROR = "Container start error";
	public static final String DESTROYCONTAINERERROR = "Container destroy error";
	public static final String FILEPATHERROR = "File paths acquire error";
	public static final String FILEBLOCKSERROR = "File blocks acquire error";
	public static final String FILEBLOCKSPARTITIONINGERROR = "File blocks partitioning error";
	public static final String TASKEXECUTORSALLOCATIONERROR = "Task executors allocation error";
	public static final String TASKFAILEDEXCEPTION = "Task Failed Exception";
	public static final String MEMORYALLOCATIONERROR = "Memory allocation error Minimum Required Memory is 128 MB";
	public static final String INSUFFMEMORYALLOCATIONERROR = "Insufficient memory, required memory is greater than the available memory";
	public static final String INSUFFCPUALLOCATIONERROR = "Insufficient cpu, required cpu is greater than the available cpu";
	public static final String INSUFFNODESERROR = "Insufficient computing nodes";
	public static final String RESOURCESDOWNRESUBMIT = "Resources (TaskExecutor) %s down while manipulating blocks, Please Resubmit";
	public static final String INSUFFNODESFORDATANODEERROR = "Insufficient computing nodes for datanode %s,available computing nodes are %d";
	public static final String ERROREDTASKS = "Task execution exceeded the limit %s, See the Errored tasks, please find below for cause: ";
	public static final String CONTAINERALLOCATIONERROR = "Container Allocation Error";
	public static final String REDUCEEXECUTIONVALUEEMPTY = "Reduce Execution Empty Value Error";
	public static final String UNSUPPORTEDMEMORYALLOCATIONMODE = "Unsupported Memory Allocation Mode Error";
	public static final String USERALLOCATEDSHARE = "User %s has already allocated his share";
	public static final String USERNOTCONFIGURED = "User %s has no allocation";
	public static final String INSUFFICIENTRESOURCES = "User '%s' has insufficient resources for allocating cpu %s or memory %s to %s";
	public static final String TABLEMUSHHAVEFIELDS = "Table must have at least one field";
	public static final String EITHERTABLEISNOTAVAILABLEINSCHEMAORNOFIELDS = "Table should be present in Schema '%s'";
	public static final String HDFSCONNECTERRORGENERIC = "Unable to connect to HDFS";
	public static final String HDFSCONNECTERROR = "Unable to connect to HDFS at '%s'";
	public static final String HDFSFILENOTFOUND = "File(s) not found in folder '%s'";
}
