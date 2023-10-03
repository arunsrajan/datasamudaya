package com.github.datasamudaya.stream.utils;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jgrapht.Graph;

import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaJobMetrics;
import com.github.datasamudaya.common.RemoteDataFetch;
import com.github.datasamudaya.common.Stage;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.stream.PipelineUtils;



/**
 * Graph Servlet for pipeline stages and 
 * @author Arun
 */
public class PipelineGraphWebServlet extends HttpServlet {

	private static final long serialVersionUID = 8713220540678338208L;

	/**
	 * Returns the response of graph in nodes and edges. 
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		response.setContentType(DataSamudayaConstants.TEXTHTML);
		response.setStatus(HttpServletResponse.SC_OK);
		var writer = response.getWriter();
		String contextpath = request.getScheme() + "://" + request.getServerName() + DataSamudayaConstants.COLON
				+ request.getLocalPort();
		String jobId = request.getParameter("jobId");
		String graph = request.getParameter("graph");
		boolean istasks = graph.equalsIgnoreCase("task");
		boolean isstage = graph.equalsIgnoreCase("stage");
		try {
			var jm = DataSamudayaJobMetrics.get().get(jobId);
			StringBuilder builder = new StringBuilder();
			builder.append(String.format("""
					        <!DOCTYPE html>
					<html>
					<head>
						<title>""" +jobId+ """
								</title>
						<script src="%s/resources/jquery-3.6.0.min.js"></script>
						<script src="%s/resources/highcharts.js"></script>
						<script src="%s/resources/networkgraph.js"></script>
						<script src="%s/resources/organization.js"></script>
					</head>
					<body>
						<div id="container"></div>
						<script>
							$(document).ready(function() {
								// Define the chart options
								var options = {
									chart: {
										type: 'networkgraph',
										marginBottom: 100
									},
									plotOptions: {
										networkgraph: {
											keys: ['from', 'to'],
											layoutAlgorithm: {
												enableSimulation: true,
												linkLength: 100
											}
										}
									},
									series: [{
										data: [""" + (isstage?getStageGraphEdges(jm.getStageGraphs()):istasks?getTaskGraphEdges(jm.getTaskGraphs()):getStageGraphEdges(jm.getStageGraphs())) + """

										],
										dataLabels: {
											enabled: true,
											style: {
												fontSize: '12px',
												color: '#000000'
											}
										}
									}],
									title: {
										text: '"""+jobId+"""
												'
									}
								};

								// Create the chart
								$('#container').highcharts(options);
							});
						</script>
					</body>
					</html>
					""",contextpath,contextpath,contextpath,contextpath));
			writer.write(builder.toString());
		} finally {

		}
	}
	
	/**
	 * Stage graph nodes and edges in string json format .
	 * @param graph
	 * @return from and to in string format
	 */
	public String getStageGraphEdges(Graph<Stage, DAGEdge> graph) {
		var builder = new StringBuilder();
		var sourcebuilder = new StringBuilder();
		var targetbuilder = new StringBuilder();
		Set<DAGEdge> edges = graph.edgeSet();
		for(DAGEdge edge: edges){
			Stage stagesource = (Stage) edge.getSource();
			Stage stagetarget = (Stage) edge.getTarget();			
			builder.append("{ from: '"
			+getStageTasks(sourcebuilder, stagesource)+"', to: '"
					+getStageTasks(targetbuilder, stagetarget)+"', name: '"+stagesource.getId()+DataSamudayaConstants.HYPHEN+stagetarget.getId()+"' },");
			sourcebuilder.delete(0, sourcebuilder.length());
			targetbuilder.delete(0, targetbuilder.length());
		}
		String fromtobuild = builder.toString();
		return fromtobuild.substring(0,fromtobuild.length()-1);
	}
	
	/**
	 * Task graph nodes and edges in string json format .
	 * @param graph
	 * @return from and to in string format
	 */
	public String getTaskGraphEdges(Graph<Task, DAGEdge> graph) {
		var builder = new StringBuilder();
		var sourcebuilder = new StringBuilder();
		var targetbuilder = new StringBuilder();
		Set<DAGEdge> edges = graph.edgeSet();
		for(DAGEdge edge: edges){
			Task tasksource = (Task) edge.getSource();
			Task tasktarget = (Task) edge.getTarget();			
			builder.append("{ from: '"
			+getTasks(sourcebuilder, tasksource)+"', to: '"
					+getTasks(targetbuilder, tasktarget)+"', name: '"+tasksource.taskid+DataSamudayaConstants.HYPHEN+tasktarget.taskid+"' },");
			sourcebuilder.delete(0, sourcebuilder.length());
			targetbuilder.delete(0, targetbuilder.length());
		}
		String fromtobuild = builder.toString();
		return fromtobuild.substring(0,fromtobuild.length()-1);
	}
	/**
	 * Gets all tasks from stage.
	 * @param builder
	 * @param stage
	 * @return tasks in html format.
	 */
	public String getStageTasks(StringBuilder builder,Stage stage) {
		for (var task : stage.tasks) {
			builder.append(PipelineUtils.getFunctions(task));
			builder.append("<BR/>");
		}
		return builder.toString();
	}
	
	
	/**
	 * Gets a task in html format.
	 * @param builder
	 * @param task
	 * @return task in html format
	 */
	public String getTasks(StringBuilder builder,Task task) {
		Object[] input = task.input;
		for(Object obj:input) {
			if(obj instanceof BlocksLocation bl) {
				builder.append(task.taskid);
				builder.append("<BR/>");
				builder.append(bl.getExecutorhp());
				builder.append("<BR/>");
			}			
		}
		RemoteDataFetch[] rdfs = task.parentremotedatafetch;
		if(rdfs!=null) {
			for (RemoteDataFetch rdf : rdfs) {
				builder.append(task.taskid);
				builder.append("<BR/>");
				builder.append(task.getHostport());
				builder.append("<BR/>Parent_");
				builder.append(rdf.getTaskid());
				builder.append("<BR/>");
			}
		}
		
		
		
		return builder.toString();
	}
	
	
}