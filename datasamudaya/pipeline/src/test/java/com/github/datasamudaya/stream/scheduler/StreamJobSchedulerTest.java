package com.github.datasamudaya.stream.scheduler;

import java.util.List;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;

import com.github.datasamudaya.common.Task;

import org.jgrapht.Graph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.junit.Test;

import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.DataSamudayaProperties;

import junit.framework.TestCase;

public class StreamJobSchedulerTest extends TestCase {

	@Test
	public void testUpdateGraph() throws Exception {
		DataSamudayaProperties.put(new Properties());
		StreamJobScheduler sjs = new StreamJobScheduler();
		String[] tes = { "192.168.10.2_1024", "192.168.10.3_1024", "192.168.10.3_1025", "192.168.10.2_1025" };
		List<String> availabletes = new ArrayList<>(Arrays.asList(tes));
		List<String> deadtes = new ArrayList<>();
		deadtes.add(availabletes.remove(0));
		Graph<StreamPipelineTaskSubmitter, DAGEdge> graph = createGraph(tes);
		sjs.updateOriginalGraph(graph, deadtes, availabletes);
		assertGraph(graph,getRoots(graph), availabletes);
	}
	
	
	@Test
	public void testUpdateGraphMultipleTesDown() throws Exception {
		DataSamudayaProperties.put(new Properties());
		StreamJobScheduler sjs = new StreamJobScheduler();
		String[] tes = { "192.168.10.2_1024", "192.168.10.3_1024", "192.168.10.3_1025", "192.168.10.2_1025" };
		List<String> availabletes = new ArrayList<>(Arrays.asList(tes));
		List<String> deadtes = new ArrayList<>();
		deadtes.add(availabletes.remove(0));
		deadtes.add(availabletes.remove(0));
		Graph<StreamPipelineTaskSubmitter, DAGEdge> graph = createGraph(tes);
		sjs.updateOriginalGraph(graph, deadtes, availabletes);
		assertGraph(graph,getRoots(graph), availabletes);
	}
	
	
	private static List<StreamPipelineTaskSubmitter> getRoots(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph) {
        List<StreamPipelineTaskSubmitter> roots = new ArrayList<>();

        for (StreamPipelineTaskSubmitter vertex : graph.vertexSet()) {
            if (graph.incomingEdgesOf(vertex).isEmpty()) {
            	roots.add(vertex);
            }
        }

        return roots;
    }

	protected Graph<StreamPipelineTaskSubmitter, DAGEdge> createGraph(String[] tes) {
		List<StreamPipelineTaskSubmitter> sptss = new ArrayList<>();
		Graph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(DAGEdge.class);
		int teslength = tes.length;
		Random rand = new Random(System.currentTimeMillis());
		for (int index = 0; index < 100; index++) {
			Task task = new Task();
			task.taskid = "Task" + (index + 1);
			String hostport = tes[rand.nextInt(100) % teslength];
			task.setHostport(hostport);
			var spts = new StreamPipelineTaskSubmitter(task, hostport, null);
			sptss.add(spts);
			graph.addVertex(spts);
		}
		formGraph(graph, formGraph(graph, sptss, 100), 200);
		return graph;
	}

	protected List<StreamPipelineTaskSubmitter> formGraph(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			List<StreamPipelineTaskSubmitter> roots, int startindex) {
		List<StreamPipelineTaskSubmitter> sptss = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Task task = new Task();
			task.taskid = "Task" + (startindex + index + 1);
			String hostport = roots.get(index).getHostPort();
			task.setHostport(hostport);
			var spts = new StreamPipelineTaskSubmitter(task, hostport, null);
			sptss.add(spts);
			graph.addVertex(spts);
			graph.addEdge(roots.get(index), spts);
		}
		return sptss;
	}

	protected void assertGraph(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph, List<StreamPipelineTaskSubmitter> roottasks, List<String> availabletes) {
		for(int index = 0; index<roottasks.size();index++) {
			StreamPipelineTaskSubmitter spts = roottasks.get(index);
			assertTrue(availabletes.contains(spts.getTask().getHostport()));
			Set<DAGEdge> dagedges = graph.outgoingEdgesOf(spts);
			for(DAGEdge dagedge: dagedges) {
				StreamPipelineTaskSubmitter childtask = graph.getEdgeTarget(dagedge);
				assertGraph(graph, Arrays.asList(childtask), availabletes);
			}
		}
	}
	
	
}
