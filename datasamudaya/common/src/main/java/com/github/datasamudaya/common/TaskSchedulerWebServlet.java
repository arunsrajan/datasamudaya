/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.common;

import static java.util.Objects.nonNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.github.datasamudaya.common.utils.Utils;

/**
 * Task Scheduler Servlet
 * 
 * @author Arun
 */
public class TaskSchedulerWebServlet extends HttpServlet {

	private static final long serialVersionUID = 8713220540678338208L;
	private static final Logger log = Logger.getLogger(TaskSchedulerWebServlet.class);

	/**
	 * The implementation method doGet of HttpServlet class implements for getting
	 * the resources and job information.
	 * 
	 * @author arun
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		response.setContentType(DataSamudayaConstants.TEXTHTML);
		response.setStatus(HttpServletResponse.SC_OK);
		var writer = response.getWriter();
		String contextpath = request.getScheme() + "://" + request.getServerName() + DataSamudayaConstants.COLON
				+ request.getLocalPort();
		try {
			var lists = DataSamudayaNodesResources.get();
			StringBuilder builder = new StringBuilder();
			builder.append("""
					<html>
					<head>
					<link href="%s/resources/datatables/DataTables-1.13.8/css/jquery.dataTables.css" rel="stylesheet">
					<link href="%s/resources/datatables/Buttons-2.4.2/css/buttons.dataTables.css" rel="stylesheet">

					<script src="%s/resources/jquery-3.7.0.js"></script>
					<script src="%s/resources/datatables/jQuery-1.12.4/jquery-1.12.4.js"></script>
					<script src="%s/resources/datatables/JSZip-3.10.1/jszip.js"></script>
					<script src="%s/resources/datatables/pdfmake-0.2.7/pdfmake.js"></script>
					<script src="%s/resources/datatables/pdfmake-0.2.7/vfs_fonts.js"></script>
					<script src="%s/resources/datatables/DataTables-1.13.8/js/jquery.dataTables.js"></script>
					<script src="%s/resources/datatables/Buttons-2.4.2/js/dataTables.buttons.js"></script>
					<script src="%s/resources/datatables/Buttons-2.4.2/js/buttons.html5.js"></script>
					""".formatted(contextpath, contextpath, contextpath, contextpath, contextpath, contextpath, contextpath,
					contextpath, contextpath, contextpath));
			builder.append("""
					   <script>
						$(document).ready(function(){
							var res = $('#resources').DataTable({
					  			    dom: 'Bflrtip',
					    buttons: [
					        'copy', 'excel', 'pdf'
					    ]
					});
							var metrics = $('#metrics').DataTable({
					  			    dom: 'Bflrtip',
					    buttons: [
					        'copy', 'excel', 'pdf'
					    ]
					});
						});
					   </script>
					   </head>
					   <body>""");

			if (!Objects.isNull(lists) && lists.keySet().size() > 0) {
				builder.append(
						"""
										<table style=\"color:#000000;border-collapse:collapse;width:800px;height:30px\" align=\"center\" border=\"1.0\" id="resources" class="display">
								<thead><th>Node</th><th>FreeMemory</th><th>TotalProcessors</th><th>Physicalmemorysize</th><th>Totaldisksize</th><th>Totalmemory</th><th>Usabledisksize</th><th></th></thead>
								<tbody>""");
				int i = 0;
				for (var node : lists.keySet()) {
					Resources resources = lists.get(node);
					String[] nodeport = node.split(DataSamudayaConstants.UNDERSCORE);
					builder.append("<tr bgcolor=\"").append(Utils.getColor(i++)).append("\">");
					builder.append("<td>");
					builder.append(resources.getNodeport());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getFreememory());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getNumberofprocessors());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getPhysicalmemorysize());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getTotaldisksize());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getTotalmemory());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getUsabledisksize());
					builder.append("</td>");
					builder.append("<td>");
					builder.append("<a href=\"http://" + nodeport[0] + DataSamudayaConstants.COLON
							+ (Integer.parseInt(nodeport[1]) + DataSamudayaConstants.PORT_OFFSET) + "\">");
					builder.append(nodeport[0] + DataSamudayaConstants.COLON
							+ (Integer.parseInt(nodeport[1]) + DataSamudayaConstants.PORT_OFFSET));
					builder.append("</a>");
					builder.append("</td>");
					builder.append("</tr>");
				}
				builder.append("</tbody></table>");
			}

			if (!CollectionUtils.isEmpty(DataSamudayaNodesResources.getAllocatedResources())) {
				ConcurrentMap<String, ConcurrentMap<String, Resources>> userres = DataSamudayaNodesResources
						.getAllocatedResources();
				var aint = new AtomicInteger(0);
				userres.entrySet().stream().forEach(userresmap -> {
					builder.append("<BR/>");
					builder.append("<h1 align=\"center\">");
					builder.append(userresmap.getKey());
					builder.append("</h1>");
					String[] nodeport = userresmap.getKey().split(DataSamudayaConstants.UNDERSCORE);
					builder.append("""
									<script language="Javascript" type="text/javascript">
											$(document).ready(function(){
											var allocres%s = $('#allocatedresources%s').DataTable({
									 			    dom: 'Bflrtip',
													    buttons: [
													        'copy', 'excel', 'pdf'
													    ]
													});
													});
													</script>
													<table style=\"color:#ff0000;border-collapse:collapse;width:800px;height:30px\" align=\"center\" border=\"1.0\" id="allocatedresources%s" class="display">
											<thead><th>User</th><th>Node</th><th>FreeMemory</th><th>TotalProcessors</th><th>Physicalmemorysize</th><th>Totaldisksize</th><th>Totalmemory</th><th>Usabledisksize</th><th></th></thead>
											<tbody>""".formatted(
							aint.get(), aint.get(), aint.getAndIncrement()));
					int i = 0;
					ConcurrentMap<String, Resources> nodeallocated = userresmap.getValue();
					for (var user : nodeallocated.keySet()) {
						Resources resources = nodeallocated.get(user);
						builder.append("<tr>");
						builder.append("<td>");
						builder.append(user);
						builder.append("</td>");
						builder.append("<td>");
						builder.append(resources.getNodeport());
						builder.append("</td>");
						builder.append("<td>");
						builder.append(resources.getFreememory());
						builder.append("</td>");
						builder.append("<td>");
						builder.append(resources.getNumberofprocessors());
						builder.append("</td>");
						builder.append("<td>");
						builder.append(resources.getPhysicalmemorysize());
						builder.append("</td>");
						builder.append("<td>");
						builder.append(resources.getTotaldisksize());
						builder.append("</td>");
						builder.append("<td>");
						builder.append(resources.getTotalmemory());
						builder.append("</td>");
						builder.append("<td>");
						builder.append(resources.getUsabledisksize());
						builder.append("</td>");
						builder.append("<td>");
						builder.append("<a href=\"http://" + nodeport[0] + DataSamudayaConstants.COLON
								+ (Integer.parseInt(nodeport[1]) + DataSamudayaConstants.PORT_OFFSET) + "\">");
						builder.append(nodeport[0] + DataSamudayaConstants.COLON
								+ (Integer.parseInt(nodeport[1]) + DataSamudayaConstants.PORT_OFFSET));
						builder.append("</a>");
						builder.append("</td>");
						builder.append("</tr>");
					}
					builder.append("</tbody></table>");
				});
			}

			Map<String, Map<String, List<LaunchContainers>>> userjobcontainersmap = GlobalContainerLaunchers
					.getUserContainersMap();
			if (nonNull(userjobcontainersmap)) {
				var aint = new AtomicInteger(0);
				userjobcontainersmap.keySet().forEach(user -> {
					builder.append("<br/>");
					builder.append("<br/>");
					builder.append("<H1 align=\"center\">");
					builder.append(user);
					builder.append("</H1>");
					Map<String, List<LaunchContainers>> usercontainersmap = userjobcontainersmap.get(user);
					usercontainersmap.keySet().forEach(jobid -> {
						builder.append("<br/>");
						builder.append("""
										  			<script language="Javascript" type="text/javascript">
										  		$(document).ready(function(){
										  			var te%s = $('#taskexecutors%s').DataTable({
										  			    dom: 'Bflrtip',
										    buttons: [
										        'copy', 'excel', 'pdf'
										    ]
										});
										  		});
										      </script>
										              <table style="color:#000000;border-collapse:collapse;width:800px;height:30px" align="center" border="1.0" id="taskexecutors%s" class="display">
										              <thead>
										              <th>User</th>
										              <th>Executor</th>
										              <th>Node</th>
										              <th>Executor<BR/>Address</th>
										              <th>Cpu</th>
										              <th>Memory (MB)</th>
										              <th>Status<BR/>(UP/DOWN)</th>
										              </thead>
										              <tbody>""".formatted(
								aint.get(), aint.get(), aint.getAndIncrement()));
						summaryTes(user, jobid, usercontainersmap.get(jobid), builder);
						builder.append("</tbody></table>");
						builder.append("<br/>");
					});
				});
			}

			if (DataSamudayaJobMetrics.get().keySet().size() > 0) {
				int i = 0;
				builder.append("<br/>");
				builder.append(
						"""
										<table style=\"color:#000000;border-collapse:collapse;width:800px;height:30px\" align=\"center\" border=\"1.0\" id="metrics" class="display">
								<thead>
								<th>Job<Br/>Id</th>
								<th>Job<Br/>Name</th>
								<th>SQL/PIG<BR/>Query</th>
								<th>Files<Br/>Used</th>
								<th>Job<Br/>Mode</th>
								<th>Total<Br/>Files<Br/>Size (MB)</th>
								<th>Total<Br/>Files<Br/>Blocks</th>
								<th>Container<Br/>Resources</th>
								<th>Job<Br/>Status</th>
								<th>Nodes</th>
								<th>Job<Br/>Start<Br/>Time</th>
								<th>Job<Br/>Completion<Br/>Time</th>
								<th>Total<Br/>Time<Br/>Taken (Sec)</th>
								<th>Stage Graph</th>
								<th>Task Graph</th>
								<th>Summary</th>
								</thead>
								<tbody>""");
				var jms = DataSamudayaJobMetrics.get();
				var jobmetrics = jms.keySet().stream().map(key -> jms.get(key)).sorted((jm1, jm2) -> {
					return (int) (jm2.getJobstarttime() - jm1.getJobstarttime());
				}).collect(Collectors.toList());
				for (var jm : jobmetrics) {
					builder.append("<tr bgcolor=\"").append(Utils.getColor(i++)).append("\">");
					builder.append("<td>");
					builder.append(jm.getJobid());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(Objects.isNull(jm.getJobname()) ? DataSamudayaConstants.EMPTY : jm.getJobname());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(
							Objects.isNull(jm.getSqlpigquery()) ? DataSamudayaConstants.EMPTY : jm.getSqlpigquery());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(toHtml(jm.getFiles()));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.getMode());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.getTotalfilesize());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.getTotalblocks());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.getContainerresources());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(toHtml(jm.getContainersallocated()));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(!Objects.isNull(jm.getNodes()) ? toHtml(new ArrayList<>(jm.getNodes())) : "");
					builder.append("</td>");
					builder.append("<td>");
					builder.append(new Date(jm.getJobstarttime()));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.getJobcompletiontime() == 0 ? "" : new Date(jm.getJobcompletiontime()));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.getTotaltimetaken() == 0 ? "" : jm.getTotaltimetaken());
					builder.append("</td>");

					builder.append("<td>");
					builder.append("<a href=\"" + contextpath + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.GRAPH + "?jobId=" + jm.getJobid() + "&graph=stage" + "\">"
							+ jm.getJobid() + "</a>");
					builder.append("</td>");

					builder.append("<td>");
					builder.append("<a href=\"" + contextpath + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.GRAPH + "?jobId=" + jm.getJobid() + "&graph=task" + "\">"
							+ jm.getJobid() + "</a>");
					builder.append("</td>");

					builder.append("<td>");
					builder.append("<a href=\"" + contextpath + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.SUMMARY + "?jobId=" + jm.getJobid() + "\">SUMMARY</a>");
					builder.append("</td>");
					builder.append("</tr>");
				}
				builder.append("</tbody></table>");

			}
			builder.append("</body></html>");
			writer.write(builder.toString());
		} catch (Exception ex) {
			log.debug("TaskScheduler Web servlet error, See cause below \n", ex);
		}
	}

	/**
	 * Summary of Executors per user per executors id
	 * 
	 * @param user
	 * @param execid
	 * @param lcs
	 * @param buffer
	 */
	private void summaryTes(String user, String execid, List<LaunchContainers> lcs, StringBuilder buffer) {
		for (LaunchContainers lc : lcs) {
			List<ContainerResources> crsalloc = lc.getCla().getCr();
			for (ContainerResources crs : crsalloc) {
				buffer.append("<tr>");
				buffer.append("<td>");
				buffer.append(user);
				buffer.append("</td>");
				buffer.append("<td>");
				buffer.append(execid);
				buffer.append("</td>");
				buffer.append("<td>");
				buffer.append(lc.getNodehostport());
				buffer.append("</td>");
				String te = lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE)[0]
						+ DataSamudayaConstants.UNDERSCORE + crs.getPort();
				buffer.append("<td>");
				buffer.append(te);
				buffer.append("</td>");
				buffer.append("<td>");
				buffer.append(crs.getCpu());
				buffer.append("</td>");
				buffer.append("<td>");
				buffer.append((crs.getMaxmemory() + crs.getDirectheap()) / DataSamudayaConstants.MB);
				buffer.append("</td>");
				buffer.append("<td>");
				buffer.append(getTaskExecutorStatus(lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE)[0], crs.getPort()));
				buffer.append("</td>");
				buffer.append("</tr>");
			}

		}
	}

	/**
	 * Return status of task executor whether it is UP or DOWN
	 * @param hp
	 * @param port
	 * @return status of task executor as UP/DOWN
	 */
	protected String getTaskExecutorStatus(String hp, int port) {
		try (Socket sock = new Socket()) {
			sock.connect(new InetSocketAddress(hp, port));
			return "<font color=\"green\">" + DataSamudayaConstants.TASKEXECUTOR_STATUS_UP + "</font>";
		} catch (Exception ex) {
			return "<font color=\"red\">" + DataSamudayaConstants.TASKEXECUTOR_STATUS_DOWN + "</font>";
		}
	}


	/**
	 * The method converts the data object in the form of list or map to html
	 * 
	 * @param data
	 * @return
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	private String toHtml(Object data) {
		StringBuilder builder = new StringBuilder();
		builder.append("<p>");
		if (!Objects.isNull(data)) {
			if (data instanceof List files) {
				for (String file : (List<String>) files) {
					builder.append(file);
					builder.append("<br/>");
				}
			} else if (data instanceof Map map) {
				map.keySet().stream().forEach(key -> {
					builder.append(key);
					builder.append(DataSamudayaConstants.COLON);
					builder.append("Percentage Completed (");
					builder.append(map.get(key));
					builder.append("%)");
					builder.append("<br/>");
				});
			}
		}
		builder.append("</p>");
		return builder.toString();
	}

}
