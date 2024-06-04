package com.github.datasamudaya.common;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Date;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.github.datasamudaya.common.utils.Utils;

public class SummaryWebServlet extends HttpServlet {
	private static final long serialVersionUID = -1208090049509225305L;
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
		String jobid = request.getParameter(DataSamudayaConstants.JOBID);
		String contextpath = request.getScheme() + "://" + request.getServerName() + DataSamudayaConstants.COLON
				+ request.getLocalPort();
		try {
			var jm = DataSamudayaJobMetrics.get();
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
					</head><body>""".formatted(contextpath, contextpath, contextpath, contextpath, contextpath, contextpath, contextpath, contextpath, contextpath, contextpath));
			builder.append(summary(jm.get(jobid)));
			builder.append("""
					</body>
					</html>
					""");
			writer.write(builder.toString());
			writer.flush();
		} catch (Exception ex) {
			log.debug("Summary Web servlet error, See cause below \n", ex);
		}
	}

	/**
		* Summary of tasks information 
		* @param jm
		* @return Tasks metrics information in HTML format.
		*/
		private String summary(JobMetrics jm) {
		SimpleDateFormat formatstartenddate = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
		StringBuilder tasksummary = new StringBuilder();
		tasksummary.append("<p>");
		var aint = new AtomicInteger(0);
		if (!CollectionUtils.isEmpty(jm.getTaskexcutortasks())) {
			jm.getTaskexcutortasks().entrySet().stream().forEachOrdered(entry -> {
				tasksummary.append("<H1 align=\"center\">");
				tasksummary.append(entry.getKey());
				tasksummary.append(":");
				tasksummary.append("</H1>");
				tasksummary.append("<BR/>");
				tasksummary.append("""
	        			<script language="Javascript" type="text/javascript">
			      		$(document).ready(function(){
			      			var res%s = $('#summary%s').DataTable({
							  			    dom: 'Bflrtip',
							    buttons: [
							        'copy', 'excel', 'pdf'
							    ]
							});							
			      		});
			          </script>
	                    <table style="color:#000000;border-collapse:collapse;width:800px;height:30px" align="center" border="1.0" id="summary%s">
	                    <thead>
	                    <th>Task<Br/>Id</th>
	                    <th>Task<Br/>Start</th>
	                    <th>Task<BR/>End</th>
	                    <th>Time<Br/>Taken</th>
	                    <th>Block<Br/>Size (MB)</th>
	                    <th>Task<Br/>Status</th>
	                    <th>Generated<Br/>Output<BR/>Size</th>
	                    <th>Converted<Br/>Input<BR/>Size</th>
	                    </thead>
	                    <tbody>""".formatted(aint.get(), aint.get(), aint.getAndIncrement()));
				double totaltimetakenexecutor = 0d;
				double totalmbprocessed = 0d;
				double blocksinmb = 0d;
				int i = 0;
				for (Task task : entry.getValue()) {
					tasksummary.append("<tr bgcolor=\"").append(Utils.getColor(i++)).append("\">");
					tasksummary.append("<td>");
					tasksummary.append(task.taskid);
					tasksummary.append("</td>");
					tasksummary.append("<td>");
					tasksummary.append(formatstartenddate.format(new Date(task.taskexecutionstartime)));
					tasksummary.append("</td>");
					tasksummary.append("<td>");
					tasksummary.append(formatstartenddate.format(new Date(task.taskexecutionendtime)));
					tasksummary.append("</td>");
					tasksummary.append("<td>");
					tasksummary.append(task.timetakenseconds);
					tasksummary.append("</td>");
					blocksinmb = task.numbytesprocessed / DataSamudayaConstants.MB;
					totalmbprocessed += blocksinmb;
					tasksummary.append("<td>");
					tasksummary.append(blocksinmb);
					tasksummary.append("</td>");
					tasksummary.append("<td>");
					tasksummary.append(task.taskstatus);
					tasksummary.append("</td>");
					tasksummary.append("<td>");
					tasksummary.append(task.numbytesgenerated);
					tasksummary.append("</td>");
					tasksummary.append("<td>");
					tasksummary.append(task.numbytesconverted);
					tasksummary.append("</td>");
					tasksummary.append("</tr>");
					totaltimetakenexecutor += task.timetakenseconds;
				}
				tasksummary.append("</tbody></table>");
				tasksummary.append("<H3 align=\"center\">");
				tasksummary.append("<BR/>");
				tasksummary.append("Total Block Size:").append(totalmbprocessed);
				tasksummary.append("<BR/>");
				tasksummary.append("Average Time Per Task:" + (totaltimetakenexecutor / entry.getValue().size()));
				tasksummary.append("</H3>");
				tasksummary.append("<BR/>");
			});
		}
		tasksummary.append("</p>");
		return tasksummary.toString();
	}
}
