package com.github.datasamudaya.common;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.datasamudaya.common.utils.Utils;

/**
 * @author arun
 * The class is a Servlet to destroy Task Executor
 */
public class TaskExecutorDestroyServlet extends HttpServlet {
	private static final long serialVersionUID = 2889522326590300697L;
	private static final Logger log = LogManager.getLogger(TaskExecutorDestroyServlet.class);
	private final ObjectMapper mapper = new ObjectMapper();
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		response.setContentType(DataSamudayaConstants.TEXTHTML);
		response.setStatus(HttpServletResponse.SC_OK);
		var writer = response.getWriter();		
		String user=request.getParameter("user");
		String eid=request.getParameter("eid");
		String nodehp=request.getParameter("node");
		String executorhp =request.getParameter("executor");
		try {
			Utils.destroyContainer(user, eid, nodehp, executorhp);
			log.info("Container Destroyed "+executorhp);
			writer.write(mapper.writeValueAsString(new TaskExecutorKillResponse(user, eid, nodehp, executorhp, "Success", DataSamudayaConstants.EMPTY)));
		} catch(Exception ex) {
			log.error("Container "+executorhp+" Not Destroyed ",ex);
			writer.write(mapper.writeValueAsString(new TaskExecutorKillResponse(user, eid, nodehp, executorhp, "Failure", ex.getMessage())));
		} finally {
			writer.flush();
			writer.close();
		}
	}
}
