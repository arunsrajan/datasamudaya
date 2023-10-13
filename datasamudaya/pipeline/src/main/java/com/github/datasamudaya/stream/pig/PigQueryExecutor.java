package com.github.datasamudaya.stream.pig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.parser.PigParserNode;
import org.apache.pig.parser.QueryParserDriver;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.StreamPipeline;

import static java.util.Objects.*;

/**
 * Pig Query Executor
 * @author arun
 *
 */
public class PigQueryExecutor {

	/**
	 * Executes Pig Command
	 * @param pigAliasExecutedObjectMap
	 * @param queryParserDriver
	 * @param pigQueries
	 * @param user
	 * @param jobid
	 * @param tejobid
	 * @param pipelineconfig
	 * @return
	 * @throws Exception
	 */
	public static Object execute(Map<String, Object> pigAliasExecutedObjectMap, QueryParserDriver queryParserDriver,
			List<String> pigQueries, String user, String jobid, String tejobid, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setContaineralloc(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE);
		pipelineconfig.setUseglobaltaskexecutors(true);		
		pipelineconfig.setUser(user);
		pipelineconfig.setTejobid(tejobid);
		pipelineconfig.setJobid(jobid);
		StringBuilder pigCommands = new StringBuilder();
		pigQueries.forEach(pigCommands::append);
		LogicalPlan logicalPlan = PigUtils.getLogicalPlan(pigCommands.toString(), queryParserDriver);
		Iterator<Operator> lofilteroperator = logicalPlan.getOperators();
		Operator operator = null;
		while (lofilteroperator.hasNext()) {
			operator = lofilteroperator.next();
		}
		if (operator instanceof LOLoad loload) {
			pigAliasExecutedObjectMap.put(loload.getAlias(), PigUtils.executeLOLoad(user, jobid, tejobid, loload, pipelineconfig));
		} else if (operator instanceof LOFilter loFilter) {
			PigParserNode node = (PigParserNode) loFilter.getLocation().node().getChildren().get(0);
			if(isNull(pigAliasExecutedObjectMap
					.get(node.getText()))) {
				throw new PigException(String.format(PigException.NOALIASFOUND, node.getText()));
			}
			pigAliasExecutedObjectMap.put(loFilter.getAlias(),PigUtils.executeLOFilter((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap
					.get(node.getText()), loFilter));
		} else if (operator instanceof LOStore lostore) {
			PigParserNode node = (PigParserNode) lostore.getLocation().node().getChildren().get(0);
			if(isNull(pigAliasExecutedObjectMap
					.get(node.getText()))) {
				throw new PigException(String.format(PigException.NOALIASFOUND, node.getText()));
			}
			PigUtils.executeLOStore((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap
					.get(node.getText()), lostore);
		} else if (operator instanceof LOCogroup loCogroup) {
			PigParserNode node = (PigParserNode) loCogroup.getLocation().node().getChildren().get(0);
			if(isNull(pigAliasExecutedObjectMap
					.get(node.getText()))) {
				throw new PigException(String.format(PigException.NOALIASFOUND, node.getText()));
			}
			pigAliasExecutedObjectMap.put(loCogroup.getAlias(),
					PigUtils.executeLOCoGroup(
							(StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get(node.getText()),
							loCogroup));
		} else if (operator instanceof LOForEach loForEach) {
			PigParserNode node = (PigParserNode) loForEach.getLocation().node().getChildren().get(0);
			if(isNull(pigAliasExecutedObjectMap
					.get(node.getText()))) {
				throw new PigException(String.format(PigException.NOALIASFOUND, node.getText()));
			}
			pigAliasExecutedObjectMap.put(loForEach.getAlias(), PigUtils.executeLOForEach(
					(StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get(node.getText()),
					loForEach));
		} else if (operator instanceof LOSort loSort) {
			PigParserNode node = (PigParserNode) loSort.getLocation().node().getChildren().get(0);
			if(isNull(pigAliasExecutedObjectMap
					.get(node.getText()))) {
				throw new PigException(String.format(PigException.NOALIASFOUND, node.getText()));
			}
			pigAliasExecutedObjectMap.put(loSort.getAlias(), PigUtils.executeLOSort(
					(StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get(node.getText()),
					loSort));
		} else if (operator instanceof LODistinct loDistinct) {
			PigParserNode node = (PigParserNode) loDistinct.getLocation().node().getChildren().get(0);
			if(isNull(pigAliasExecutedObjectMap
					.get(node.getText()))) {
				throw new PigException(String.format(PigException.NOALIASFOUND, node.getText()));
			}
			pigAliasExecutedObjectMap.put(loDistinct.getAlias(), PigUtils.executeLODistinct(
					(StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get(node.getText())));
		} else if (operator instanceof LOJoin loJoin) {
			List<Operator> operators = loJoin.getInputs(logicalPlan);
			List<String> expjoinalias = new ArrayList<>(); 
			for (Operator input : operators) {
                LogicalRelationalOperator inputOp = (LogicalRelationalOperator) input;
                expjoinalias.add(inputOp.getAlias());
            }
			int noofjoinexp = expjoinalias.size();
			MultiMap<Integer, LogicalExpressionPlan> expplans = loJoin.getExpressionPlans();
			List<List<String>> joincolumns = new ArrayList<>();
			for(int mapindex=0;mapindex<noofjoinexp; mapindex++) {
				List<LogicalExpressionPlan> leps = expplans.get(mapindex);
				List<String> columnstojoin = new ArrayList<>();
				joincolumns.add(columnstojoin);
				for(LogicalExpressionPlan lep:leps) {
					columnstojoin.add(((ProjectExpression)lep.getOperators().next()).getColAlias());
				}
			}
			if(isNull(pigAliasExecutedObjectMap
					.get(expjoinalias.get(0)))) {
				throw new PigException(String.format(PigException.NOALIASFOUND, expjoinalias.get(0)));
			}
			if(isNull(pigAliasExecutedObjectMap
					.get(expjoinalias.get(1)))) {
				throw new PigException(String.format(PigException.NOALIASFOUND, expjoinalias.get(1)));
			}
			pigAliasExecutedObjectMap.put(loJoin.getAlias(), PigUtils.executeLOJoin(
					(StreamPipeline<Map<String, Object>>)pigAliasExecutedObjectMap.get(expjoinalias.get(0)),
					(StreamPipeline<Map<String, Object>>)pigAliasExecutedObjectMap.get(expjoinalias.get(1)),
					joincolumns.get(0),
					joincolumns.get(1),
					loJoin));
		}
		return "";
	}

}
