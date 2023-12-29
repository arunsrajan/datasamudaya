package com.github.datasamudaya.stream.pig;

import static java.util.Objects.nonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
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

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.StreamPipeline;

/**
 * Pig Query Executor
 * 
 * @author arun
 *
 */
public class PigQueryExecutor {

	/**
	 * Executes Pig Command
	 * 
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
	public static void executePlan(LogicalPlan logicalPlan, String alias, String user, String jobid, String tejobid,
			PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setContaineralloc(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE);
		pipelineconfig.setUseglobaltaskexecutors(true);
		pipelineconfig.setUser(user);
		pipelineconfig.setTejobid(tejobid);
		pipelineconfig.setJobid(jobid);
		PigUtils.executeDump(traversePlan(logicalPlan, alias, user, jobid, tejobid, pipelineconfig), user, jobid, tejobid, pipelineconfig); ;
	}

	/**
	 * The function which traverses to the plan and returns pipeline object.
	 * @param plan
	 * @param alias
	 * @param user
	 * @param jobid
	 * @param tejobid
	 * @param pipelineconfig
	 * @return pipeline object
	 * @throws Exception
	 */
	public static StreamPipeline<?> traversePlan(OperatorPlan plan, String alias, String user, String jobid,
			String tejobid, PipelineConfig pipelineconfig) throws Exception {
		List<Operator> operators = plan.getSinks();
		List<Operator> operatorstoexec = new ArrayList<>();
		for (Operator operator : operators) {
			if (operator instanceof LogicalRelationalOperator lro) {
				if (lro.getAlias().equalsIgnoreCase(alias)) {
					traverseOperator(operator, 0, operatorstoexec);
				} else {
					List<Operator> operatorspred = operator.getPlan().getPredecessors(operator);
					for(Operator predoper:operatorspred) {
						if (predoper instanceof LogicalRelationalOperator lropred) {
							if (lropred.getAlias().equalsIgnoreCase(alias)) {
								traverseOperator(predoper, 0, operatorstoexec);
							}
						}
					}
				}
			} else {
				traversePlan(operator.getPlan(), alias, user, jobid, tejobid, pipelineconfig);
			}
		}
		return executeOperators(operatorstoexec, (LogicalPlan)plan, user, jobid, tejobid, pipelineconfig);
	}

	/**
	 * The function traverses the operator
	 * @param operator
	 * @param depth
	 * @param operatorstoexec
	 * @throws ExecException
	 */
	private static void traverseOperator(Operator operator, int depth, List<Operator> operatorstoexec)
			throws ExecException {
		List<Operator> operators = operator.getPlan().getPredecessors(operator);
		// Recursively traverse the successors of the current operator
		operatorstoexec.add(0, operator);
		if (nonNull(operators)) {
			for (Operator predecessor : operators) {				
				traverseOperator(predecessor, depth + 1, operatorstoexec);
			}
		}
	}

	/**
	 * The function returns pipeline object for operators in order.
	 * @param operatorstoexec
	 * @param plan
	 * @param user
	 * @param jobid
	 * @param tejobid
	 * @param pipelineconfig
	 * @return pipeline object
	 * @throws Exception
	 */
	private static StreamPipeline<?> executeOperators(List<Operator> operatorstoexec, LogicalPlan plan, String user,
			String jobid, String tejobid, PipelineConfig pipelineconfig) throws Exception {
		StreamPipeline<?> sp = null;
		for (Operator operator : operatorstoexec) {
			if (operator instanceof LOLoad loload) {
				sp = PigUtils.executeLOLoad(user, jobid, tejobid, loload, pipelineconfig);
			} else if (operator instanceof LOFilter loFilter) {
				sp = PigUtils.executeLOFilter((StreamPipeline<Map<String, Object>>) sp, loFilter);
			} else if (operator instanceof LOStore lostore) {
				PigUtils.executeLOStore(sp, lostore);
			} else if (operator instanceof LOCogroup loCogroup) {
				sp = PigUtils.executeLOCoGroup((StreamPipeline<Map<String, Object>>) sp, loCogroup);
			} else if (operator instanceof LOForEach loForEach) {
				sp = PigUtils.executeLOForEach((StreamPipeline<Map<String, Object>>) sp, loForEach);
			} else if (operator instanceof LOSort loSort) {
				sp = PigUtils.executeLOSort((StreamPipeline<Map<String, Object>>) sp, loSort);
			} else if (operator instanceof LODistinct loDistinct) {
				sp = PigUtils.executeLODistinct((StreamPipeline<Map<String, Object>>) sp);
			} else if (operator instanceof LOJoin loJoin) {
				List<Operator> operators = loJoin.getInputs(plan);
				List<String> expjoinalias = new ArrayList<>();
				for (Operator input : operators) {
					LogicalRelationalOperator inputOp = (LogicalRelationalOperator) input;
					expjoinalias.add(inputOp.getAlias());
				}
				int noofjoinexp = expjoinalias.size();
				MultiMap<Integer, LogicalExpressionPlan> expplans = loJoin.getExpressionPlans();
				List<List<String>> joincolumns = new ArrayList<>();
				for (int mapindex = 0; mapindex < noofjoinexp; mapindex++) {
					List<LogicalExpressionPlan> leps = expplans.get(mapindex);
					List<String> columnstojoin = new ArrayList<>();
					joincolumns.add(columnstojoin);
					for (LogicalExpressionPlan lep : leps) {
						columnstojoin.add(((ProjectExpression) lep.getOperators().next()).getColAlias());
					}
				}
				sp = PigUtils.executeLOJoin(
						(StreamPipeline<Map<String, Object>>) traversePlan(operator.getPlan(), expjoinalias.get(0),
								user, jobid, tejobid, pipelineconfig),
						(StreamPipeline<Map<String, Object>>) traversePlan(operator.getPlan(), expjoinalias.get(1),
								user, jobid, tejobid, pipelineconfig),
						joincolumns.get(0), joincolumns.get(1), loJoin);
			}
		}
		return sp;
	}

	private PigQueryExecutor() {
	}

}
