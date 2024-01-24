package com.github.datasamudaya.stream.pig;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
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
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.stream.CsvOptionsSQL;
import com.github.datasamudaya.stream.StreamPipeline;

/**
 * Pig Query Executor
 * 
 * @author arun
 *
 */
public class PigQueryExecutor {

	private static Logger log = LoggerFactory.getLogger(PigQueryExecutor.class);
	
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
	public static void executePlan(LogicalPlan logicalPlan, boolean isstore, String alias, String user, String jobid, String tejobid,
			PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setContaineralloc(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE);
		pipelineconfig.setUseglobaltaskexecutors(true);
		pipelineconfig.setUser(user);
		pipelineconfig.setTejobid(tejobid);
		pipelineconfig.setJobid(jobid);
		Set<String> requiredcolumns = new LinkedHashSet<>();
		Set<String> allcolumns = new LinkedHashSet<>();
		List<String> aliasjoin = new ArrayList<>();
		if(isstore) {
			traversePlan(logicalPlan, isstore, alias, user, jobid, tejobid, pipelineconfig, requiredcolumns, allcolumns, aliasjoin, alias);
			return;
		}
		PigUtils.executeDump(traversePlan(logicalPlan, isstore, alias, user, jobid, tejobid, pipelineconfig, requiredcolumns, allcolumns, aliasjoin, alias), user, jobid, tejobid, pipelineconfig);
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
	public static StreamPipeline<?> traversePlan(OperatorPlan plan, boolean isstore, String alias, String user, String jobid,
			String tejobid, PipelineConfig pipelineconfig,
			Set<String> requiredcolumns,
			Set<String> allcolumns,
			List<String> aliasjoin,
			String termalias) throws Exception {
		Operator operatortoexec = findLatestAssignment(plan, isstore, alias);
		List<Operator> operatorstoexec = new ArrayList<>();
		if(operatortoexec instanceof LOStore lostore) {
			traverseOperator(operatortoexec, 0, operatorstoexec);
		}
		else if(operatortoexec instanceof LOJoin lojoin) {
			operatorstoexec.add(operatortoexec);
		}
		else if (operatortoexec instanceof LogicalRelationalOperator lro) {
			if (lro.getAlias().equalsIgnoreCase(alias)) {
				traverseOperator(operatortoexec, 0, operatorstoexec);
			} else {
				List<Operator> operatorspred = operatortoexec.getPlan().getPredecessors(operatortoexec);
				for (Operator predoper : operatorspred) {
					if (predoper instanceof LogicalRelationalOperator lropred) {
						if (lropred.getAlias().equalsIgnoreCase(alias)) {
							traverseOperator(predoper, 0, operatorstoexec);
						}
					}
				}
			}
		} else {
			traversePlan(operatortoexec.getPlan(), isstore, alias, user, jobid, tejobid, pipelineconfig, requiredcolumns, allcolumns, aliasjoin, termalias);
		}

		List<Operator> operatorstoobtainschemas = new ArrayList<>(operatorstoexec); 

		final List<String> columns;
		if(!(operatortoexec instanceof LOJoin)) {
			Operator operatorloload = operatorstoobtainschemas.remove(0);
			extractRequiredColumns(Arrays.asList(operatorloload), allcolumns, null);
			extractRequiredColumns(operatorstoobtainschemas, requiredcolumns, allcolumns);
			if(CollectionUtils.isEmpty(operatorstoobtainschemas)) {
				requiredcolumns.addAll(allcolumns);
			}
			columns = new ArrayList<>();
			columns.addAll(allcolumns);			
		} else {
			columns = new ArrayList<>();
			allcolumns = new LinkedHashSet<>();
		}
		requiredcolumns = requiredcolumns.stream().map(col->columns.indexOf(col)+DataSamudayaConstants.EMPTY).collect(Collectors.toCollection(LinkedHashSet::new));
		return executeOperators(operatorstoexec, requiredcolumns, new ArrayList<>(), new ArrayList<>(allcolumns), (LogicalPlan) plan, user, jobid, tejobid, pipelineconfig, aliasjoin, termalias);		
	}
	
	
	/**
	 * Obtain only the required columns from schema
	 * @param operators
	 * @param requiredColumns
	 * @throws ExecException
	 * @throws Exception 
	 */
	private static void extractRequiredColumns(List<Operator> operators, Set<String> requiredColumns, 
			Set<String> allcolumns) throws ExecException, Exception {
		boolean isforeach = false;
		if (nonNull(operators)) {
			for (Operator successor : operators) {
				if (successor instanceof LOForEach) {
					isforeach = true;
					break;
				}
			}
		}
		Set<String> rcco = new LinkedHashSet<>();
		if(nonNull(allcolumns)) {
			rcco.addAll(allcolumns);
		}
        // Recursively traverse the current operator
        for (int index=0;index<operators.size();index++) {
        	Operator operator = operators.get(index);
        	if (operator instanceof LOJoin) {
                // Handle LOJoin specific logic
                for (Operator predecessor : operator.getPlan().getPredecessors(operator)) {
                    extractRequiredColumns(Arrays.asList(predecessor), rcco, allcolumns);
                }
            } else if (operator instanceof LOLoad loadOperator) {
                // Handle LOLoad specific logic
                requiredColumns.addAll(getColumnsFromSchemaFields(loadOperator.getSchema().getFields(), allcolumns));
            } else if (operator instanceof LOFilter loFilter) {
                // Handle LOFilter or LOForEach specific logic
            	if(isforeach) {
	            	LogicalExpressionPlan lep = loFilter.getFilterPlan();
	        		List<Operator> exp = lep.getSources();
	        		List<String> columns = new ArrayList<>();
	        		PigUtils.getColumnsFromExpressions((LogicalExpression)exp.get(0), columns);
	        		columns.retainAll(allcolumns);
    				requiredColumns.addAll(columns);
    				rcco.clear();
	            	rcco.addAll(requiredColumns);
            	} else {
	            	LogicalRelationalOperator relationalOperator = (LogicalRelationalOperator) operator;
	            	requiredColumns.addAll(getColumnsFromSchemaFields(relationalOperator.getSchema().getFields(), rcco));
	            	rcco.clear();
	            	rcco.addAll(requiredColumns);
            	}
            } else if(operator instanceof LOForEach loForEach) {
            	List<FunctionParams> functionparams = PigUtils.getFunctionsWithParamsGrpBy(loForEach);
        		LogicalExpression[] lexp = PigUtils.getLogicalExpressions(functionparams);
        		if(nonNull(lexp)) {
        			List<String> columns = new ArrayList<>();
        			Set<String> currentrcco = new LinkedHashSet<>();
        			for(LogicalExpression lex:lexp) {
        				PigUtils.getColumnsFromExpressions(lex, columns);
        				columns.retainAll(allcolumns);        				
        				requiredColumns.addAll(columns);
        				currentrcco.addAll(columns);
        				columns.clear();
        			}
    				rcco.clear();
    				rcco.addAll(currentrcco);
        		}
            } else if (operator instanceof LOCogroup cogroupOperator) {
                // Handle LOCogroup specific logic
                requiredColumns.addAll(getColumnsFromSchemaFields(cogroupOperator.getSchema().getFields(), rcco));
                rcco.clear();
                rcco.addAll(requiredColumns);
            } else if (operator instanceof LOSort sortOperator) {
                // Handle LOSort specific logic
                requiredColumns.addAll(getColumnsFromSchemaFields(sortOperator.getSchema().getFields(), rcco));
                rcco.clear();
                rcco.addAll(requiredColumns);
            } else if (operator instanceof LODistinct distinctOperator) {
                // Handle LODistinct specific logic
                requiredColumns.addAll(getColumnsFromSchemaFields(distinctOperator.getSchema().getFields(), rcco));
                rcco.clear();
                rcco.addAll(requiredColumns);
            }
        }
    }

	/**
	 * Get Columns from logical schema fields.
	 * @param schemafields
	 * @return set of schema field names.
	 */
	private static Set<String> getColumnsFromSchemaFields(List<LogicalFieldSchema> schemafields, Set<String> allcolumns){
		if(isNull(allcolumns)) {
			return schemafields.stream().map(field->field.alias).collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return schemafields.stream().filter(field->allcolumns.contains(field.alias)).map(field->field.alias).collect(Collectors.toCollection(LinkedHashSet::new));
	}
	
	
	/**
	 * The function returns the latest assignment for the given operator plan and alias
	 * @param plan
	 * @param alias
	 * @return latest assignment operator plan
	 */
	private static Operator findLatestAssignment(OperatorPlan plan, boolean isstore, String alias) {
        Iterator<Operator> operators = plan.getOperators();
        Operator latestoperatorforalias = null;
        for (;operators.hasNext();) {
            Operator operator = operators.next();
            if(isstore && operator instanceof LOStore) {
            	latestoperatorforalias = operator;
            }
            else if (operator instanceof LogicalRelationalOperator lro) {
                if (lro.getAlias().equals(alias)) {
                    // Found the latest assignment
                	latestoperatorforalias = operator;
                }
            }
        }
        return latestoperatorforalias; // Alias not found
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

	List<String> outcols = new ArrayList<>();
	
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
	private static StreamPipeline<?> executeOperators(List<Operator> operatorstoexec, 
			Set<String> requiredcolumns, 
			List<String> outcols,
			List<String> allcols,
			LogicalPlan plan, String user,
			String jobid, 
			String tejobid, 
			PipelineConfig pipelineconfig,
			List<String> alias,
			String termalias) throws Exception {
		StreamPipeline<?> sp = null;
		for (int numoper=0;numoper<operatorstoexec.size();numoper++) {
			Operator operator = operatorstoexec.get(numoper);			
			if (operator instanceof LOLoad loload) {				
				sp = PigUtils.executeLOLoad(user, jobid, tejobid, loload, pipelineconfig);
				CsvOptionsSQL csvoptsql = ((CsvOptionsSQL) sp.getCsvOptions());
				csvoptsql.setRequiredcolumns(new ArrayList<>(requiredcolumns));
				requiredcolumns.clear();
				requiredcolumns.addAll(allcols);
				if(numoper == operatorstoexec.size()-1) {
					alias.addAll(allcols);
				}
				sp = sp.map(new MapFunction() {

					@Override
					public Object apply(Object t) {
						return t;
					}
				});
			} else if (operator instanceof LOFilter loFilter) {
				String operalias = loFilter.getAlias();
				sp = PigUtils.executeLOFilter((StreamPipeline<Object[]>) sp, loFilter, new ArrayList<>(requiredcolumns), outcols, !termalias.equals(operalias));
				requiredcolumns.clear();
				requiredcolumns.addAll(outcols);
				outcols.clear();
				if(numoper == operatorstoexec.size()-1) {
					PigUtils.getAliaseForJoin(operator, alias);
				}
			} else if (operator instanceof LOStore lostore) {
				PigUtils.executeLOStore(sp, lostore);
			} else if (operator instanceof LOCogroup loCogroup) {
				String operalias = loCogroup.getAlias();
				sp = PigUtils.executeLOCoGroup((StreamPipeline<Object[]>) sp, loCogroup, new ArrayList<>(requiredcolumns), outcols, !termalias.equals(operalias));
				requiredcolumns.clear();
				requiredcolumns.addAll(outcols);
				outcols.clear();
				if(numoper == operatorstoexec.size()-1) {
					PigUtils.getAliaseForJoin(operator, alias);
				}
			} else if (operator instanceof LOForEach loForEach) {
				String operalias = loForEach.getAlias();
				sp = PigUtils.executeLOForEach((StreamPipeline<Object[]>) sp, loForEach, new ArrayList<>(requiredcolumns), outcols, !termalias.equals(operalias));
				requiredcolumns.clear();
				requiredcolumns.addAll(outcols);
				outcols.clear();
				if(numoper == operatorstoexec.size()-1) {
					PigUtils.getAliaseForJoin(operator, alias);
				}
			} else if (operator instanceof LOSort loSort) {
				String operalias = loSort.getAlias();
				sp = PigUtils.executeLOSort((StreamPipeline<Object[]>) sp, loSort, new ArrayList<>(requiredcolumns), outcols, !termalias.equals(operalias));
				requiredcolumns.clear();
				requiredcolumns.addAll(outcols);
				outcols.clear();
				if(numoper == operatorstoexec.size()-1) {
					PigUtils.getAliaseForJoin(operator, alias);
				}
			} else if (operator instanceof LODistinct loDistinct) {
				sp = PigUtils.executeLODistinct((StreamPipeline<Object[]>) sp);
				if(numoper == operatorstoexec.size()-1) {
					PigUtils.getAliaseForJoin(operator, alias);
				}
			} else if (operator instanceof LOJoin loJoin) {
				String operalias = loJoin.getAlias();
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
				Set<String> requiredcolumnsleft = new LinkedHashSet<>();
				Set<String> allcolumnsleft = new LinkedHashSet<>();
				Set<String> requiredcolumnsright = new LinkedHashSet<>();
				Set<String> allcolumnsright = new LinkedHashSet<>();
				List<String> aliasleft = new ArrayList<>();
				List<String> aliasright = new ArrayList<>();
				sp = PigUtils.executeLOJoin(
						(StreamPipeline<Object[]>) traversePlan(operator.getPlan(), false, expjoinalias.get(0),
								user, jobid, tejobid, pipelineconfig, requiredcolumnsleft, allcolumnsleft, aliasleft, termalias),
						(StreamPipeline<Object[]>) traversePlan(operator.getPlan(), false, expjoinalias.get(1),
								user, jobid, tejobid, pipelineconfig, requiredcolumnsright, allcolumnsright, aliasright, termalias),
						joincolumns.get(0), joincolumns.get(1), loJoin, new ArrayList<>(requiredcolumnsleft), new ArrayList<>(allcolumnsleft), 
						new ArrayList<>(requiredcolumnsright), new ArrayList<>(allcolumnsright), aliasleft, aliasright, !termalias.equals(operalias));
			}
		}
		return sp;
	}

	private PigQueryExecutor() {
	}

}
