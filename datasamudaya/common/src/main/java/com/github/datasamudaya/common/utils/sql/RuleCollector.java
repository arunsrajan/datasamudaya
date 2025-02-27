package com.github.datasamudaya.common.utils.sql;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class collects all the rules applicable to a given RelNode
 * 
 * @author arun
 */
public class RuleCollector {
	private static final Logger log = LoggerFactory.getLogger(RuleCollector.class);
	static Set<RelOptRule> ruleset = new HashSet<>();
	static {
		try {
			Set<String> reloptrulestr = new LinkedHashSet<>();
			Field[] fields = CoreRules.class.getDeclaredFields();
			for (Field field : fields) {
				Object rule = field.get(null);
				if (rule instanceof RelOptRule rulerelopt && !reloptrulestr.contains(rulerelopt.toString())) {
					ruleset.add(rulerelopt);
					reloptrulestr.add(rulerelopt.toString());
				}
			}
			List<RelOptRule> enumrules = new ArrayList<>(EnumerableRules.rules());
			enumrules.remove(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
			ruleset.addAll(enumrules);
			ruleset.remove(JoinProjectTransposeRule.Config.DEFAULT.toRule());
			ruleset.remove(CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE);
			ruleset.remove(CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER);
			ruleset.remove(CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE_INCLUDE_OUTER);
		} catch (Exception ex) {
			log.error("Unable to Obtain RuleSet:", ex);
		}
	}

	/**
	 * The function collects all the rules applicable to a given RelNode
	 * 
	 * @param relNode
	 * @return set of rules
	 */
	public Set<RelOptRule> collectRules(RelNode relNode) {
		Set<RelOptRule> reloptrules = new LinkedHashSet<>();
		collectRulesRecursive(relNode, reloptrules);
		return reloptrules;
	}

	/**
	 * The function recursively collects all the rules applicable to a given RelNode
	 * 
	 * @param relNode
	 * @param rules
	 */
	private void collectRulesRecursive(RelNode relNode, Set<RelOptRule> rules) {
		// Add CoreRules
		for (RelOptRule rule : ruleset) {
			if (isRuleApplicable(rule, relNode)) {
				rules.add(rule);
			}
		}

		// Recursively traverse the children
		for (RelNode input : relNode.getInputs()) {
			collectRulesRecursive(input, rules);
		}
	}

	/**
     * The function checks if a rule is applicable to a given RelNode
     * @param rule
     * @param relNode
     * @return the rule applicable to the RelNode
     */
    private boolean isRuleApplicable(RelOptRule rule, RelNode relNode) {
        // Check if the rule's operand matches the RelNode
    	RelOptRuleOperand operand = rule.getOperand();
    	if(operand.matches(relNode)) {
    		if(rule.getOutTrait() instanceof EnumerableConvention convention && convention.getName().equals("ENUMERABLE")) {
    			return true;
    		}
			if (CollectionUtils.isNotEmpty(relNode.getInputs())) {
				if (CollectionUtils.isNotEmpty(operand.getChildOperands())) {
					if (operand.getChildOperands().size() == relNode.getInputs().size() && operand.getChildOperands().size()>1) {
						int ruleindex = 0;
						for (RelNode input : relNode.getInputs()) {
							RelOptRuleOperand childOperand = operand.getChildOperands().get(ruleindex++);
							if (childOperand.matches(input) && childOperand.getMatchedClass() != input.getClass()) {
								return false;
							}
						}
						return true;
					} else if(operand.getChildOperands().size() <= relNode.getInputs().size()) {
						for (RelNode input : relNode.getInputs()) {
							for(RelOptRuleOperand childOperand : operand.getChildOperands()) {
								if (childOperand.matches(input) && childOperand.getMatchedClass() == input.getClass()) {
									return true;
								}
							}
						}
						return false;
					}
					return true;
				}
				return true;
			} else {
    			return true;
    		}
       }
       return false;
    }
}
