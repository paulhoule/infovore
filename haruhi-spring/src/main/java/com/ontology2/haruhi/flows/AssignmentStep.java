package com.ontology2.haruhi.flows;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class AssignmentStep extends FlowStep {
    private static final Log logger = LogFactory.getLog(AssignmentStep.class);
    private final List<Assignment> assignments;
    
    public AssignmentStep(List<Assignment> assignments) {
        this.assignments=assignments;
    }
    
    public AssignmentStep(Assignment... assignments) {
        this.assignments=Arrays.asList(assignments);
    }
    
    /**
     * 
     * Updates local variables with new computed values; you are not allowed to change a variable
     * once it is set
     * 
     * @param local -- pre-existing local arguments;  this variable is not changed
     * @param flowArgs -- a list of flow arguments that defines positional variable
     * @return local arguments with the assignments added
     */
    
    public Map<String,Object> process(final Map<String,Object> local,final List<String> flowArgs) {
        HashMap<String, Object> output = new HashMap<>(local);
        SpringStepContext stepContext=new SpringStepContext(flowArgs,local);
        ExpressionParser parser = new SpelExpressionParser();
        
        for(Assignment that:assignments) {
            Expression e=parser.parseExpression(that.getExpression());
            StandardEvaluationContext c=new StandardEvaluationContext(stepContext);
            stepContext.assignVariables(c);
            Object value = e.getValue(c);
            logger.trace("parsing ["+that+"] with result +["+value+"]");
            if (output.containsKey(that.getAssignTo())) {
                throw new IllegalArgumentException("Cannot overwrite existing local variable ["+that.getAssignTo()+"]");
            }
            output.put(that.getAssignTo(),value);
        };
        return output;
    }
}
