package com.ontology2.haruhi.flows;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public abstract class SpringStep extends FlowStep {
    private static final Log logger = LogFactory.getLog(SpringStep.class);
    final List<String> argDefinitions;
    
    public SpringStep(List<String> argDefinitions) {
        this.argDefinitions=argDefinitions;
    }
    
    public List<String> getStepArgs(Map<String,Object> local,List<String> flowArgs) {
        SpringStepContext stepContext=new SpringStepContext(flowArgs,local);
        ExpressionParser parser = new SpelExpressionParser();
        List<String> stepArgs=new ArrayList<>();
        
        for(String that:argDefinitions) {
            Expression e=parser.parseExpression(that);
            StandardEvaluationContext c=new StandardEvaluationContext(stepContext);
            stepContext.assignVariables(c);
            String value = e.getValue(c,String.class);
            logger.trace("parsing ["+that+"] with result +["+value+"]");
            stepArgs.add(value);
        };
        
        return stepArgs;
    };
    
    public List<String> getStepArgs(String... flowArgs) {
        return getStepArgs(new HashMap<String,Object>(),Arrays.asList(flowArgs));
    }

}
