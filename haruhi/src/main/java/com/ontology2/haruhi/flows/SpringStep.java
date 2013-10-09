package com.ontology2.haruhi.flows;

import java.util.Arrays;
import java.util.List;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.google.common.collect.Lists;

public abstract class SpringStep implements FlowStep {

    final List<String> argDefinitions;
    
    public SpringStep(List<String> argDefinitions) {
        this.argDefinitions=argDefinitions;
    }
    
    public List<String> getStepArgs(List<String> flowArgs) {
        SpringStepContext stepContext=new SpringStepContext(flowArgs);
        ExpressionParser parser = new SpelExpressionParser();
        List<String> stepArgs=Lists.newArrayList();
        
        for(String that:argDefinitions) {
            Expression e=parser.parseExpression(that);
            EvaluationContext c=new StandardEvaluationContext(stepContext);
            stepArgs.add(e.getValue(c,String.class));
        };
        
        return stepArgs;
    };
    
    public List<String> getStepArgs(String... flowArgs) {
        return getStepArgs(Arrays.asList(flowArgs));
    };
    
    public class SpringStepContext {
        private final List<String> pos;

        public SpringStepContext(List<String> pos) {
            super();
            this.pos = pos;
        }
        
        public List<String> getPos() {
            return pos;
        }
    }

}
