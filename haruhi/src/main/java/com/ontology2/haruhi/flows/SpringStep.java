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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class SpringStep extends FlowStep {
    private static final Log logger = LogFactory.getLog(SpringStep.class);
    final List<String> argDefinitions;
    
    public SpringStep(List<String> argDefinitions) {
        this.argDefinitions=argDefinitions;
    }
    
    public List<String> getStepArgs(Map<String,Object> local,List<String> flowArgs) {
        SpringStepContext stepContext=new SpringStepContext(flowArgs,local);
        ExpressionParser parser = new SpelExpressionParser();
        List<String> stepArgs=Lists.newArrayList();
        
        for(String that:argDefinitions) {
            logger.info("parsing ["+that+"]");
            Expression e=parser.parseExpression(that);
            EvaluationContext c=new StandardEvaluationContext(stepContext);
            stepArgs.add(e.getValue(c,String.class));
        };
        
        return stepArgs;
    };
    
    public List<String> getStepArgs(String... flowArgs) {
        return getStepArgs(new HashMap<String,Object>(),Arrays.asList(flowArgs));
    };
    
    public class SpringStepContext {
        private final List<String> pos;
        private final Map<String,Object> local;

        public SpringStepContext(List<String> pos, Map<String, Object> local) {
            super();
            this.pos = pos;
            this.local = local;
        }
        
        public List<String> getPos() {
            return pos;
        }
        
        public Map<String,Object> getLocal() {
            return local;
        }
        
        //
        // right now hardcoded to the HDFS root
        //
        
        public String getTmpDir() {
            return "/";
        }
    }

}
