package com.ontology2.haruhi.flows;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;

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

    public void assignVariables(StandardEvaluationContext c) {
        for(int i=0;i<pos.size();i++) {
            c.setVariable("$"+Integer.toString(i), pos.get(i));
        }
        
        for(Entry<String, Object> that:local.entrySet()) {
            c.setVariable(that.getKey(), that.getValue());
        }
    }
}