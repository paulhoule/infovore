package com.ontology2.bakemono.sieve3;

import java.util.Collection;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;

import org.apache.hadoop.fs.Path;

public class Sieve3Configuration {
    
    public static final String SIEVE3DEFAULT="sieve3Default";
    
    public static class Rule {
        private final String outputName;
        private final Predicate<PrimitiveTriple> condition;

        public Rule(final String outputName,final Predicate<PrimitiveTriple> condition) {
            this.outputName=outputName;
            this.condition=condition;
        }

        public String getOutputName() {
            return outputName;
        }

        public Predicate<PrimitiveTriple> getCondition() {
            return condition;
        }
    };
    
    private final ImmutableList<Rule> rules;
    public Sieve3Configuration(List<Rule> rules) {
        this.rules=new ImmutableList.Builder<Rule>().addAll(rules).build();
    }
    
    public Sieve3Configuration(Rule... rules) {
        this.rules=new ImmutableList.Builder<Rule>().add(rules).build();
    }
    
    public ImmutableList<Rule> getRules() {
        return rules;
    }
}
