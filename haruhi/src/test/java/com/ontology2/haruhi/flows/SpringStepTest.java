package com.ontology2.haruhi.flows;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

public class SpringStepTest {

    @Test
    public void upperCaseArgument() {
        JobStep step=new JobStep(
                "'hello world'.toUpperCase()"
        );
        
        List<String> out=step.getStepArgs();
        assertEquals("HELLO WORLD",out.get(0));
        assertEquals(1,out.size());
    }
    
    @Test
    public void itDoesMath() {
        JobStep step=new JobStep(
                "(7*7).toString()"
        );
        
        List<String> out=step.getStepArgs();
        assertEquals("49",out.get(0));
        assertEquals(1,out.size());
    }
    
    @Test
    public void itFindPositionalArguments() {
        JobStep step=new JobStep(
                "'furry '+pos[0]+' brothers'"
        );
        
        List<String> out=step.getStepArgs("freak");
        assertEquals("furry freak brothers",out.get(0));
        assertEquals(1,out.size());
    }



}
