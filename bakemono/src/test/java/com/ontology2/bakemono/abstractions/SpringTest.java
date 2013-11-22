package com.ontology2.bakemono.abstractions;

import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.context.ApplicationContext;

import com.ontology2.bakemono.sieve3.Sieve3Configuration;

public class SpringTest {

    @Test
    public void canGetSieve3Configuration() {
        // right now we don't actually use the Hadoop Config,  so null is OK
        ApplicationContext c=Spring.getApplicationContext(null);
        Sieve3Configuration sieve3Config = c.getBean(Sieve3Configuration.SIEVE3DEFAULT,Sieve3Configuration.class);
        assertEquals(sieve3Config.getRules().size(),13);
    }

}
