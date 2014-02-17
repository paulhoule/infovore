package com.ontology2.bakemono.bloom;

import org.junit.Test;

public class BloomMathTest {
    @Test
    public void bloomAnswer() {
        int THOUSAND=1000;
        int MILLION=THOUSAND*THOUSAND;
        int BILLION=THOUSAND*MILLION;

        int n=100*MILLION;
        int m=1*BILLION;

        System.out.println("Optimal k:"+BloomMath.optimalK(m,n));
        System.out.println("Probability of false positive: "+BloomMath.p(m, n));
    }
}
