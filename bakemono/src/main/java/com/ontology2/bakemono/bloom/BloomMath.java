package com.ontology2.bakemono.bloom;

public class BloomMath {
    public static double p(int m,int n,int k) {
        return Math.pow(-Math.expm1(-k*((1.0)*n/m)),k);
    }

    public static double optimalK(int m,int n) {
        return (Math.log(2)*m)/n;
    }

    public static double p(int m,int n) {
        return p(m,n,(int) Math.ceil(optimalK(m,n)));
    };
}
