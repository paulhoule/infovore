package com.ontology2.chopper.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class CumulativeSum extends EvalFunc<Long> {

    long sum=0;
    @Override
    public Long exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        
        try {
            long amt= (Long) input.get(0);
            sum += amt;
            return sum;
        } catch(Exception e) {
            throw new IOException("Caught exception processing input row ",e);
        }
    }
    

}
