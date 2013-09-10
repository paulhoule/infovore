package com.ontology2.chopper.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class CumulativeCount extends EvalFunc<Long> {

    long cnt=0;
    @Override
    public Long exec(Tuple input) throws IOException {
        cnt++;
        return cnt;
    }

}
