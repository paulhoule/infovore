package com.ontology2.bakemono.sieve3;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.junit.Test;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.ontology2.bakemono.mapred.RealMultipleOutputs;

public class Sieve3MapperTest {

    public void itClosesMultipleOutputs() throws IOException, InterruptedException {
        Sieve3Mapper s3m=new Sieve3Mapper();
        s3m.mos=mock(RealMultipleOutputs.class);
        s3m.cleanup(mock(Context.class));
        verify(s3m.mos).close();
    }

}
