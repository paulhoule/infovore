package com.ontology2.bakemono.sumRDF;

import com.ontology2.bakemono.RecyclingIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;

public class ReducerTest {
    private SumRDFReducer reducer;
    private Reducer<Text,FloatWritable,Text,Text>.Context context;

    @Before
    public void setup() throws IOException, InterruptedException {
        reducer=new SumRDFReducer();
        context=mock(Reducer.Context.class);
    }

    @Test
    public void addThemUp() throws IOException, InterruptedException {
        Iterable<FloatWritable> those=new RecyclingIterable(FloatWritable.class,
                new FloatWritable(0.2F),
                new FloatWritable(0.7F),
                new FloatWritable(0.1F)
        );

        reducer.reduce(new Text("<http://www.example.com/Headknocker>"),those,context);
        verify(context).write(
                new Text("<http://www.example.com/Headknocker>"),
                new Text("<http://rdf.basekb.com/public/subjectiveEye3D>\t\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>\t.")
        );
    }
}
