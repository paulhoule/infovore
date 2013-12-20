package com.ontology2.bakemono.diffFacts;

import com.google.common.collect.Lists;
import com.ontology2.bakemono.RecyclingIterable;
import com.ontology2.bakemono.joins.TaggedTextItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;
public class TestReducer {
    DiffFactReducer<Text> that;
    Reducer.Context context;

    @Before
    public void setup() {
        that=new DiffFactReducer<Text>();
        context=mock(Reducer.Context.class);
    }

    @Test
    public void addAFact() throws IOException, InterruptedException {
        RecyclingIterable<VIntWritable> iterable=new RecyclingIterable(
                VIntWritable.class,
                DiffFactReducer.TWO
         );

        that.reduce(
            new TaggedTextItem("anderson",10)
            , iterable
            , context
        );

        verify(context).write(new Text("A"),new Text("anderson"));
        verifyNoMoreInteractions(context);
    }

    @Test
    public void subtractAFact() throws IOException, InterruptedException {
        RecyclingIterable<VIntWritable> iterable=new RecyclingIterable(
                VIntWritable.class,
                DiffFactReducer.ONE
        );

        that.reduce(
                new TaggedTextItem("cooper",10)
                , iterable
                , context
        );

        verify(context).write(new Text("D"),new Text("cooper"));
        verifyNoMoreInteractions(context);
    }

    @Test
    public void theMoreThingsStayTheSame() throws IOException, InterruptedException {
        RecyclingIterable<VIntWritable> iterable=new RecyclingIterable(
                VIntWritable.class,
                DiffFactReducer.ONE,
                DiffFactReducer.TWO
        );

        that.reduce(
                new TaggedTextItem("rather",10)
                , Lists.newArrayList(DiffFactReducer.ONE,DiffFactReducer.TWO)
                , context
        );

        verifyNoMoreInteractions(context);
    }
}
