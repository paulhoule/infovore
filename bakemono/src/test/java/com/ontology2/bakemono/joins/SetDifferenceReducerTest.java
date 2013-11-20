package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.mockito.Mockito.*;

public class SetDifferenceReducerTest {
    SetDifferenceReducer<Text> that;
    Reducer.Context context;

    @Before
    public void setup() {
        that=new SetDifferenceReducer<Text>();
        context=mock(Reducer.Context.class);
    }

    @Test
    public void justOne() throws IOException, InterruptedException {
        that.reduce(
            new TaggedTextItem(
                    new Text("Canada Goose")
                    ,new VIntWritable(5)
            )
            ,new ArrayList<VIntWritable>() {{
                add(new VIntWritable(1));
            }}
            ,context);

        verify(context).write(
                new Text("Canada Goose")
                ,null);
        verifyNoMoreInteractions(context);
    }

    @Test
    public void justTwo() throws IOException, InterruptedException {
        that.reduce(
                new TaggedTextItem(
                        new Text("Canada Goose")
                        ,new VIntWritable(5)
                )
                ,new ArrayList<VIntWritable>() {{
                    add(new VIntWritable(2));
                }}
                ,context);

        verifyNoMoreInteractions(context);
    }

    @Test
    public void oneAndTwo() throws IOException, InterruptedException {
        that.reduce(
                new TaggedTextItem(
                        new Text("Canada Goose")
                        ,new VIntWritable(5)
                )
                ,new ArrayList<VIntWritable>() {{
                    add(new VIntWritable(1));
                    add(new VIntWritable(2));
                }}
                ,context);

        verifyNoMoreInteractions(context);
    }
}
