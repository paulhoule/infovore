package com.ontology2.bakemono.joins;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class TestAcceptWithMatchingKeyReducer {
    AcceptWithMatchingKeyReducer<Text,Text> that;
    Reducer.Context context;

    @Before
    public void setup() {
        that=new AcceptWithMatchingKeyReducer<Text,Text>();
        context=mock(Reducer.Context.class);
    }

    @Test
    public void rejectsIfNoMatch() throws IOException, InterruptedException {
        TaggedTextItem key=new TaggedTextItem(
                new Text("xanadu")
                ,2);

        Iterable<TaggedItem<Text>> stream=new ArrayList<TaggedItem<Text>>() {{
            add(new TaggedTextItem("in xanadu did Kublah Kahn a stately pleasure dome decree",2));
            add(new TaggedTextItem("Ted Nelson developed the Xanadu Green prototype in 1979",2));
        }};

        that.reduce(key,stream,context);
        verifyNoMoreInteractions(context);
    }

    @Test
    public void acceptsIfMatch() throws IOException, InterruptedException {
        TaggedTextItem key=new TaggedTextItem(
                new Text("xanadu")
                ,1);

        Iterable<TaggedItem<Text>> stream=new ArrayList<TaggedItem<Text>>() {{
            add(new TaggedTextItem("",1));
            add(new TaggedTextItem("in xanadu did Kublah Kahn a stately pleasure dome decree",2));
            add(new TaggedTextItem("Ted Nelson developed the Xanadu Green prototype in 1979",2));
        }};

        that.reduce(key,stream,context);
        verify(context).write(null,new Text("in xanadu did Kublah Kahn a stately pleasure dome decree"));
        verify(context).write(null,new Text("Ted Nelson developed the Xanadu Green prototype in 1979"));
        verifyNoMoreInteractions(context);
    }

    @Test
    public void rejectsIfNoFacts() throws IOException, InterruptedException {
        TaggedTextItem key=new TaggedTextItem(
                new Text("xanadu")
                ,1);

        Iterable<TaggedItem<Text>> stream=new ArrayList<TaggedItem<Text>>() {{
            add(new TaggedTextItem("",1));
        }};

        that.reduce(key,stream,context);
        verifyNoMoreInteractions(context);
    }
}
