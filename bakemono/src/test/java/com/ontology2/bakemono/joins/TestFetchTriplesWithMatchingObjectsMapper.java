package com.ontology2.bakemono.joins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.ontology2.bakemono.joins.SetJoinMapper.INPUTS;
import static org.mockito.Mockito.*;

public class TestFetchTriplesWithMatchingObjectsMapper {
    private FetchTriplesWithMatchingObjectsMapper that;
    private Mapper.Context setupContext;
    private Mapper.Context mapContext;

    @Before
    public void setup() throws IOException, InterruptedException {
        that=new FetchTriplesWithMatchingObjectsMapper();
        setupContext = mock(Mapper.Context.class);
        mapContext=mock(Mapper.Context.class);
        stub(setupContext.getConfiguration()).toReturn(
                new Configuration() {{
                    set(INPUTS + ".1", "s3n://basekb-sandbox/2013-11-10/uniqSubjects");
                    set(INPUTS + ".2", "s3n://basekb-now/2013-11-10/sieved/links/");
                }}
        );
    }

    @Test
    public void processItemFromObjectList() throws IOException, InterruptedException {
        stubPath("s3n://basekb-sandbox/2013-11-10/uniqSubjects/m-32194.gz");
        that.setup(setupContext);
        that.map(
                new LongWritable(1)
                , new Text("<http://rdf.example.com/DOOM>")
                , mapContext);
        verify(mapContext).write(
            argThat(new TaggedItemMatcher<Text>(new TaggedTextItem("<http://rdf.example.com/DOOM>", 1))),
            argThat(new TaggedItemMatcher<Text>(new TaggedTextItem("", 1)))
        );
    }

    @Test
    public void processItemFromFactList() throws IOException, InterruptedException {
        stubPath("s3n://basekb-now/2013-11-10/sieved/links/m-23456.gz");
        that.setup(setupContext);
        String fact="<http://rdf.example.com/A> <http://rdf.example.com/B> <http://rdf.example.com/C> .";
        that.map(
                new LongWritable(1)
                , new Text(fact)
                , mapContext);
        verify(mapContext).write(
                argThat(new TaggedItemMatcher<Text>(new TaggedTextItem("<http://rdf.example.com/C>", 2))),
                argThat(new TaggedItemMatcher<Text>(new TaggedTextItem(fact, 2)))
        );
    }

    private void stubPath(String pathString) {
        stub(setupContext.getInputSplit()).toReturn(
                new FileSplit(
                        new Path(pathString)
                        ,0
                        ,0
                        ,null
                )
        );
    }

}
