package com.ontology2.bakemono.joins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;
import static com.ontology2.bakemono.joins.SetJoinMapper.*;

public class TextSimpleJoinMapperTest {
    @Test
    public void dryrun() throws IOException, InterruptedException {
        SetJoinMapper<Text> m=new TextSimpleJoinMapper();
        Mapper.Context c1=mock(Mapper.Context.class);
        stub(c1.getConfiguration()).toReturn(
                new Configuration() {{
                    set(INPUTS + ".23", "s3n://basekb-now/2013-11-10/sieved/a/");
                    set(INPUTS + ".75", "s3n://basekb-now/2013-11-10/sieved/links/");
                }}
        );

        m.setup(c1);

        Mapper.Context c2=mock(Mapper.Context.class);
        stub(c2.getInputSplit()).toReturn(
                new FileSplit(
                    new Path("s3n://basekb-now/2013-11-10/sieved/links/links-m-00328.nt.gz")
                    ,0
                    ,0
                    ,null
                )
        );
        m.map(new LongWritable(666),new Text("Ganbaru!"),c2);
        verify(c2).getInputSplit();
        verify(c2).write(
                new TaggedTextItem(new Text("Ganbaru!"),new VIntWritable(75)), new VIntWritable(75)
        );
        verifyNoMoreInteractions(c2);

        Mapper.Context c3=mock(Mapper.Context.class);
        stub(c3.getInputSplit()).toReturn(
                new FileSplit(
                        new Path("s3n://basekb-now/2013-11-10/sieved/description/description-m-00099.nt.gz")
                        ,0
                        ,0
                        ,null
                )
        );
        m.map(new LongWritable(667),new Text("He was some kind of recording genius"),c3);
        verify(c3).getInputSplit();
        verify(c3).write(
                new TaggedTextItem(new Text("He was some kind of recording genius"),new VIntWritable(0)), new VIntWritable(0)
        );
        verifyNoMoreInteractions(c3);

        Mapper.Context c4=mock(Mapper.Context.class);
        stub(c4.getInputSplit()).toReturn(
                new FileSplit(
                        new Path("s3n://basekb-now/2013-11-10/sieved/a/a-m-21345.nt.gz")
                        ,0
                        ,0
                        ,null
                )
        );

        m.map(new LongWritable(668),new Text("Nothing comes easy"),c4);
        verify(c4).getInputSplit();
        verify(c4).write(
                new TaggedTextItem(new Text("Nothing comes easy"),new VIntWritable(23)), new VIntWritable(23)
        );
        verifyNoMoreInteractions(c4);
    }
}
