package com.ontology2.bakemono.baseKBToDBpedia;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestMapper {
    BaseKBToDBpediaMapper mapper;
    Mapper<LongWritable,Text,Text,Text>.Context context;

    @Before
    public void setup() {
        mapper=new BaseKBToDBpediaMapper();
        context=mock(Mapper.Context.class);
    }

    @Test
    public void rejectsOther() throws IOException, InterruptedException {
        mapper.map(
                new LongWritable(999),
                new Text("<http://a.b.c/>\t<http://rdf.basekb.com/ns/type.object.key>\t\"/wikipedia/en_id/136701\"\t."),
                context
        );
        verifyNoMoreInteractions(context);
    }

    @Test
    public void moonPrismPower() throws IOException, InterruptedException {
        mapper.map(
                new LongWritable(999),
                new Text("<http://a.b.c/>\t<http://rdf.basekb.com/ns/type.object.key>\t\"/wikipedia/en_title/Sailor_Moon\"\t."),
                context
        );

        verify(context).write(
                new Text("<http://a.b.c/>"),
                new Text("<http://www.w3.org/2002/07/owl#sameAs>\t<http://dbpedia.org/resource/Sailor_Moon>\t.")
        );
        verifyNoMoreInteractions(context);
    }
}
