package com.ontology2.bakemono.dbpediaToBaseKB;

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
    DBpediaToBaseKBMapper mapper;
    Mapper<LongWritable,Text,Text,Text>.Context context;

    @Before
    public void setup() {
        mapper=new DBpediaToBaseKBMapper();
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
    public void tsukiNiKawatteOshiyokiyo() throws IOException, InterruptedException {
        mapper.map(
                new LongWritable(999),
                new Text("<http://a.b.c/>\t<http://rdf.basekb.com/ns/type.object.key>\t\"/wikipedia/en/Sailor_Moon\"\t."),
                context
        );
        verify(context).write(
            new Text("<http://dbpedia.org/resource/Sailor_Moon>"),
            new Text("<http://www.w3.org/2002/07/owl#sameAs>\t<http://a.b.c/>\t.")
        );
    }


}
