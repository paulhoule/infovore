package com.ontology2.bakemono.entityCentric;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class EntityCentricMapperTest {
    Mapper.Context context;
    EntityCentricMapper mapper;

    @Before
    public void setup() {
        mapper=new EntityCentricMapper();
        context=mock(Mapper.Context.class);
    }

    @Test
    public void extractSubjectAsKey() throws IOException, InterruptedException {
        final Text value = new Text("<http://example.com/A> <http://example.com/B> <http://example.com/C>");
        mapper.map(new LongWritable(1), value,context);
        verify(context).write(new Text("<http://example.com/A>"),value);
    }
}
