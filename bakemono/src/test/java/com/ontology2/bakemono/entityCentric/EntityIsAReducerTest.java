package com.ontology2.bakemono.entityCentric;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.*;
public class EntityIsAReducerTest {

    Reducer.Context context;
    Configuration configuration;
    EntityIsAReducer reducer;


    @Before
    public void setup() throws IOException, InterruptedException {
        context=mock(Reducer.Context.class);
        configuration=mock(Configuration.class);
        when(configuration.get(EntityIsAReducer.TYPE_LIST)).thenReturn("<http://rdf.basekb.com/ns/skiing.ski_area>");
        when(context.getConfiguration()).thenReturn(configuration);
        reducer = new EntityIsAReducer();
        reducer.setup(context);
    }

    @Test
    public void itIsA() throws IOException, InterruptedException {
        List<Text> facts=Lists.newArrayList(
            new Text("<http://rdf.basekb.com/ns/m.03m4lm6>\t<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://rdf.basekb.com/ns/skiing.ski_area>\t.")
            ,new Text("<http://rdf.basekb.com/ns/m.03m4lm6> <http://www.w3.org/2000/01/rdf-schema#label> \"Greek Peak\"@en .")
        );

        reducer.reduce(
                new Text("<http://rdf.basekb.com/ns/m.03m4lm6>")
                ,facts
                ,context);

        for(Text fact:facts) {
            verify(context).write(null,fact);
        }
    }

    @Test
    public void itIsNotA() throws IOException, InterruptedException {
        List<Text> facts=Lists.newArrayList(
                new Text("<http://rdf.basekb.com/ns/m.0531pd> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdf.basekb.com/ns/fictional_universe.fictional_organization> .")
                ,new Text("<http://rdf.basekb.com/ns/m.0531pd> <http://www.w3.org/2000/01/rdf-schema#label> \"Death Busters\"@en .")
        );

        reducer.reduce(
                new Text("<http://rdf.basekb.com/ns/m.0531pd>")
                ,facts
                ,context);

        verifyNoMoreInteractions(ignoreStubs(context));
    }
}
