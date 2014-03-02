package com.ontology2.bakemono.rewriteSubjectMapper;

import com.ontology2.bakemono.RecyclingIterable;
import com.ontology2.bakemono.joins.TaggedTextItem;
import com.ontology2.bakemono.rewriteSubject.RewriteSubjectReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestReducer {
    RewriteSubjectReducer reducer;
    Reducer<TaggedTextItem,TaggedTextItem,Text,Text>.Context context;

    @Before
    public void setup() {
        reducer=new RewriteSubjectReducer();
        context=mock(Reducer.Context.class);
    }

    @Test
    public void rewriteThis() throws IOException, InterruptedException {
        RecyclingIterable<TaggedTextItem> inputz=new RecyclingIterable<>(TaggedTextItem.class,
            new TaggedTextItem("<http://dbpedia.org/resources/Usagi_Tsukino>\t<http://www.w3.org/2002/07/owl#sameAs>\t<http://dbpedia.org/resources/Sailor_Moon>\t.",1),
            new TaggedTextItem("<http://dbpedia.org/resources/Usagi_Tsukino>\t<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://example.com/type/Superhero>\t.",16)
        );

        reducer.reduce(
                new TaggedTextItem("<http://dbpedia.org/resources/Usagi_Tsukino>",1),
                inputz,
                context
        );

        verify(context).write(
                new Text("<http://dbpedia.org/resources/Sailor_Moon>"),
                new Text("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://example.com/type/Superhero>\t.")
        );
    }

    @Test
    public void dontRewriteThat() throws IOException,InterruptedException {
        RecyclingIterable<TaggedTextItem> inputz=new RecyclingIterable<>(TaggedTextItem.class,
                new TaggedTextItem("<http://dbpedia.org/resources/Usagi_Tsukino>\t<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://example.com/type/Superhero>\t.",16)
        );

        reducer.reduce(
                new TaggedTextItem("<http://dbpedia.org/resources/Usagi_Tsukino>",1),
                inputz,
                context
        );

        verifyNoMoreInteractions(context);
    }
}
