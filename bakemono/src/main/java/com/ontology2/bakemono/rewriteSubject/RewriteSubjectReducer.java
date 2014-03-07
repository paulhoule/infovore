package com.ontology2.bakemono.rewriteSubject;

import com.ontology2.bakemono.joins.TaggedTextItem;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RewriteSubjectReducer extends Reducer<TaggedTextItem,TaggedTextItem,Text,Text> {
    PrimitiveTripleCodec ptc=new PrimitiveTripleCodec();
    Log LOG= LogFactory.getLog(RewriteSubjectReducer.class);
    @Override
    public void reduce(TaggedTextItem key, Iterable<TaggedTextItem> values, Context context) throws IOException, InterruptedException {
        String newSubject=null;
        for(TaggedTextItem value:values) {
            int tag=value.getTag().get();
            PrimitiveTriple t=ptc.decode(value.getKey().toString());
            switch(tag) {
                case 1:
                    if(t.getPredicate().equals("<http://www.w3.org/2002/07/owl#sameAs>"))
                        newSubject=t.getObject();
                break;
                default:
                    if(newSubject!=null)
                        context.write(
                                new Text(newSubject),
                                new Text(t.getPredicate()+"\t"+t.getObject()+"\t.")
                        );
            }
        }
    }
}
