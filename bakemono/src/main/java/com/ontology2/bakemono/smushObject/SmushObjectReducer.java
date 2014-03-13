package com.ontology2.bakemono.smushObject;

import com.ontology2.bakemono.joins.TaggedTextItem;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SmushObjectReducer  extends Reducer<TaggedTextItem,TaggedTextItem,Text,Text> {
    PrimitiveTripleCodec ptc=new PrimitiveTripleCodec();
    Log LOG= LogFactory.getLog(SmushObjectReducer.class);
    @Override
    public void reduce(TaggedTextItem key, Iterable<TaggedTextItem> values, Context context) throws IOException, InterruptedException {
        String newObject=null;
        for(TaggedTextItem value:values) {
            int tag=value.getTag().get();
            PrimitiveTriple t=ptc.decode(value.getKey().toString());
            switch(tag) {
                case 1:
                    if(t.getPredicate().equals("<http://www.w3.org/2002/07/owl#sameAs>"))
                        newObject=t.getObject();
                    break;
                default:
                    if(newObject!=null)
                        context.write(
                                new Text(t.getSubject()),
                                new Text(t.getPredicate()+"\t"+newObject+"\t.")
                        );
            }
        }
    }
}
