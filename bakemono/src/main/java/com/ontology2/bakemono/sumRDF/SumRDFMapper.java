package com.ontology2.bakemono.sumRDF;

import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SumRDFMapper extends Mapper<LongWritable,Text,Text,FloatWritable> {
    PrimitiveTripleCodec ptc=new PrimitiveTripleCodec();
    private final String activePredicate="<http://www.yahoo.com/>";

    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
        PrimitiveTriple t=ptc.decode(value.toString());
        if(t.getPredicate().equals("<http://rdf.basekb.com/public/subjectiveEye3D>")) {
            String number=getQuoteContents(t.getObject());
            if(number!=null) {
                float numericValue=Float.parseFloat(number);
                context.write(new Text(t.getSubject()),new FloatWritable(numericValue));
            }
        }
    }

    // Notably not smart about \ characters (which don't exist in the
    // numeric cases we're handling)

    public static String getQuoteContents(String q) {
        int leftQ=q.indexOf('"');
        if(leftQ!=0)
            return null;
        int rightQ=q.indexOf('"', leftQ+ 1);
        if(rightQ<0)
            return null;

        return q.substring(leftQ+1,rightQ-leftQ);
    }
}
