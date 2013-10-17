package com.ontology2.bakemono.pse3;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.google.common.base.Splitter;
import com.google.common.cache.LoadingCache;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.bakemono.abstractions.KeyValueAcceptor;
import com.ontology2.bakemono.abstractions.NamedKeyValueAcceptor;
import com.ontology2.bakemono.abstractions.PrimaryKeyValueAcceptor;
import com.ontology2.bakemono.jena.NodePair;
import com.ontology2.bakemono.jena.WritableTriple;
import com.ontology2.bakemono.mapred.RealMultipleOutputs;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import com.ontology2.rdf.JenaUtil;

public class PSE3Mapper extends Mapper<LongWritable,Text,WritableTriple,LongWritable> {
    private static final LongWritable ONE = new LongWritable(1);
    
    private static org.apache.commons.logging.Log logger = LogFactory.getLog(PSE3Mapper.class);
    final LoadingCache<String,Node> nodeParser=JenaUtil.createNodeParseCache();

    final static PrimitiveTripleCodec p3Codec=new PrimitiveTripleCodec();
    private final Pattern $escape=Pattern.compile("[$][0-9A-F]{4}");
    
    //
    // all of these are deliberately in the default scope so that the test classes
    // can mess with them
    //

    RealMultipleOutputs mos;
    KeyValueAcceptor<WritableTriple,LongWritable> accepted;
    KeyValueAcceptor<Text,Text> rejected;

    
    @Override
    public void setup(Context context) throws IOException,
    InterruptedException {
        mos=new RealMultipleOutputs(context);
        super.setup(context);
        accepted=new PrimaryKeyValueAcceptor(context);
        rejected=new NamedKeyValueAcceptor(mos,"rejected");
    }

    int myCnt=0;
    @Override
    public void map(LongWritable arg0, Text arg1, Context c) throws IOException, InterruptedException {
        PrimitiveTriple row3=p3Codec.decode(arg1.toString());
        try {
            Node_URI subject=(Node_URI) nodeParser.get(row3.getSubject());
            Node_URI predicate=(Node_URI) nodeParser.get(row3.getPredicate());
            Node object=nodeParser.get(row3.getObject());

            if(has$escape(subject) || has$escape(predicate) || has$escape(object)) {
                reject(c,row3);
                return;
            }
            
            Triple realTriple=new Triple(subject,predicate,object);
            accepted.write(new WritableTriple(realTriple),ONE,c);
            incrementCounter(c,PSE3Counters.ACCEPTED,1);
        } catch(Throwable e) {
            logger.error("Caught exception in PSE3Mapper",e);
            reject(c, row3);
        }
    }
    
    //
    // Barf on $xxxx escape sequences in any data type
    //
    
    private boolean has$escape(Node that) {
        return $escape.matcher(that.toString()).find();
    }

    private void reject(Context c, PrimitiveTriple row3) throws IOException,
            InterruptedException {
        incrementCounter(c,PSE3Counters.REJECTED,1);
        rejected.write(
                new Text(row3.getSubject()),
                new Text(row3.getPredicate()+"\t"+row3.getObject()+"\t."),c);
    }

    //
    // this code prevents failing test because the mock object Context we are passing back
    // always returns null from getCounter...  With a more sophisticated mock object perhaps
    // the system will produce individual mocks for each counter so we can watch what
    // happens with counters
    //
    
    private void incrementCounter(Context context,Enum <?> counterId,long amount) {
        Counter counter=context.getCounter(counterId);
        if(counter!=null) {
            counter.increment(amount);
        };
    };

    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        mos.close();
    }

}
