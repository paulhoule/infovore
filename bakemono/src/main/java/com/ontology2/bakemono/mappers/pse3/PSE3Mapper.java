package com.ontology2.bakemono.mappers.pse3;

import java.io.IOException;

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
import com.ontology2.bakemono.PrimaryKeyValueAcceptor;
import com.ontology2.bakemono.abstractions.KeyValueAcceptor;
import com.ontology2.bakemono.abstractions.NamedKeyValueAcceptor;
import com.ontology2.bakemono.jena.NodePair;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.primitiveTriples.PrimitiveTripleCodec;
import com.ontology2.rdf.JenaUtil;

public class PSE3Mapper extends Mapper<LongWritable,Text,Node,NodePair> {
    private static org.apache.commons.logging.Log logger = LogFactory.getLog(PSE3Mapper.class);
    final LoadingCache<String,Node> nodeParser=JenaUtil.createNodeParseCache();

    final static PrimitiveTripleCodec p3Codec=new PrimitiveTripleCodec();

    //
    // all of these are deliberately in the default scope so that the test classes
    // can mess with them
    //

    MultipleOutputs mos;
    KeyValueAcceptor<Node,NodePair> accepted;
    KeyValueAcceptor<Text,Text> rejected;

    @Override
    protected void setup(Context context) throws IOException,
    InterruptedException {
        super.setup(context);
        mos=new MultipleOutputs(context);
        accepted=new PrimaryKeyValueAcceptor(context);
        rejected=new NamedKeyValueAcceptor(mos,"rejected");
    }

    @Override
    public void map(LongWritable arg0, Text arg1, Context c) throws IOException, InterruptedException {
        PrimitiveTriple row3=p3Codec.decode(arg1.toString());
        try {					
            Node_URI subject=(Node_URI) nodeParser.get(row3.subject);
            Node_URI predicate=(Node_URI) nodeParser.get(row3.predicate);
            Node object=nodeParser.get(row3.object);
            Triple realTriple=new Triple(subject,predicate,object);
            accepted.write(realTriple.getSubject(),new NodePair(realTriple.getPredicate(),realTriple.getObject()));
        } catch(Throwable e) {
            rejected.write(
                    new Text(row3.subject),
                    new Text(row3.predicate+"\t"+row3.object+"."));
        }
    }


    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        mos.close();
    }

}
