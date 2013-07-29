package com.ontology2.bakemono;

import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import com.google.common.cache.LoadingCache;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.rdf.JenaUtil;

public class ParallelSuperEyeballMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {
	private static org.apache.commons.logging.Log logger = LogFactory.getLog(ParallelSuperEyeballMapper.class);
	final LoadingCache<String,Node> nodeParser=JenaUtil.createNodeParseCache();
	
	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
		PrimitiveTriple row3=new PrimitiveTriple("a","b","c");
		try {					

			Node_URI subject=(Node_URI) nodeParser.get(row3.subject);
			Node_URI predicate=(Node_URI) nodeParser.get(row3.predicate);
			Node object=nodeParser.get(row3.object);
			Triple realTriple=new Triple(subject,predicate,object);
			return;
		} catch(Throwable e) {
			return;
		}
	}

}
