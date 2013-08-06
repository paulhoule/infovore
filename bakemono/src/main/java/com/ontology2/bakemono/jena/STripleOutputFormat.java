package com.ontology2.bakemono.jena;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.openjena.riot.out.SinkTripleOutput;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;

public class STripleOutputFormat extends TripleOutputFormat<Node,NodePair> {

    @Override
    protected Triple makeTriple(Node key, NodePair value) {
        // TODO Auto-generated method stub
        return new Triple(key,value.getOne(),value.getTwo());
    }

}
