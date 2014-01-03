package com.ontology2.bakemono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.ontology2.bakemono.joins.FetchTriplesWithMatchingObjectsTool;
import com.ontology2.bakemono.joins.SetDifferenceTool;
import com.ontology2.bakemono.mapmap.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import arq.cmdline.CmdArgModule;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ontology2.bakemono.freebasePrefilter.FreebaseRDFTool;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.pse3.PSE3Tool;
import com.ontology2.bakemono.ranSample.RanSampleTool;
import com.ontology2.bakemono.sieve3.Sieve3Tool;
import com.ontology2.centipede.shell.CommandLineApplication;
import org.springframework.beans.factory.BeanNotOfRequiredTypeException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Main extends MainBase {

    public Main(String[] arg0) {
        super(arg0);
    }

    public List<String> getApplicationContextPath() {
        return Lists.newArrayList("com/ontology2/bakemono/applicationContext.xml");
    }

    public static void main(String[] arg0) throws Exception {
        new Main(arg0).run();
    }

}
