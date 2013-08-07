package com.ontology2.bakemono.tools.freebasePrefilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.mappers.freebasePrefilter.FreebaseRDFMapper;

public class FreebaseRDFTool implements Tool {

    private Configuration configuration=new Configuration();

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration=conf;

    }

    @Override
    public int run(String[] arg0) throws Exception {
        if(arg0.length!=2)
            Main.errorCausedByUser("You must specify both input and output paths");

        String input=arg0[0];
        String output=arg0[1];

        Job job = new Job(configuration,"prefilter");
        job.setJarByClass(FreebaseRDFTool.class);		
        job.setMapperClass(FreebaseRDFMapper.class);

        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  

        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        return job.waitForCompletion(true) ? 0 :1;
    }

}
