package com.ontology2.bakemono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

public class Main implements Runnable {


    private static Log logger = LogFactory.getLog(Main.class);

    public static class IncorrectUsageException extends Exception {
        public IncorrectUsageException(String message) {
            super(message);
        }

    }

    Map<String,Class> myApps=new HashMap<String,Class>() {{
        put("freebaseRDFPrefilter",FreebaseRDFTool.class);
        put("pse3",PSE3Tool.class);
        put("sieve3",Sieve3Tool.class);
        put("ranSample",RanSampleTool.class);
        put("fs",FsShell.class);                    // from Hadoop
        put("uniqURIObjects",UniqURIObjectTool.class);
        put("uniqInternalURIObjects", UniqueInternalURIObjectTool.class);
        put("uniqURISubjects", UniqueURISubjectTool.class);
        put("uniqURIPredicates", UniqueURIPredicateTool.class);
    }};
    
    Map<String, TopLevelCommand> myCommands=new HashMap<String,TopLevelCommand>() {{
        put("run",new RunATool());
        put("list",new ListTools());
    }};

    final List<String> args;

    public Main(String[] arg0) {
        args=Lists.newArrayList(arg0);
    }

    public static void main(String[] arg0) throws Exception {
        new Main(arg0).run();
    }

    @Override
    public void run() {
        try {
            try {
                parseArguments();
                cmd.run();
            } catch(IncorrectUsageException ex) {
                usage(ex);
                return;
            }
        } catch(Exception ex) {
            logger.error("Uncaught exception in application",ex);			
        };
    }

    void parseArguments() throws Exception {
        if(args.isEmpty())
            errorCausedByUser("you didn't specify any arguments");
        
        cmd = myCommands.get(args.get(0));

        if(cmd==null)
            errorCausedByUser("bakemono only supports the following commands: "+Joiner.on(" ").join(myCommands.keySet()));

        if(args.size()<cmd.getMinimumArgumentCount()+1)
            errorCausedByUser("the "+args.get(0)+" command requires at least one argument,  the name of a tool");

        cmd.validateArguments();
    }



    protected String toolName;
    protected Tool tool;
    protected List<String> toolArgs;

    TopLevelCommand cmd;

    String getToolName() {
        return this.toolName;
    }

    String getTool() {
        return this.toolName;
    }

    List<String> getToolArgs() {
        return toolArgs;
    }

    public static IncorrectUsageException errorCausedByUser(String error) throws IncorrectUsageException {
        throw new IncorrectUsageException(error);
    }

    private void usage(IncorrectUsageException ex) {
        System.out.println("User error: "+ex.getMessage());
    }

    abstract class TopLevelCommand implements Runnable {
        int getMinimumArgumentCount() { return 0; }
        void validateArguments() throws Exception {};
    };
    
    public class RunATool extends TopLevelCommand {
        @Override
        public void run()  {
            try {
                ToolRunner.run(tool,toolArgs.toArray(new String[0]));
            } catch(Exception ex) {
                logger.error("Uncaught excepion in application",ex);            
            };
        }
        
        @Override public int getMinimumArgumentCount() { return 1; }
        void validateArguments() throws Exception {
            toolName=args.get(1);
            if(!myApps.containsKey(toolName))
                errorCausedByUser("you specified a tool ["+toolName+"] not supported by the bakemono system");

            Class clazz=myApps.get(toolName);
            tool=(Tool) clazz.newInstance();
            toolArgs=Lists.newArrayList(Iterables.skip(args, 2));
        }
    }
    
    class ListTools extends TopLevelCommand {

        @Override
        public void run() {
            System.out.println("Tools supported by this build of bakemono:");
            System.out.println();
            for(Entry<String,Class> i:myApps.entrySet()) {
                System.out.println("    "+i.getKey());
            }
        }

    }
}
