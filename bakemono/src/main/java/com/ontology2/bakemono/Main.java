package com.ontology2.bakemono;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.shell.CommandLineApplication;

public class Main implements Runnable {
	private static Log logger = LogFactory.getLog(Main.class);
	
	static class IncorrectUsageException extends Exception {
		public IncorrectUsageException(String message) {
			super(message);
		}

	}

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
				runTool();
			} catch(IncorrectUsageException ex) {
				usage(ex);
				return;
			}
		} catch(Exception ex) {
			logger.error("Uncaught excepion in application",ex);			
		};
	}

	//
	// for sake of testability this is broken out.  if anything needs to be actually
	// initialized that should be done by setting fields.
	//
	
	//
	// I didn't want to allow IncorrectUsageException() to be thrown from
	// runTool() but the way the Tool interface is implemented it isn't natural
	// to validate the parameters ahead of time.
	//
	

	void parseArguments() throws IncorrectUsageException {
		if(args.isEmpty())
			errorCausedByUser("you didn't specify any arguments");
		
		if(!args.get(0).equals("run"))
			errorCausedByUser("the only command supported is the run command");
		
		if(args.size()<2)
			errorCausedByUser("the run command requires at least one argument,  the name of a tool");
		
		toolName=args.get(1);
		if(!toolName.equals("freebaseRDFPrefilter"))
			errorCausedByUser("the run command can run only one tool,  the freebaseRDFPrefilter");
		
		tool=new FreebaseRDFTool();
		toolArgs=Lists.newArrayList(Iterables.skip(args, 2));
	}
	
	protected String toolName;
	protected Tool tool;
	protected List<String> toolArgs;
	
	String getToolName() {
		return this.toolName;
	}
	
	String getTool() {
		return this.toolName;
	}
	
	List<String> getToolArgs() {
		return toolArgs;
	}
	
	static IncorrectUsageException errorCausedByUser(String error) throws IncorrectUsageException {
		throw new IncorrectUsageException(error);
	}
	
	private void usage(IncorrectUsageException ex) {
		System.out.println("User error: "+ex.getMessage());
	}
	
	void runTool() throws Exception {
		ToolRunner.run(tool,toolArgs.toArray(new String[0]));
	}


}
