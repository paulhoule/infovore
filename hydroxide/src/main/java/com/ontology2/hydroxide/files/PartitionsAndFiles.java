package com.ontology2.hydroxide.files;

import java.io.File;
import java.io.Reader;
import java.io.Serializable;
import java.util.Map;

import com.ontology2.millipede.Codec;
import com.ontology2.millipede.DummyPartitionFunction;
import com.ontology2.millipede.FileOpener;
import com.ontology2.millipede.IdentityCodec;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.NQuadsMultiFile;
import com.ontology2.millipede.PartitionFunction;
import com.ontology2.millipede.SerializedMultiFile;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.primitiveTriples.PartitionPrimitiveTripleOnSubject;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.primitiveTriples.PrimitiveTripleCodec;
import com.ontology2.millipede.sink.SingleFileSink;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.SingleFileSource;
import com.ontology2.millipede.triples.PartitionOnSubjectT;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import com.hp.hpl.jena.n3.turtle.parser.TurtleParser;

public class PartitionsAndFiles {
	
	
	public static TripleMultiFile getKeyStructure() {
		return createTripleMultiFile("keyStructure",true);
	}
	
	public static String getTurtleZeroFile() {
		return getWorkDirectory()+"/turtle0";
	}
	
	public static String getTurtleZeroRuleboxFile() {
		return getWorkDirectory() + "/ruleboxes/TurtleZeroRulebox.ttl";
	}
	
	public static String getNamespaceUsage() {
		return getWorkDirectory() + "/namespaceUsage.txt";
	}
	
	public static String getNamespaceList() {
		return getWorkDirectory() + "/namespaceList.txt";
	}
	
	public static TripleMultiFile getCore() {
		return createTripleMultiFile("core",true);
	}
	
	public static TripleMultiFile getAnomaly() {
		return createTripleMultiFile("anomaly",true);
	}

	private static String _baseDirectory=null;
	public static String getBaseDirectory() {
		if (null==_baseDirectory) {
			_baseDirectory=_getBaseDirectory();
		}
		return _baseDirectory;	
	}
	
	private static String _getBaseDirectory() {
		Map<String,String> env=System.getenv();
		if (env.containsKey("INFOVORE_BASE")) {
			return env.get("INFOVORE_BASE");
		}
		
		File homeDirectory=new File(System.getProperty("user.home"));
		return new File(homeDirectory,"infovoreStorage").getAbsolutePath();
	}

	public static String getDataDirectory() {
		return getBaseDirectory()+"/data";	
	}
	
	public static String getSourceDirectory() {
		return getBaseDirectory()+"/src";
	}
	
	public static String getInstanceDirectory() {
		String instanceName = getInstanceName();
		return getDataDirectory()+"/"+instanceName;
	}

	public static String getInstanceName() {
		String instanceName="default";
		Map<String,String> env=System.getenv();
		if (env.containsKey("INFOVORE_INSTANCE")) {
			instanceName=env.get("INFOVORE_INSTANCE");
		}
		return instanceName;
	}
	
	public static String getWorkDirectory() {
		return getInstanceDirectory()+"/work";
	}
	
	public static String getOutputDirectory() {
		return getInstanceDirectory()+"/output";
	}
	
	static String resolveFilename(String name) {
		return getInstanceDirectory()+"/"+name;
	}
	
	
	static TripleMultiFile createTripleMultiFile(String name,boolean compressed) {
		return new TripleMultiFile(
				resolveFilename(name), 
				"triples", 
				getCompressConfiguration(name,compressed) ? ".nt.gz" : ".nt",
				getTriplePartitionFunction());		
	}
	
	private static <T extends Serializable> SerializedMultiFile<T> createSerializedMultiFile(String name,boolean compressed) {
		return new SerializedMultiFile<T>(
				resolveFilename(name), 
				"objects", 
				getCompressConfiguration(name,compressed) ? ".ser.gz" : ".ser",
			    new DummyPartitionFunction<T>(1024));		
	}
	
	static boolean getCompressConfiguration(String name,boolean compressedDefault) {
		String keyVar="COMPRESS_"+name;
		
		Map<String,String> env=System.getenv();
		if (!env.containsKey(keyVar)) {
			return compressedDefault;
		}
		
		String value=env.get(keyVar);
		return Boolean.parseBoolean(value);
	}
	
	private static PartitionOnSubjectT getTriplePartitionFunction() {
		return new PartitionOnSubjectT(1024);
	}

	
	public static String getTurtleOneRejectedFile() {
		return getOutputDirectory()+"/turtle1Rejected.quads";
	}

	public static TripleMultiFile getTurtleTwoFacts() {
		return createTripleMultiFile("turtle2Facts",true);
	}
	
	public static String getTurtleTwoFile() {
		return getWorkDirectory()+"/turtle2.ttl";
	}

	public static TripleMultiFile getTurtleThree() {
		return createTripleMultiFile("turtle3",true);
	}

	
	public static TripleMultiFile getKnownAs() {
		return createTripleMultiFile("knownAs",false);
	}

	public static MultiFile<Triple> getKeyProperties() {
		return createTripleMultiFile("keyProperties",false);
	}
	
	public static SerializedMultiFile<int[]> getLinkFrom() {
		return createSerializedMultiFile("linkFrom",false);
	}
	
	public static SerializedMultiFile<int[]> getLinkTo() {
		return createSerializedMultiFile("linkTo",false);
	}
	
	public static SerializedMultiFile<int[]> getWhiteList() {
		return createSerializedMultiFile("whitelist",false);
	}
	
	public static SerializedMultiFile<int[]> getBlackList() {
		return createSerializedMultiFile("blacklist",false);
	}
	
	public static String getExpandedBlackListFile() {
		return getWorkDirectory()+"/expandedBlacklist.ser";
	}

	public static TripleMultiFile getBaseKBLite() {
		return createTripleMultiFile("/output/baseKBLite",true);
	}

	public static TripleMultiFile getBaseKBPro() {
		return createTripleMultiFile("/output/baseKBPro",true);
	}

	public static TripleMultiFile getSortedKnownAs() {
		return createTripleMultiFile("sortedKnownAs",true);
	}
	
	// TODO: the multipler factor is something that depends on the task and the job
	// setting the thread count high will cause my Windows machine to get hosed and
	// means I can't do other things.  On the other hand,  if the machine is a cloud
	// machine doing one task,  this doesn't matter.
	
	public static int getNThreads() {
		Map<String,String> env=System.getenv();
		if (env.containsKey("INFOVORE_DEBUG")) {
			return 1;
		}
		
		return 0;
		//return 2*Runtime.getRuntime().availableProcessors();
	}

	public static int getPartitionCount() {
		return 1024;
	}

	public static Sink<String> getRawFreebaseRejected() throws Exception {
		String filename=getWorkDirectory()+"/freebase-raw-rejected.tsv";
		return new SingleFileSink<String>(new IdentityCodec(),filename);
	}
	

	public static TripleMultiFile getBaseKBLime() {
		return createTripleMultiFile("baseKBLime",true);
	}

	public static TripleMultiFile getBaseKBFsr() {
		return createTripleMultiFile("baseKB_FSR",true);
	}
}
