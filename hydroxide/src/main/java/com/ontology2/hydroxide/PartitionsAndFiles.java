package com.ontology2.hydroxide;

import java.io.Reader;
import java.io.Serializable;
import java.util.Map;

import com.ontology2.hydroxide.turtleZero.FreebaseKeyRecord;
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
import com.ontology2.millipede.sink.SingleFileSink;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.SingleFileSource;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import com.hp.hpl.jena.n3.turtle.parser.TurtleParser;

public class PartitionsAndFiles {
	public static SingleFileSource<FreebaseQuad> getRawQuads() throws Exception {
		String filename=getInputDirectory()+"/freebase-datadump-quadruples.tsv.bz2";
		Codec<FreebaseQuad> c=new QuadCodec();
		return new SingleFileSource<FreebaseQuad>(c,filename);
	}
	
	public static SingleFileSource<String> getSimpleTopicDump() throws Exception {
		String filename=getInputDirectory()+"/freebase-simple-topic-dump.tsv.bz2";
		return new SingleFileSource<String>(new IdentityCodec(),filename);
	}
	
	public static MultiFile<FreebaseQuad> getPartioned() {
		return createMultiFile("partioned",true);
	}
	
	public static LineMultiFile<FreebaseQuad> getSorted() {
		return createMultiFile("sorted",true);
	}
	
	public static TripleMultiFile getKeyStructure() {
		return createTripleMultiFile("keyStructure",true);
	}
	
	public static String getTurtleZeroFile() {
		return getWorkDirectory()+"/turtle0";
	}
	
	public static String getCommentCacheFile() {
		return getWorkDirectory()+"/commentCache";
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
		
		String osName=System.getProperty("os.name");
		if (osName.startsWith("Windows")) {
			return "d:/infovore";
		}
		return "/invofore";
	}

	public static String getDataDirectory() {
		return getBaseDirectory()+"/data";	
	}
	
	public static String getSourceDirectory() {
		return getBaseDirectory()+"/src";
	}
	
	public static String getInstanceDirectory() {
		String instanceName="current";
		Map<String,String> env=System.getenv();
		if (env.containsKey("INFOVORE_INSTANCE")) {
			instanceName=env.get("INFOVORE_INSTANCE");
		}
		return getDataDirectory()+"/"+instanceName;
	}
	
	public static String getFreebaseFile() {
		String instanceName=null;
		Map<String,String> env=System.getenv();
		if (env.containsKey("INFOVORE_FREEBASE_FILE")) {
			instanceName=env.get("INFOVORE_FREEBASE_FILE");
		}
		
		if (instanceName==null) {
			throw new RuntimeException("Path to Freebase RDF dump not given in INFOVORE_FREEBASE_FILE environment variable.");
		}
		
		return instanceName;
	}
	
	public static String getInputDirectory() {
		return getInstanceDirectory()+"/input";
	}
	
	public static String getWorkDirectory() {
		return getInstanceDirectory()+"/work";
	}
	
	public static String getOutputDirectory() {
		return getInstanceDirectory()+"/output";
	}
	
	private static String resolveFilename(String name) {
		if(name.startsWith("/")) {
			return getInstanceDirectory()+name;
		} else {
			return getWorkDirectory()+"/"+name;
		}
	}
	
	private static LineMultiFile<FreebaseQuad> createMultiFile(String name,boolean compressed) {
		PartitionOnSubject partitionFunction = getPartitionFunction();
		Codec<FreebaseQuad> c=new QuadCodec();
		return new LineMultiFile<FreebaseQuad>(
				resolveFilename(name), 
				"quads", 
				getCompressConfiguration(name,compressed) ? ".gz" : "", 
				partitionFunction,
				c);		
	}
	
	private static TripleMultiFile createTripleMultiFile(String name,boolean compressed) {
		return new TripleMultiFile(
				resolveFilename(name), 
				"triples", 
				getCompressConfiguration(name,compressed) ? ".nt.gz" : ".nt",
				getTriplePartitionFunction());		
	}
	
	private static NQuadsMultiFile createNQuadsMultiFile(String name,boolean compressed) {
		return new NQuadsMultiFile(
				resolveFilename(name), 
				"nquads", 
				getCompressConfiguration(name,compressed) ? ".nq.gz" : ".nq",
				getNQuadsPartitionFunction());		
	}	
	
	private static <T extends Serializable> SerializedMultiFile<T> createSerializedMultiFile(String name,boolean compressed) {
		return new SerializedMultiFile<T>(
				resolveFilename(name), 
				"objects", 
				getCompressConfiguration(name,compressed) ? ".ser.gz" : ".ser",
			    new DummyPartitionFunction<T>(1024));		
	}
	
	private static boolean getCompressConfiguration(String name,boolean compressedDefault) {
		String keyVar="COMPRESS_"+name;
		
		Map<String,String> env=System.getenv();
		if (!env.containsKey(keyVar)) {
			return compressedDefault;
		}
		
		String value=env.get(keyVar);
		return Boolean.parseBoolean(value);
	}

	private static PartitionOnSubject getPartitionFunction() {
		return new PartitionOnSubject(1024);
	}
	
	private static PartitionOnSubjectT getTriplePartitionFunction() {
		return new PartitionOnSubjectT(1024);
	}
	
	private static PartitionOnSubjectNQ getNQuadsPartitionFunction() {
		return new PartitionOnSubjectNQ(1024);
	}

	public static LineMultiFile<FreebaseKeyRecord> keyFile() {
		PartitionFunction<FreebaseKeyRecord> partitionFunction =
			new DummyPartitionFunction<FreebaseKeyRecord>(1024);
		
		Codec<FreebaseKeyRecord> codec=new FreebaseKeyRecord.Codec();
		return new LineMultiFile<FreebaseKeyRecord>(
				getWorkDirectory()+"/keys", 
				"keys", 
				".gz", 
				partitionFunction,
				codec);		
	}

	public static MultiFile<FreebaseQuad> getTurtleOne() {
		return createMultiFile("turtle1",true);
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

	public static MultiFile<FreebaseQuad> getTurtleThreeRejected() {
		return createMultiFile("turtle3rejected",true);
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

	public static NQuadsMultiFile getProInput() {
		return createNQuadsMultiFile("proInput",true);
	}

	public static TripleMultiFile getSortedKnownAs() {
		return createTripleMultiFile("sortedKnownAs",true);
	}
		
	public static TripleMultiFile getRawComments() {
		return createTripleMultiFile("rawComments",true);
	}

	public static TripleMultiFile getSortedComments() {
		return createTripleMultiFile("sortedComments",true);
	}

	public static TripleMultiFile getMissingArticles() {
		return createTripleMultiFile("missingArticles",false);
	}

	public static TripleMultiFile getHarvestedComments() {
		return createTripleMultiFile("harvestedComments",false);
	}

	public static TripleMultiFile getSortedHarvestedComments() {
		return createTripleMultiFile("sortedHarvestedComments",false);
	}
	
	public static LineMultiFile<PrimitiveTriple> getPartitionedExpandedTriples() {
		PartitionFunction<PrimitiveTriple> partitionFunction = new PartitionPrimitiveTripleOnSubject(1024);
		return new LineMultiFile<PrimitiveTriple>(
				resolveFilename("expandedRawFreebase") 
				,"triples"
				,getCompressConfiguration("expandedRawFreebase",true) ? ".gz" : "", 
				partitionFunction,
				new PrimitiveTripleCodec());		
	}
	
	public static SingleFileSource<String> getRawTriples() throws Exception {
		String filename=getFreebaseFile();
		Codec<FreebaseQuad> c=new QuadCodec();
		return SingleFileSource.createRaw(filename);		
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

	public static LineMultiFile<PrimitiveTriple> getBaseKBLimeRejected() {
		PartitionFunction<PrimitiveTriple> partitionFunction = new PartitionPrimitiveTripleOnSubject(1024);
		return new LineMultiFile<PrimitiveTriple>(
				resolveFilename("baseKBLimeRejected") 
				,"triples"
				,getCompressConfiguration("baseKBLimeRejected",true) ? ".gz" : "", 
				partitionFunction,
				new PrimitiveTripleCodec());	
	}
}
