package com.ontology2.hydroxide.partitionNTriplesApp;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.reporting.ReportingVocabulary;
import com.ontology2.millipede.shell.CommandLineApplication;
import com.ontology2.millipede.source.SingleFileSource;

public abstract class InfovoreApplication extends CommandLineApplication {

	protected final Model summary = ModelFactory.createDefaultModel();
	protected final ReportingVocabulary v = new ReportingVocabulary(summary);
	protected final Resource me = v.something();
	
	public void dontOverwrite(LineMultiFile<?> output) throws Exception {
		if(output.testExists()) {
			throw new Exception("Destination files already exist at ["+output.getFileName()+"]");	
		}
	}

	public void identifyInputFile(SingleFileSource<String> input) {
		Resource inputFile=v.something();
		summary.add(inputFile,v.a(),v.File());
		summary.add(inputFile,v.path(),v.file(input.getFile()));
		summary.add(summary.createLiteralStatement(inputFile,v.fromInstance(),PartitionsAndFiles.getInstanceName()));
		summary.add(inputFile,v.flowsTo(),me);
		summary.add(summary.createLiteralStatement(me,v.inputCharactersCount(),input.getChars()));
	}

	public void identifySelf() {
		summary.add(me,v.a(),v.Job());
		summary.add(me,v.implementedBy(),v.asClass(this));
	}

	public void writeSummaryFile(LineMultiFile<PrimitiveTriple> output) throws FileNotFoundException,
			IOException {
				OutputStream ttlOut = new FileOutputStream(output.summaryFile());
				summary.write(ttlOut,"TURTLE");
				ttlOut.close();
			}

}
