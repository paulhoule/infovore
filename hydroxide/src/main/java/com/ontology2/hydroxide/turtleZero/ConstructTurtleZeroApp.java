package com.ontology2.hydroxide.turtleZero;

import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.hydroxide.turtleZero.ExtractKeyRecordsApp.ExtractKeyRecords;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.source.NaturalOrdering;
import com.ontology2.millipede.source.OrderVerificationFilter;
import com.ontology2.millipede.source.OrderedMergeSource;
import com.ontology2.millipede.source.Source;
import com.ontology2.millipede.source.StoredValueSource;

public class ConstructTurtleZeroApp {
	static Logger logger=Logger.getLogger(ConstructTurtleZeroApp.class);
	
	public static void main(String[] args) throws Exception {
		LineMultiFile<FreebaseKeyRecord> in=PartitionsAndFiles.keyFile();
		Source<FreebaseKeyRecord> merged=OrderedMergeSource.fromMultiFile(in);
		Source<FreebaseKeyRecord> tested=new OrderVerificationFilter<FreebaseKeyRecord>(merged,new NaturalOrdering<FreebaseKeyRecord>());

		TurtleZero t0=new TurtleZero();
		
		Collection<FreebaseKeyRecord> failed=t0.addAllKeys(tested);
		int failCount=failed.size();
		int passCount=1;
		
		while(failCount>0) {
			logger.info("After pass "+passCount+" there are "+failCount+" failing namespace lookups");
			failed=t0.addAllKeys(new StoredValueSource<FreebaseKeyRecord>(failed));
			passCount++;
			if (failed.size()==failCount)
				break;
			failCount=failed.size();
		}
		
		logger.info("After pass "+passCount+" there are "+failCount+" failing namespace lookups");
		
		t0.logFails(failed);
		t0.close();
	}
}
