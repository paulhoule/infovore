package com.ontology2.hydroxide.randomSampler;

import java.io.PrintWriter;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Sets;
import com.hp.hpl.jena.rdf.model.Model;
import com.ontology2.hydroxide.partitionNTriplesApp.InfovoreApplication;
import com.ontology2.millipede.Plumbing;
import com.ontology2.millipede.sink.EmptyReportSink;
import com.ontology2.millipede.sink.LineSink;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.SingleFileSource;

public class RandomSamplerApp extends InfovoreApplication {

    @Override
    protected void _run(String[] args) throws Exception {
        if(args.length<2) {
            die("randomSampler [input filename] [p]");
        }

        String inputFilename=args[0];
        final double probability=Double.parseDouble(args[1]);

        SingleFileSource<String> input=SingleFileSource.createRaw(inputFilename);
        final LineSink output=new LineSink(new PrintWriter(System.out));
        Sink<String> randomSampler=new EmptyReportSink<String>() {
            Set<String> alreadySaw=Sets.newHashSet();

            final Random generator = new Random();

            @Override
            public void accept(String obj) throws Exception {
                double uniformDeviate = generator.nextDouble();
                if (obj.startsWith("@")) {
                    if(!alreadySaw.contains(obj)) {
                        output.accept(obj);
                        alreadySaw.add(obj);
                    }
                } else if(uniformDeviate<probability) {
                    output.accept(obj);
                }
            }
        };

        Plumbing.flow(input, randomSampler);
    }
}
