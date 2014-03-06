package com.ontology2.bakemono.rewriteSubject;

import com.ontology2.bakemono.mapreduce.InputPath;
import com.ontology2.bakemono.util.ReducerOptions;
import com.ontology2.centipede.parser.Option;

import java.util.List;

public class RewriteSubjectOptions extends ReducerOptions {
    @Option(description="owl:sameAs statements that rewrite subject")
    @InputPath(1)
    public List<String> sameAs;
}
