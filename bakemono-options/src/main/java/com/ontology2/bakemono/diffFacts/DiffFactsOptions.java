package com.ontology2.bakemono.diffFacts;

import com.ontology2.bakemono.util.DirectoryPrefixOptions;
import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;

import java.io.File;
import java.util.List;

public class DiffFactsOptions extends DirectoryPrefixOptions {
    @Option(name="R",description="number of reducers")
    public int reducerCount;

    @Option(description="input and output file default directory")
    public String dir;

    @Option(description="input paths for left side of comparison",contextualConverter=Converter.class)
    public List<String> left;

    @Option(description="input paths for right side of comparison",contextualConverter=Converter.class)
    public List<String> right;

    @Option(description="output path",contextualConverter=Converter.class)
    public String output;
}
