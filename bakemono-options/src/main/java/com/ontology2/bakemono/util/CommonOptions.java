package com.ontology2.bakemono.util;

import com.ontology2.bakemono.mapreduce.InputPath;
import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;

import java.util.List;

public class CommonOptions extends DirectoryPrefixOptions {


    @Option(description="input paths",contextualConverter=Converter.class)
    @InputPath(16)
    public List<String> input;

    @Option(description="output path",contextualConverter=Converter.class)
    public String output;


}
