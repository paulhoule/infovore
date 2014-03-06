package com.ontology2.bakemono.util;

import com.ontology2.centipede.parser.Option;

public class ReducerOptions extends CommonOptions {
    @Option(name="R",description="number of reducers",defaultValue = "1")
    public int reducerCount;
}
