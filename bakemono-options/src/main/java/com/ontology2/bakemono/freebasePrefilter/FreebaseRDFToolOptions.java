package com.ontology2.bakemono.freebasePrefilter;

import com.ontology2.bakemono.util.CommonOptions;
import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import java.util.List;

public class FreebaseRDFToolOptions extends CommonOptions {
    @Option(name="R",description="number of reducers")
    public int reducerCount;
}
