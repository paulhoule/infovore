package com.ontology2.bakemono.diffFacts;

import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.List;

public class DiffFactsOptions implements HasOptions {
    @Option(name="R",description="number of reducers")
    public int reducerCount;

    @Option(description="input file default directory")
    public String dir;

    @Option(description="input paths for left side of comparison",contextualConverter=Converter.class)
    public List<String> left;

    @Option(description="input paths for right side of comparison",contextualConverter=Converter.class)
    public List<String> right;

    @Option(description="output path",contextualConverter=Converter.class)
    public String output;

    public static class Converter implements ContextualConverter<String> {
        public String convert(String value, HasOptions that) {
            String defaultDir=getDefaultDir((DiffFactsOptions) that);

            if(defaultDir.isEmpty())
                return value;

            Path there=new Path(defaultDir,value);
            return there.toString();
        }

        public String getDefaultDir(DiffFactsOptions that) {
            return that.dir;
        }
    }
}
