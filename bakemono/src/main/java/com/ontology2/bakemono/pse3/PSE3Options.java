package com.ontology2.bakemono.pse3;


import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import org.apache.hadoop.fs.Path;

import java.util.List;

public class PSE3Options implements HasOptions {
    @Option(name="R",description="number of reducers")
    public int reducerCount;

    @Option(description="input and output file default directory")
    public String dir;

    @Option(description="input files",contextualConverter=PathConverter.class)
    public List<String> input;

    @Option(description="output file",contextualConverter=PathConverter.class)
    public String output;

    public static class PathConverter implements ContextualConverter<String> {
        public String convert(String value, HasOptions that) {
            String defaultDir=getDefaultDir((PSE3Options) that);

            if(defaultDir.isEmpty())
                return value;

            Path there=new Path(defaultDir,value);
            return there.toString();
        }

        public String getDefaultDir(PSE3Options that) {
            return that.dir;
        }
    }
}
