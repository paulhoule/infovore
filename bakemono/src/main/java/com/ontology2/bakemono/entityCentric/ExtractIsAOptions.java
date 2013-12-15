package com.ontology2.bakemono.entityCentric;


import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class ExtractIsAOptions implements HasOptions  {
    @Option(name="R",description="number of reducers")
    public int reducerCount;

    @Option(description="input file default directory")
    public String dir;

    @Option(description="input files",contextualConverter=PathConverter.class)
    public List<String> input;

    @Option(description="output files",contextualConverter=PathConverter.class)
    public String output;

    @Option(description="rdf prefix")
    public String prefix;

    @Option(description="accepted types")
    public List<String> type;

    public static class PathConverter implements ContextualConverter<String> {
        public String convert(String value, HasOptions that) {
            String defaultDir=getDefaultDir((ExtractIsAOptions) that);

            if(defaultDir.isEmpty())
                return value;

            Path there=new Path(defaultDir,value);
            return there.toString();
        }

        public String getDefaultDir(ExtractIsAOptions that) {
            return that.dir;
        }
    }

    public static class URIConverter implements ContextualConverter<String> {

        public String convert(String value, HasOptions that) {
            URI prefix=getPrefixURI((ExtractIsAOptions) that);

            URI there=prefix.resolve(value);
            return there.toString();
        }

        public URI getPrefixURI(ExtractIsAOptions that) {
            try {
                return new URI(that.prefix);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid -prefix URI ["+that.prefix+"]");
            }
        }
    }
}
