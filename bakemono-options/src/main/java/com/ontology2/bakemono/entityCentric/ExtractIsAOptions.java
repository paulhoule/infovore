package com.ontology2.bakemono.entityCentric;


import com.ontology2.bakemono.util.ReducerOptions;
import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class ExtractIsAOptions extends ReducerOptions {

    @Option(description="rdf prefix")
    public String prefix;

    @Option(description="accepted types",contextualConverter=URIConverter.class)
    public List<String> type;


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
