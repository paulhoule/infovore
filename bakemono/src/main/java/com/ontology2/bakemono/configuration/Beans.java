package com.ontology2.bakemono.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.sieve3.Sieve3Configuration;
import com.ontology2.bakemono.sieve3.Sieve3Configuration.Rule;

@Configuration
public class Beans {
    
  @Bean
  public Sieve3Configuration sieve3Default() {
  return new Sieve3Configuration(
          new Rule("a", matchesA()),
          new Rule("label", matchesLabel()),
          new Rule("name", matchesTypeObjectName()),
          new Rule("keyNs", inKeyNamespace()),
          new Rule("key", matchesTypeObjectKey()),
          new Rule("notableForPredicate",matchesNotableForPredicate()),
          new Rule("description",matchesDescription()),
          new Rule("text",matchesText()),
          new Rule("webpages",matchesWebPage()),
          new Rule("notability",isAboutNotability()),
          new Rule("links",isLinkRelationship())
  );
}

private static Predicate<PrimitiveTriple> matchesPredicate(final String that) {
  return new Predicate<PrimitiveTriple>() {
      @Override public boolean apply(PrimitiveTriple input) {
          return input.getPredicate().equals(that);
      }
  };
}

private static Predicate<PrimitiveTriple> predicateStartsWith(final String that) {
  return new Predicate<PrimitiveTriple>() {
      @Override public boolean apply(PrimitiveTriple input) {
          return input.getPredicate().startsWith("<"+that);
      }
  };
}

private static Predicate<PrimitiveTriple> matchesA() {
  return matchesPredicate("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
};

private static Predicate<PrimitiveTriple> matchesTypeObjectName() {
  return matchesPredicate("<http://rdf.basekb.com/ns/type.object.name>");
};

private static Predicate<PrimitiveTriple> matchesLabel() {
  return matchesPredicate("<http://www.w3.org/2000/01/rdf-schema#label>");
};

private static Predicate<PrimitiveTriple> inKeyNamespace() {
  return predicateStartsWith("http://rdf.basekb.com/key/");
};

private static Predicate<PrimitiveTriple> matchesTypeObjectKey() {
  return matchesPredicate("<http://rdf.basekb.com/ns/type.object.key>");
};

static Predicate<PrimitiveTriple> isLinkRelationship() {
  return new Predicate<PrimitiveTriple>() {
      @Override public boolean apply(PrimitiveTriple input) {
          return input.getObject().startsWith("<") && input.getObject().endsWith(">");
      }
  };
};

private static Predicate<PrimitiveTriple> matchesNotableForPredicate() {
  return matchesPredicate("<http://rdf.basekb.com/ns/common.notable_for.predicate>");
};

private static Predicate<PrimitiveTriple> matchesDescription() {
  return matchesPredicate("<http://rdf.basekb.com/ns/common.topic.description>");
};

private static Predicate<PrimitiveTriple> matchesText() {
  return matchesPredicate("<http://rdf.basekb.com/ns/common.document.text>");
};

private static Predicate<PrimitiveTriple> matchesWebPage() {
  return matchesPredicate("<http://rdf.basekb.com/ns/common.topic.topic_equivalent_webpage>");
};

private static Predicate<PrimitiveTriple> isAboutNotability() {
  return Predicates.or(
          matchesPredicate("<http://rdf.basekb.com/ns/common.topic.notable_types>"),
          matchesPredicate("<http://rdf.basekb.com/ns/common.notable_for.object>"),
          matchesPredicate("<http://rdf.basekb.com/ns/common.topic.notable_for>"),
          matchesPredicate("<http://rdf.basekb.com/ns/common.topic.notable_for.notable_object>")
  );
};
}
