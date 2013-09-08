--
-- bring in JAR with UDFs
--

REGISTER /home/paul/.m2/repository/com/ontology2/chopper/2.0-SNAPSHOT/chopper-2.0-SNAPSHOT-jar-with-dependencies.jar;

--
-- define aliases for UDFs so we can access them easily
--

DEFINE PrimitiveTripleInput com.ontology2.chopper.io.PrimitiveTripleInput;
DEFINE CumulativeCount com.ontology2.chopper.udf.CumulativeCount;
DEFINE CumulativeSum com.ontology2.chopper.udf.CumulativeSum;