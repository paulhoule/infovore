run lib/chopper.pig

rawFacts = LOAD '/5lime' USING com.ontology2.chopper.io.PrimitiveTripleInput();

sNodes = FOREACH rawFacts GENERATE s;
pNodes = FOREACH rawFacts GENERATE p;
oNodesAll = FOREACH rawFacts GENERATE o;
oNodes = FILTER oNodesAll BY SUBSTRING(o,0,1)=='<';

nodes = UNION sNodes,pNodes,oNodes;
groupNodes = GROUP nodes BY s;
countedNodes = FOREACH groupNodes GENERATE group AS uri:chararray,COUNT(nodes) AS cnt:long;
sortedNodes = ORDER countedNodes BY cnt DESC PARALLEL 1;
cumNodes = FOREACH sortedNodes GENERATE CumulativeCount() as id:int,uri AS uri:chararray,CumulativeSum(cnt) AS cumCount:long,cnt as cnt:long;

prevalenceGrouped = GROUP countedNodes By cnt;
prevalenceCounted = FOREACH prevalenceGrouped GENERATE group AS prevalence:long,COUNT(countedNodes) AS cnt; 
prevalenceSorted = ORDER prevalenceCounted BY cnt DESC;

groupNodesAll = GROUP nodes ALL;
bigCount = FOREACH groupNodesAll GENERATE COUNT(nodes);

distinctNodes = DISTINCT nodes;
groupDistinctNodesAll = GROUP distinctNodes All;
distinctCount = FOREACH groupDistinctNodesAll GENERATE COUNT(distinctNodes);

STORE prevalenceSorted INTO '/prevalenceSorted';

-- STORE cumNodes INTO '/cumNodes2.gz';
-- STORE bigCount INTO '/bigCount';
-- STORE distinctCount INTO '/distinctCount';

-- STORE prevalenceCounted 

--- bigCount = FOREACH groupNodesAll GENERATE COUNT();
--- gcn = GROUP cumNodes ALL;
--- cumTotal = FOREACH gcn GENERATE MAX(cumNodes.id) AS quantity:long,MAX(cumNodes.cumCount) AS totalOccurences:long; 

--- STORE cumTotal INTO '/cumNodes.gz';