Overview
--------

Infovore is an RDF processing system that uses Hadoop to process RDF data
sets in the billion triple range and beyond.  Infovore was originally designed to process
the (old) proprietary Freebase dump into RDF,  but once Freebase came out with an official RDF
dump,  Infovore gained the ability to clean and purify the dump,  making it not just possible
but easy to process Freebase data with triple stores such as Virtuoso 7.

Every week we run Infovore in Amazon Elastic/Map reduce in order to produce a product known as
[:BaseKB](http://basekb.com/)

Building
--------

mvn clean install

Installing
----------

The following cantrip, run from the top level "infovore" directory, initializes the bash shell
for the use of the "haruhi" program,  which can be used to run Infovore applications
packaged in the Bakemono Jar.

source haruhi/target/path.sh

More Information
----------------

See 

https://github.com/paulhoule/infovore/wiki 

for documentation and join the discussion group at

https://groups.google.com/forum/#!forum/infovore-basekb



[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/paulhoule/infovore/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

