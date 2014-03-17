Overview
--------

Infovore is an RDF processing system that uses Hadoop to process RDF data
sets in the billion triple range and beyond.  Infovore was originally designed to process
the (old) proprietary Freebase dump into RDF,  but once Freebase came out with an official RDF
dump,  Infovore gained the ability to clean and purify the dump,  making it not just possible
but easy to process Freebase data with triple stores such as Virtuoso 7.

Every week we run Infovore in Amazon Elastic/Map reduce in order to produce a product known as
[:BaseKB](http://basekb.com/).

Infovore depends on the [Centipede](https://github.com/paulhoule/centipede/wiki) framework for packaging
and processing command-line arguments.  The [Telepath](https://github.com/paulhoule/telepath/wiki) project
extends the Infovore project in order to process Wikipedia usage information to produce a product called
[:SubjectiveEye3D](https://github.com/paulhoule/telepath/wiki/SubjectiveEye3D).


Supporting
----------

It costs several hundreds of dollars per month to process and store files in connection with this work.
Please join <a href="https://www.gittip.com/">Gittip</a> and make a <a href="https://www.gittip.com/paulhoule/">small weekly donation</a> to keep this data free.


Building
--------

Infovore software requires JDK 7.

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




