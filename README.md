Overview
--------

If you're interested in creating :BaseKB from an old Freebase quad dump,
get the [1.0](https://github.com/paulhoule/infovore/archive/v1.0.tar.gz) tag,
which has been tested on current hardware.  You can also download a
[pre-computed copy of :BaseKB](http://basekb.com/)

Documentation for the Infovore tools can be found on the [Wiki](https://github.com/paulhoule/infovore/wiki)
and [additional information](http://basekb.com/docs/) is available about :BaseKB. 

Work is proceeding on the trunk on tools for processing the official Freebase RDF dump
and other large RDF data sets.

Purpose
-------

The infovore framework is capable of converting Freebase to RDF,  rewriting
SPARQL queries using Freebase's name resolution system,  and running tests
to confirm correct operation of :BaseKB

Prerequsites
------------

You'll need a copy of the Freebase quad dump from

http://download.freebase.com/datadumps/

Infovore has been tested against the 2012-11-04 quad dump.

You'll need to select a base directory and an instance name for your
copy of Freebase.  You'll configure these using shell environment variables

$ export INFOVORE_BASE=/freebase
$ export INFOVORE_INSTANCE=2012-11-04

you should install your data dump at

/freebase/data/2012-11-04/input/freebase-datadump-quadruples.tsv.bz2

Infovore will write temporary files to the work subdirectory of the instance
directory and will write final output to the output subdirectory.

As currently configured,  the instance directory grows to 80GB in the
process of creating baseKBLite and baseKBPro.  Future versions of Infovore
may reduce disk consumption,  but currently intermediate files are saved in
case they are necessary for research and debugging

Running Infovore
----------------

First you should build and run the script that installs the path and
environment variables to run infovore

$ mvn clean install
$ source hydroxide-apps/path.sh

then do

$ createPro.sh

to create :BaseKB Pro,  a complete rendition of Freebase in RDF.  Once you've
created :BaseKB Pro,  you can do

$ createLite.sh

to create :BaseKB Lite,  a subset of :BaseKB Pro that is restricted to topics
that exist in both DBpedia and Freebase.
