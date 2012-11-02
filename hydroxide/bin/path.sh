#!/bin/sh

BASEDIR=`dirname $BASH_SOURCE`/..
BASEDIR=`(cd "$BASEDIR"; pwd)`
BINDIR=$BASEDIR/target/appassembler/bin

chmod +x $BINDIR/*

export PATH=$PATH:$BINDIR
