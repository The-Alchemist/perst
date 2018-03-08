#!/bin/sh

java -classpath .:../lib/perst.jar org.garret.perst.CompressDatabase $1 $2 $3
