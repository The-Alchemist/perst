#!/bin/sh

java -Xmx256M -classpath .:../lib/perst.jar TestDbServer $1 $2 $3

