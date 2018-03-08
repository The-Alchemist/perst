#!/bin/sh

java -Xmx512M -classpath .:../lib/perst.jar TestPerf $1 $2 $3
