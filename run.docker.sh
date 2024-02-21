#!/bin/bash
perl /usr/src/parser/report-parser.pl $@
printenv > /etc/environment
cron -f
