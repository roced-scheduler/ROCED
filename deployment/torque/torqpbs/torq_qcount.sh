#!/usr/bin/env bash
qstat | egrep "Q $1|R $1" | wc -l

