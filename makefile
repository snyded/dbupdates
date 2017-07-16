# makefile
# This makes "dbupdates"

dbupdates: dbupdates.ec
	esql -static -O dbupdates.ec -o dbupdates -s
	@rm -f dbupdates.c
