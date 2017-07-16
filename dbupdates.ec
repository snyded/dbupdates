/*
    dbupdates.ec - generates SQL to update statistics for a database or table
    Copyright (C) 1997  David A. Snyder
 
    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; version 2 of the License.
 
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
 
    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/

#ifndef lint
static char sccsid[] = "@(#)dbupdates.ec 1.1  08/08/97 11:09:35  08/08/97 11:50:20";
#endif /* not lint */


#include <stdio.h>
#include <string.h>

$char	owner[8+1];
char	*database = NULL, *table = NULL, *datafile = NULL;
int	debug = 0, dflg = 0, errflg = 0, lflg = 0, tflg = 0;
void	exit(), dberror();

main(argc, argv)
int     argc;
char    *argv[];
{
	$char	sqlstmt[BUFSIZ], tabtype[1+1];
	char	*dot;
	extern char	*optarg;
	extern int	optind, opterr;
	register int	c;
	void	gather_data(), us_highlow(), us_medium(), us_morehigh();

	/* Print copyright message */
	(void)fprintf(stderr, "DBUPDATES version 1.1, Copyright (C) 1997 David A. Snyder\n\n");

	/* get command line options */
	while ((c = getopt(argc, argv, "bd:lt:")) != EOF)
		switch (c) {
		case 'b':
			debug++;
			break;
		case 'd':
			dflg++;
			database = optarg;
			break;
		case 'l':
			lflg++;
			break;
		case 't':
			tflg++;
			table = optarg;
			break;
		default:
			errflg++;
			break;
		}

	if (argc > optind)
		datafile = argv[argc - 1];

	/* validate command line options */
	if (errflg || !dflg) {
		(void)fprintf(stderr, "usage: %s -d dbname [-t tabname] [-l] [datafile]\n", argv[0]);
		exit(1);
	}

	/* open the specified database */
	(void)sprintf(sqlstmt, "database %s", database);
	$prepare db_exec from $sqlstmt;
	if (sqlca.sqlcode)
		dberror("prepare db_exec");
	$execute db_exec;
	if (sqlca.sqlcode)
		dberror("execute db_exec");

	/* get the tabtype for the table specified (if tabname provided) */
	if (tflg) {
		if ((dot = strchr(table, '.')) == NULL) {
			(void)sprintf(sqlstmt, "select tabtype from 'informix'.systables where tabname = \"%s\"", table);
			*owner = NULL;
		} else {
			*dot = NULL;
			(void)strcpy(owner, table);
			table = ++dot;
			(void)sprintf(sqlstmt, "select tabtype from 'informix'.systables where owner = \"%s\" and tabname = \"%s\"",
			  owner, table);
		}
		$prepare get_tabtype from $sqlstmt;
		if (sqlca.sqlcode)
			dberror("prepare get_tabtype");
		$execute get_tabtype into $tabtype;
		if (sqlca.sqlcode) {
			if (sqlca.sqlcode == 100) {
				sqlca.sqlcode = -206;
				(void)strcpy(sqlca.sqlerrm, table);
			}
			dberror("execute get_tabtype");
		}
		if (*tabtype != 'T') {
			(void)fprintf(stderr, "%s: Only tables can have statistics updated.\n", argv[0]);
			exit(1);
		}
	}

	/* gather some data from the database */
	gather_data();
	$prepare highlow_stmt from
	  "select unique owner, tabname, colname from ustmp where us_level = ? order by tabname, colname";
	if (sqlca.sqlcode)
		dberror("prepare highlow_stmt");
	$declare highlow_curs cursor for highlow_stmt;
	if (sqlca.sqlcode)
		dberror("declare highlow_curs");

	/* generate some SQL code */
	(void)fprintf(stderr, "*** Generating DATABASE statement ***\n");
	(void)printf("database %s;\n\n", database);
	if (lflg) {
		(void)fprintf(stderr, "*** Generating UPDATE STATISTICS LOW DROP DISTRIBUTIONS statement ***\n");
		(void)printf("update statistics low drop distributions;\n\n");
	}
	(void)fprintf(stderr, "*** Generating UPDATE STATISTICS LOW statements ***\n");
	us_highlow('L');
	(void)fprintf(stderr, "*** Generating UPDATE STATISTICS MEDIUM statements ***\n");
	us_medium();
	(void)fprintf(stderr, "*** Generating UPDATE STATISTICS HIGH statements ***\n");
	us_highlow('H');
	if (*datafile) {
		(void)fprintf(stderr, "*** Generating more UPDATE STATISTICS HIGH statements ***\n");
		us_morehigh();
	}

	return(0);
}


void
gather_data()
{
	$char	sqlstmt[16384];
	char	*do_select();
	register int	i;

	*sqlstmt = NULL;

	(void)strcat(sqlstmt, do_select('H', 1));
	for (i = 2; i <= 16; i++) {
		(void)strcat(sqlstmt, "union ");
		(void)strcat(sqlstmt, do_select('L', i));
	}
	(void)strcat(sqlstmt, "into temp ustmp with no log;");

	$prepare ustmp_exec from $sqlstmt;
	if (sqlca.sqlcode)
		dberror("prepare ustmp_exec");
	$execute ustmp_exec;
	if (sqlca.sqlcode)
		dberror("execute ustmp_exec");
}


void
us_highlow(hl)
$char	hl;
{
	$char	tabname[18+1], colname[18+1];

	$open highlow_curs using $hl;
	if (sqlca.sqlcode)
		dberror("open highlow_curs");
	$fetch highlow_curs into $owner, $tabname, $colname;
	if (sqlca.sqlcode < 0)
		dberror("fetch(1) highlow_curs");
	while (sqlca.sqlcode != SQLNOTFOUND) {
		ldchar(owner, strlen(owner), owner);
		ldchar(tabname, strlen(tabname), tabname);
		ldchar(colname, strlen(colname), colname);
		(void)printf("update statistics %s for table '%s'.%s(%s);\n", (hl == 'H') ? "high" : "low", owner, tabname, colname);
		$fetch highlow_curs into $owner, $tabname, $colname;
		if (sqlca.sqlcode < 0)
			dberror("fetch(2) highlow_curs");
	}
	$close highlow_curs;
	if (sqlca.sqlcode)
		dberror("close highlow_curs");

	(void)putchar('\n');
}


void
us_medium()
{
	$char	sqlstmt[BUFSIZ], tabname[18+1], colname[18+1];
	$double	tabcol;
	char	buf[BUFSIZ], prevtab[18+1];
	short	first = 1;

	*prevtab = NULL;

	(void)sprintf(sqlstmt,
"select unique systables.owner, systables.tabname, syscolumns.colname, \
 systables.tabid + syscolumns.colno / 1000 tabcol \
 from systables, syscolumns \
 where systables.tabid >= 100 \
 and systables.tabtype = \"T\" \
 and systables.tabid = syscolumns.tabid \
 and (systables.tabid + syscolumns.colno / 1000) not in \
 (select unique tabcol from ustmp)");

	if (tflg) {
		if (*owner) {
			(void)sprintf(buf, "and systables.owner = \"%s\" ", owner);
			(void)strcat(sqlstmt, buf);
		}
		(void)sprintf(buf, "and systables.tabname = \"%s\" ", table);
		(void)strcat(sqlstmt, buf);
	}
	(void)strcat(sqlstmt, " order by tabcol;");

	$prepare medium_stmt from $sqlstmt;
	if (sqlca.sqlcode)
		dberror("prepare medium_stmt");
	$declare medium_curs cursor for medium_stmt;
	if (sqlca.sqlcode)
		dberror("declare medium_curs");

	$open medium_curs;
	if (sqlca.sqlcode)
		dberror("open medium_curs");
	$fetch medium_curs into $owner, $tabname, $colname, $tabcol;
	if (sqlca.sqlcode < 0)
		dberror("fetch(1) medium_curs");
	while (sqlca.sqlcode != SQLNOTFOUND) {
		ldchar(owner, strlen(owner), owner);
		ldchar(tabname, strlen(tabname), tabname);
		ldchar(colname, strlen(colname), colname);
		if (strcmp(tabname, prevtab)) {
			if (!first)
				(void)printf(");\n");
			else
				first = 0;
			(void)printf("update statistics medium for table '%s'.%s(%s", owner, tabname, colname);
			(void)strcpy(prevtab, tabname);
		} else
			(void)printf(",%s", colname);
		$fetch medium_curs into $owner, $tabname, $colname, $tabcol;
		if (sqlca.sqlcode < 0)
			dberror("fetch(2) medium_curs");
	}
	(void)printf(");\n");

	(void)putchar('\n');
}


void
us_morehigh()
{
	char	s[BUFSIZ], *owner, *tabname, *colname, *colon, *dot;

	if (!freopen(datafile, "r", stdin)) {
		perror(datafile);
		exit(1);
	}

	while (gets(s) != NULL) {
		owner = tabname = colname = NULL;

		if ((colon = strchr(s, ':')) != NULL) {
			*colon++ = NULL;
			owner = s;
		} else
			colon = s;

		if ((dot = strchr(colon, '.')) != NULL) {
			*dot++ = NULL;
			tabname = colon;
		}
		colname = dot;

		if (*tabname && *colname) {
			(void)printf("update statistics high for table ");
			if (*owner)
				(void)printf("'%s'.", owner);
			(void)printf("%s(%s);\n", tabname, colname);
		}
	}
}


char *
do_select(hl, part)
char	hl;
short	part;
{
	char	sqlstmt[BUFSIZ], buf[BUFSIZ];

	(void)sprintf(sqlstmt,
"select unique systables.owner, systables.tabname, syscolumns.colname, \
\"%c\" us_level, systables.tabid + syscolumns.colno / 1000 tabcol \
 from systables, syscolumns, sysindexes \
 where systables.tabid >= 100 \
 and systables.tabtype = \"T\" \
 and systables.tabid = syscolumns.tabid \
 and syscolumns.tabid = sysindexes.tabid \
 and syscolumns.colno = abs(sysindexes.part%d) ",
	  hl, part);

	if (tflg) {
		if (*owner) {
			(void)sprintf(buf, "and systables.owner = \"%s\" ", owner);
			(void)strcat(sqlstmt, buf);
		}
		(void)sprintf(buf, "and systables.tabname = \"%s\" ", table);
		(void)strcat(sqlstmt, buf);
	}

	return(sqlstmt);
}


void
dberror(object)
char	*object;
{
	int	msglen;
	char	buf[BUFSIZ], errmsg[BUFSIZ];

	if (debug)
		(void)fprintf(stderr, "SQL statment: %s\n", object);

	(void)rgetlmsg(sqlca.sqlcode, errmsg, sizeof(errmsg), &msglen);
	(void)sprintf(buf, errmsg, sqlca.sqlerrm);
	(void)fprintf(stderr, "%d: %s", sqlca.sqlcode, buf);

	exit(1);
}


