#!/bin/bash

OPTIND=1

LOGINPUT="insert into BookingsLogFile (logTime, process, stepId, message, rowCount)
values(NOW(), '$1', $2, '$3', $4);"

mysql -u $USERID -h$MYSQLHOST $SCHEMA -e "$LOGINPUT show warnings; show errors;"
