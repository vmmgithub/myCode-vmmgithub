#!/bin/bash

FILE=$1
LINES=$2

if [[ ! -f $FILE ]]
then
echo "$FILE does not exist"
exit 1;
fi

if [[ -z $LINES ]]
then
LINES=1500000
fi

PREFIX=`echo $FILE | cut -d'.' -f1`
SUFFIX=`echo $FILE | cut -d'.' -f2`
TMPNAME="$PREFIX$SUFFIX."
split -d -l $LINES "$FILE" "$TMPNAME"
mv "$FILE" "$FILE.orig"
echo "Renamed $FILE to $FILE.orig"

for F in $TMPNAME*
do
if [[ ! ( $F =~ ^.*csv$ || $F =~ ^.*processed$ ) ]]
then
N="$F.$SUFFIX.csv"
mv $F "$N"
echo "Created $N"
fi
done
