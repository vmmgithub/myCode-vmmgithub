BUID="$1"

if [[ -z $BUID ]]
then
echo "Usage: $0 <buid>"
exit 1
fi

BASE="../../reports/$BUID"

cat  ${BASE}.offers.out | cut -f2,6,10 | grep undefined | cut -f3 | sort | uniq > ${BASE}.oppsOnUnassignedOffers.tmp
grep -F -f ${BASE}.oppsOnUnassignedOffers.tmp ${BASE}.opps.out | cut -f1,4 | grep -v undefined > ${BASE}.oppsWithReps.txt
rm ${BASE}.oppsOnUnassignedOffers.tmp

cat ${BASE}.oppsWithReps.txt | cut -f1 > ${BASE}.oppsWithReps.tmp
grep -F -f ${BASE}.oppsWithReps.tmp ${BASE}.offers.out | cut -f2,6,10 | grep undefined > ${BASE}.problemOffersWithOutReps.txt
count=`wc -l ${BASE}.problemOffersWithOutReps.txt | cut -d' ' -f1`
rm ${BASE}.oppsWithReps.tmp

echo "$count unassigned offers that should have been assigned."



