while read bu
do
now=`date +"%DT%T"`
echo "[${now}] Processing ${bu}"
# ./convertAndAttach.sh emea ${bu} Unassigned upload
./convertAndAttach.sh emea ${bu} Unassigned both 
done < emeaBU.lst
