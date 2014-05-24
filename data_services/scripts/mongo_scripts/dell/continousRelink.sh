for LIMIT in "500000" "1000000" "2000000" "3000000" "4000000" "5000000" "6000000" "7000000" "8000000" "9000000" "10000000" "11000000" "12000000"
do
	dt=`date`
	echo "[$dt] Starting iteration $iter with $LIMIT ... "
	./relinkAll.sh $1 $2 $3 $4 $LIMIT
	dt=`date`
	echo "[$dt] Completed iteration $iter with $LIMIT  "
done
