if [[ "$1" == "" ]]; then
   echo "Usage: $0 <tenant> [startDate] [endDate] [salesStages]"
   exit 1
fi

tenant=$1
startDate=$2
endDate=$3
salesStages=$4
output="../../reports"
log="../../reports/${tenant}.gainShare.out"

if [[ -z $startDate ]] 
then
   startDate='undefined'
fi

if [[ -z $endDate ]] 
then
   endDate='undefined'
fi

if [[ -z $salesStages ]] 
then
   salesStages='undefined'
fi

mongo testdata --quiet gainShare.js --eval "var tenant = '${tenant}'; var startDate = '${startDate}'; var endDate = '${endDate}'; var salesStages = '${salesStages}';"   >> ${log} 
