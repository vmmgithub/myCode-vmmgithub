tenant="$1"
prod02="prod02dl-int.ssi-cloud.com"
prod02dl2="prod02dl2-int.ssi-cloud.com"
uat02="uat02dl-int.ssi-cloud.com"
dellprd="dell-prd1dl4-int.ssi-cloud.com"
config="config.ssi-cloud.com"
configt2="config-t2.ssi-cloud.com"
stgcurrent="stgcurrent.ssi-cloud.com"
stgnext="stgnext.ssi-cloud.com"

case $1 in
bluecoat | siemens | bazaarvoice | nielsen | ibm | avispl | google | jci | btinet )
url=$prod02
;;
pki | polycom)
url=$prod02
;;
projectcristal | perkinelmer)
url=$config
;;
aspect | ariasystems)
url=$uat02
;;
dell)
url=$dellprd
;;
cisco)
url=$prod02
;;
blackboard)
url=$configt2
;;
juniper)
url=$prod02
;;
*)
echo "Missing or invalid tenant name"
exit 1
;;
esac
