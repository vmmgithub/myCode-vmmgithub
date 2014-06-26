tenants=("aria" "aspect" "avispl" "bazaarvoice" "blackboard" "bluecoat" "btinet" "cisco" "dell" "google" "guidance" "ibm" "juniper" "nielsen" "perkinelmer" "polycom" "sap" "servicesource" "siemens" "vocus" "workday")

user="Bill Moor"
login="bruce.lewis"
pawd="passwordone"

for tenant in "${tenants[@]}"; do
	cmd="./listUsers.js -h prod02dl-int.ssi-cloud.com --user ${login}@${tenant}.com  --password ${pawd} --tenant ${tenant} --searchBy '{\"displayName\": \"${user}\"}'"
	#o=`${cmd}`
	echo ${tenant} ${cmd}
done
