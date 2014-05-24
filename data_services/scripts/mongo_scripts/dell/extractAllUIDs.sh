nohup ./extractUIDs.sh "app.products" "app.product/service" &
nohup ./extractUIDs.sh "app.products" "app.product/covered" &
nohup ./extractUIDs.sh "core.contacts" "core.contact/organization" & 
nohup ./extractUIDs.sh "core.contacts" "core.contact/person" & 
nohup ./extractUIDs.sh "core.addresses" "core.address" & 
nohup ./extractUIDs.sh "app.assets" "app.asset/service" &
nohup ./extractUIDs.sh "app.assets" "app.asset/covered" &
