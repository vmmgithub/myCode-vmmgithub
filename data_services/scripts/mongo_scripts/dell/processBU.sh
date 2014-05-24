BUID="$1"
#./continousRelink.sh core.contacts $BUID Organization 'core.contact/organization' > "../../reports/$BUID.contacts.continous.log"
./continousRelink.sh app.assets $BUID Asset 'app.asset/covered' > "../../reports/$BUID.assets.covered.continous.log"
./continousRelink.sh app.assets $BUID Asset 'app.asset/service' > "../../reports/$BUID.assets.service.continous.log"

