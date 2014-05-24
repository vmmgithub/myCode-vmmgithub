load('./helper.js');
load('./moment.js');

var tenants = [
    "ibm",
    "guidance",
    "jci",
    "btinet",
    "bazaarvoice",
    "aspect",
    "siemens",
    "avispl",
    "nielsen",
    "dell"
];

var from = moment().subtract({months: 12}).startOf('month').format();
var to = moment().add({months: 12}).endOf('month').format();
var now = ISODate().valueOf();

tenants.forEach(function(t) {
    var aAmount = 0,
    	oPastAmount = 0,
    	oPastTargetAmount = 0,
        oFutureAmount = 0,
        oFutureTargetAmount = 0,        
    	oCurr = null;

    /*
    db.app.assets.find({
        "systemProperties.tenant" : t,
		"systemProperties.expiredOn" : ISODate("9999-01-01T00:00:00Z"),
		type: "app.asset/service"
    }, {amount: 1}).forEach(function(opp) {
        aAmount += opp.amount.normalizedAmount.amount;
    });
    */
    
    db.app.opportunities.find({
        "systemProperties.tenant" : t,
		"systemProperties.expiredOn" : ISODate("9999-01-01T00:00:00Z"),
		targetDate: {
            $gte: ISODate(from), 
            $lte: ISODate(to)
        },
        '$or': [
            {isSubordinate: false}, 
            {isSubordinate: {$exists: false}}
        ]
    }, {amount: 1, targetAmount: 1, targetDate: 1}).forEach(function(opp) {
		if (!oCurr) oCurr = opp.amount.normalizedAmount.code.displayName;

        if (opp.targetDate.valueOf() < now) {
            oPastAmount += opp.amount.normalizedAmount.amount;
            oPastTargetAmount += opp.targetAmount.normalizedAmount.amount;
        } else {
            oFutureAmount += opp.amount.normalizedAmount.amount;
            oFutureTargetAmount += opp.targetAmount.normalizedAmount.amount;
        }

    });

    print(t + " " + toFixed(aAmount) + " " + toFixed(oPastAmount) + " " + toFixed(oPastTargetAmount) 
            + " " + toFixed(oFutureAmount) + " " + toFixed(oFutureTargetAmount) 
            + " " + oCurr);
});
