var tenants = [
    "dell"
];

var now = ISODate().valueOf();
var baseDate = ISODate();
baseDate.setHours(0); 
baseDate.setMinutes(0);
baseDate.setSeconds(0);
baseDate.setMilliseconds(0);
baseDate.setDate(1);

// END THE FUTURE 12TH MONTH
var from = ISODate(baseDate.toISOString());
from.setFullYear(from.getFullYear()-1);

// START OF THE PAST 12TH MONTH
var to = ISODate(baseDate.toISOString());
to.setFullYear(to.getFullYear()+1);
to.setMonth(to.getMonth()+1);

tenants.forEach(function(t) {
    var filter = {
            'systemProperties.tenant' : t,
            'systemProperties.expiredOn' : ISODate("9999-01-01T00:00:00Z"),
            targetDate: {
                $gte: from, 
                $lt: to
            },
            'flows.salesStages.state.name': {$nin: ['transitioned', 'transition']},
            isSubordinate: false,
        },
        project = {'amount.normalizedAmount.amount': 1, 'targetAmount.normalizedAmount.amount': 1, targetDate: 1};

    // Option 1: Find
/*  
    var aAmount = 0,
        oPastAmount = 0,
        oPastTargetAmount = 0,
        oFutureAmount = 0,
        oFutureTargetAmount = 0,        
        oCurr = null;

    db.app.opportunities.find({filter, project).forEach(function(opp) {
        if (opp.targetDate.valueOf() <= now) {
            oPastAmount += opp.amount.normalizedAmount.amount;
            oPastTargetAmount += opp.targetAmount.normalizedAmount.amount;
        } else {
            oFutureAmount += opp.amount.normalizedAmount.amount;
            oFutureTargetAmount += opp.targetAmount.normalizedAmount.amount;
        }
    });

    print(t + " " + aAmount + " " + oPastAmount + " " + oPastTargetAmount + " " + oFutureAmount + " " + oFutureTargetAmount);
*/

    // Option 2: Aggregate
    var res = db.app.opportunities.aggregate([
        {
            $match: filter
        },
        {
            $project: project
        },
        {
            $project: {
                preAmt: {$cond: [{ $lte: [ "$targetDate", now ] }, "$amount.normalizedAmount.amount", 0] },
                preTAmt: {$cond: [{ $lte: [ "$targetDate", now ] }, "$targetAmount.normalizedAmount.amount", 0] },
                posAmt: {$cond: [{ $gt: [ "$targetDate", now ] }, "$amount.normalizedAmount.amount", 0] },
                posTAmt: {$cond: [{ $gt: [ "$targetDate", now ] }, "$targetAmount.normalizedAmount.amount", 0] },
            }
        }, 
        {
            $group: {
                _id: "$systemProperties.tenant",
                oPastAmount: {$sum: "$preAmt"},
                oPastTargetAmount: {$sum: "$preTAmt"},
                oFutureAmount: {$sum: "$posAmt"},
                oFutureTargetAmount: {$sum: "$posTAmt"},
            }
        }
    ]);

    var r = res && res.result && res.result[0];
    print(t + " " + 0 + " " + r.oPastAmount + " " + r.oPastTargetAmount + " " + r.oFutureAmount + " " + r.oFutureTargetAmount);

});

