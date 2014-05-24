load('./asset_ids.js');

var getDL = function() {
var d = 
{
	displayName : "Relink Service Assets for Offers",
	_id: ObjectId(),
	"dq" : [ ],
	"externalIds" : [ ],
	"inputSummary" : {
		"collectionSummary" : [
			{
				"collectionName" : "app.assets",
				"numberRecords" : limit,
				"duplicateRecords" : 0,
				"actionRecordsCount" : {
					"linker" : "0"
				},
				"qRankRecordsCount" : {
					"0" : "0",
					"1" : "0",
					"2" : "0",
					"3" : "0",
					"4" : "0"
				},
				"_id" : ObjectId("51a14a6a0138de770d29eec9")
			}
		],
		"dataSourceName" : "Relink"
	},
	"startTime" : ISODate(),
	"progress" : {
	},
	"relationships" : {
		"dataloadConfig" : {
			"relation" : {
				"displayName" : "Data Load Config",
				"key" : "50ac01a21f9c0c0000000748",
				"type" : "core.lookup",
				"name" : "dataloadConfig"
			},
			"targets" : [
				{
					"_id" : ObjectId("50adb42aefaf7bc24c023327"),
					"key" : "50ac01ec1f9c0c0000000e4c",
					"displayName" : "Asset Linker",
					"type" : "app.dataload.config"
				}
			],
			"firstTarget" : "Successor Asset Config"
		}
	},
	"status" : {
		"key" : "50ac01a41f9c0c0000000a9d",
		"displayName" : "Pending",
		"type" : "app.lookup",
		"name" : "pending"
	},
	"systemProperties" : {
		"createdBy" : "bill.moor@dell.com",
		"createdOn" : ISODate(),
		"expiredOn" : ISODate("9999-01-01T00:00:00Z"),
		"lastModifiedBy" : "bill.moor@dell.com",
		"lastModifiedOn" : ISODate(),
		"qRank" : 4,
		"revisionId" : 3,
		"tenant" : "dell"
	},
	"tags" : [ ],
	"type" : "app.dataload"
};

return d;
};

var saveDL = function(dl) {
 dl.systemProperties.lastModifiedOn = new ISODate();
 dl.loadEndTime = new ISODate();
 db.app.dataloads.insert(dl);
};

var i = 0;
var limit = 20000;
var dl;

ids.forEach(function(id) {
if (i++ % limit == 0 || i == 0) {
	if (!dl) {
		dl = getDL();
	} else {
		saveDL(dl);
		dl = getDL();
	}
}

if (i % 1000 == 0) print ('Completed ' + i + ' records with ' + dl._id.valueOf());

db.app.assets.find({'_id': ObjectId(id)}).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(function(d) {
	d.externalIds[1].id = dl._id.valueOf();
	db.app.assets.update({_id: d._id}, {$set: {externalIds: d.externalIds, 'systemProperties.dlOn': ISODate()}});
});

});

saveDL(dl);

db.getLastError();
