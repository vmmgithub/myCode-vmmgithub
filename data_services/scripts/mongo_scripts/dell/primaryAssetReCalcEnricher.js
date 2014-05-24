load('./uniq.tags.js')

var setUID = function(doc, type, id) {
  type = type || 'UID';

  if (doc && doc.externalIds) {
    doc.externalIds.forEach(function(xid) {
       if (xid.schemeId.name == type) xid.id = id;
    });
  }
}

var newDlId = ObjectId();
var dlStart = ISODate();
var dlName = 'Primary Asset Recalc';

var dlConfig = db.app.dataload.configs.findOne({displayName: /Successor/});
  dlConfig = {
    key: dlConfig._id.valueOf(),
    displayName: dlConfig.displayName,
    type: dlConfig.type
  };

var i = 0;
var updated = 0;
tags.forEach(function(tag) {
	if (i++ % 1000 == 0) print('[' + ISODate()+ '] done updating tags ' + i + ' and ' + updated);

	var first = true;
	db.app.assets.find({
	    "systemProperties.tenant" : "dell",
	    "systemProperties.expiredOn" : ISODate("9999-01-01:00:00:00Z"),
	    "type" :"app.asset/service",
	    "extensions.master.serialNumber.value": tag.id,
	    "extensions.tenant.primary.value": true
	}).sort({'systemProperties.dlOn': -1, 'systemProperties.createdOn': -1}).forEach(function(asset) {
		updated++ ;
		if (first) {
			setUID(asset, newDlId);
		    asset.systemProperties.dlOn = ISODate();
		    asset.systemProperties.qRank = 0;
			first = false;
		} else {
			asset.extensions.tenant.primary.value = false;
		}
	    db.app.assets.save(asset);

	});
});

if (updated != 0) {
	var endTime = ISODate();
	var dataload = {
	  '_id' : newDlId,
	  'displayName' : dlName,
	  'inputSummary' : {
	    'collectionSummary' : [
	      {
	        'collectionName' : 'app.assets',
	        'numberRecords' : tags.length
	      }
	    ],
	    'dataSourceName' : 'Primary Asset Manual Retry'
	  },
	  'relationships' : {
	    'dataloadConfig' : {
	      'firstTarget' : dlConfig.displayName,
	      'relation' : {
	        'type' : 'core.lookup',
	        'displayName' : 'Data Load Config',
	        'name' : 'dataloadConfig',
	        'key' : '50ac01a21f9c0c0000000748'
	      },
	      'targets' : [
	        dlConfig
	      ]
	    }
	  },
	  'startTime' : dlStart,
	  'endTime' : endTime,
	  'loadEndTime' : endTime,
	  'status' : {
	    'key' : '50ac01a41f9c0c0000000a9d',
	    'displayName' : 'Pending',
	    'type' : 'app.lookup',
	    'name' : 'pending'
	  },
	  'systemProperties' : {
	    'createdBy' : 'bill.moor@dell.com',
	    'createdOn' : dlStart,
	    'expiredOn' : ISODate('9999-01-01T00:00:00Z'),
	    'lastModifiedBy' : 'bill.moor@dell.com',
	    'lastModifiedOn' : endTime,
	    'qRank' : 4,
	    'revisionId' : 1,
	    'tenant' : 'dell'
	  },
	  'dq' : [ ],
	  'externalIds' : [ ],
	  'keywords' : [
	    dlName.toLowerCase()
	  ],
	  'tags' : [ ],
	  'type' : 'app.dataload'
	};

	db.app.dataloads.insert(dataload);

	print('DL_ID ' + newDlId.valueOf() + ' for ' + tags.length + ' records');

}

db.getLastError();

