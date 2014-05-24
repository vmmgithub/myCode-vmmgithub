var filter = {
    'systemProperties.tenant': 'dell',
    'systemProperties.expiredOn': ISODate('9999-01-01'),
    'systemProperties.createdOn': {$gt: ISODate('2013-07-01')},
    'status.name': {$in: ['pending', 'inProgress']}
};

db.app.dataloads.find(filter).addOption(DBQuery.Option.noTimeout).readPref('secondary').forEach(function(doc) {
	var dlId = doc._id.valueOf();

	var expected = doc.inputSummary && doc.inputSummary.collectionSummary && doc.inputSummary.collectionSummary[0] && doc.inputSummary.collectionSummary[0].numberRecords;
	var cName = doc.inputSummary && doc.inputSummary.collectionSummary && doc.inputSummary.collectionSummary[0] && doc.inputSummary.collectionSummary[0].collectionName;

	if (!expected || cName) print('[ERROR] Issue with collectionSummary on ' + dlId + ' ' + doc.displayName);

	var actual = db[cName].count({'externalIds.id': dlId, 'systemProperties.expiredOn': ISODate('9999-01-01')});

	if (actual < expected) 
		print('[ERROR] Mismatch on record count ' + dlId + ' ' + doc.displayName);
	else
		print('[SUCCESS] Everything good on ' + dlId + ' ' + doc.displayName);
});

