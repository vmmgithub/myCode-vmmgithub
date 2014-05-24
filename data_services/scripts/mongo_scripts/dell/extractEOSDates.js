var filter = {
	'systemProperties.tenant': 'dell', 
	'systemProperties.expiredOn': ISODate('9999-01-01T00:00:00Z'),
	type: "app.product/covered", 
	"extensions.tenant.systemType.value.name": "enterprise",  
};

var getUID = function(doc, type) {
  type = type || 'UID';
  var id;

  if (doc && doc.externalIds) {
    doc.externalIds.forEach(function(xid) {
       if (xid.schemeId.name == type) id = xid.id;
    });
  }
  return id;
}

print("var keys = [")
db.app.products.find(filter).forEach(function(doc){
	print('"'+ doc._id.valueOf()+ '",');
});

print ("];");

print("var ids = [")
db.app.products.find(filter).forEach(function(doc){
	print('"'+ getUID(doc)+ '",');
});
print ("];");
