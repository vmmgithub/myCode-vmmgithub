rs.slaveOk();
load(file);
a.forEach(function(item) {
   if (item == '') 
      return;
   docs = db[coll].find({"_id": ObjectId(item), "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z")}).limit(1).hint({"_id": 1});
   var found = false;
   docs.forEach(function(doc) {
      found = true;
   });
   if (!found) {
      print(item);
   }
});
