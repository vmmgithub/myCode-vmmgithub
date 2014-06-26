var transactionalCollections = ["app.assets", "app.bookings", "app.offers", "app.opportunities", "app.products", "app.quotes", "app.tasks"];
//echo(process.args);
var collections = db.getCollectionNames();
collections.forEach(function(collection){
    if(transactionalCollections.indexOf(collection) != -1){
        print("Removing collection: " + collection);
        db[collection].remove({"systemProperties.tenant":"dell"});
    }
});