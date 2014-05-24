tenant=$1
mongo testdata --quiet runDQByCollection.js --eval "var tenant='${tenant}'; var coll='app.assets'; var captureDetails=true"
