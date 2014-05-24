var csv = require("ya-csv");

function CsvHelper() {

    var columns = [];

    return {
        setColumnNames: function (names) {
            columns = names;
        },
        read: function (path, callback) {
            var csvData = [];
            var headers = [];
            var reader = csv.createCsvFileReader(path, {
                columnsFromHeader: false
            });
            
            if (columns.length) {
                reader.setColumnNames(columns);
            }
            reader.addListener('data', function (data) {
                if (headers.length == 0) {
                    headers = data
                } else if (data && data.length > 0) {
                    var item = {};
                    item[data[0]] = data[1];
                    csvData.push(item);
                }
            });

            reader.addListener("end", function () {
                callback(csvData);
            });
        },
        readAsObj: function (path, callback) {
            var csvData = [];
            var reader = csv.createCsvFileReader(path, {
                columnsFromHeader: true
            });
            
            
            if (columns.length) {
                reader.setColumnNames(columns);
            }
            reader.addListener('data', function (data) {
                csvData.push(data);
            });


            reader.addListener("end", function () {
                callback(csvData);
            });
        },
        readAsync: function (path, columnsFromHeader, dataCallback, endCallback) {
            var reader = csv.createCsvFileReader(path, {
                columnsFromHeader: (columnsFromHeader == true) ? true : false
            });
            reader.addListener('data', function (data) {
                dataCallback(data, reader);
            });

            reader.addListener("end", function () {
                endCallback();
            });
        }
    }
}

module.exports = CsvHelper;
