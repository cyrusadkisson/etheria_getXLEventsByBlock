const AWS = require('aws-sdk');
const dynamoDB = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event, context, callback) => {
    console.log("event=" + JSON.stringify(event));

    // event.params.querystring.version is guaranteed by API gateway

    return new Promise((resolve, reject) => {

        if (event.params.querystring.tile) { // specific tile

            var params = {
                TableName: 'ExchangeXLActivity2',
                IndexName: 'tileIndexAndVersion-blockNumberAndTxIndex-index',
                KeyConditionExpression: 'tileIndexAndVersion = :hkey and blockNumberAndTxIndex > :rkey',
                ExpressionAttributeValues: {
                    ':hkey': event.params.querystring.tile + "v" + event.params.querystring.version,
                    ':rkey': 0
                },
                ScanIndexForward: false
            };

            dynamoDB.query(params, function(err, reverseQueryData) {
                if (err) {
                    console.log("Error", err);
                    reject(err);
                }
                else {
                    console.log("db query was not erroneous. reverseQueryData=" + JSON.stringify(reverseQueryData));
                    resolve(reverseQueryData.Items);
                }
            });
        }
        else { // exchange events for all tiles

            var params2 = {
                TableName: 'ExchangeXLActivity2',
                // IndexName: 'tileIndexAndVersion-blockNumberAndTxIndex-index',
                KeyConditionExpression: 'version = :hkey and blockNumberAndTxIndex > :rkey',
                ExpressionAttributeValues: {
                    ':hkey': event.params.querystring.version,
                    ':rkey': 0
                },
                ScanIndexForward: false
            };

            dynamoDB.query(params2, function(err, reverseQueryData) {
                if (err) {
                    console.log("Error", err);
                    reject(err);
                }
                else {
                    console.log("db query was not erroneous. reverseQueryData=" + JSON.stringify(reverseQueryData));
                    resolve(reverseQueryData.Items);
                }
            });
            
        }

    });
};
