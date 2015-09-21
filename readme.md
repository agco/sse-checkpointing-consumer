

```javascript

var consumer = require('sse-checkpointing-consumer'),
    request = require('request');

var rabbit;
// initialise jackrabbit

var exchange = rabbit.topic('change.events');

consumer
    .consume(function makeSSEStream(lastEventId) {
        grabToken().then(function(token) {
            return request({
                uri: 'http://myapi.example.com/changes/stream?resources=trackingData,equipment',
                headers: {'Authorization': 'Bearer ' + token, 'last-event-id': lastEventId}
            });
        })
    })
    .onEvent(function (sse) {
        return new Promise(function (resolve) {
            exchange.publish({text: sse.data}, {key: sse.event}).on('drain', resolve);
        })
    })
    .checkpoint({redisUrl: 'redis://someuser:secret@localhost:10242', time: '10s', messages: 5});

function grabToken() {
    // fetch a token from the oauth2 authorization server
}

// monitor stream health
setTimeout(function () {
    var lastProcessedTime = consumer.getLastProcessedTime();
    // report current time - lastProcessedTime to new relic
}, 30000);

```