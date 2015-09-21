

```javascript

var consumer = require('sse-checkpointing-consumer'),
    request = require('request');

var redis;
// initialise redis

consumer
    .consume(function makeSSEStream(lastEventId) {
        grabToken().then(function(token) {
            return request({
                uri: 'http://agco-telemetry-api.herokuapp.com/changes/stream?resources=trackingData,equipment',
                headers: {'Authorization': 'Bearer ' + token, 'last-event-id': lastEventId}
            });
        })
    })
    .onEvent(function (sse) {
        return new Promise(function (resolve) {
            exchange.publish({text: sse.data}, {key: sse.event}).on('drain', resolve);
        })
    })
    .checkpoint(redis, {time: '10s', messages: 5});

function grabToken() {
    // fetch a token from the oauth2 authorization server
}

```