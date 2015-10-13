# SSE Checkpointing Consumer

SSE Checkpointing Consumer implements a stream-based consumer for Server-sent
events with guarantees of hook success with automatic retries and stream
pausing. It optionally uses Redis to store checkpoint messages to be recalled
when creating the stream.

## Example
```javascript
var Consumer = require('sse-checkpointing-consumer'),
    consumer = new Consumer(),
    request = require('request');

var rabbit;
// initialise jackrabbit

var exchange = rabbit.topic('change.events');

consumer
    .consume(function makeSSEStream(lastEventId) {
        return grabToken().then(function(token) {
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
    .checkpoint({redisUrl: 'redis://someuser:secret@localhost:10242', messages: 5});

function grabToken() {
    // fetch a token from the oauth2 authorization server
}
```

## Usage
```javascript
 var Consumer = require('sse-checkpointing-consumer');

 //must return a readable stream that follows EventSource spec
 function createStream(lastCheckpointId) {
 }
 
 //eventHook is called with a single event chunk
 //will be retried until hook returns a successful promise or truthy value
 function eventHook(chunk) {
 }
 
 var consumer = new Consumer();
 
 consumer.consume(createStream)
 	.onEvent(eventHook)
    .checkpoint({
    	redisUrl: 'tcp://localhost:6379',
	    messages: 1,
    	callback: checkCheckpoint
    });
```
