# SSE Checkpointing Consumer

SSE Checkpointing Consumer implements a **reliable** Nodejs stream-based consumer for **Server-sent
events**. Upon receiving an event it's dispatched to a user provided eventHook function which will be retried until successful, failures are re-offered after an incremental back-off period. 

**At-least-once processing** is ensured with a **persistent checkpointing** mechanism, every x messages the id of the last processed message is written to Redis. When the process crashes and subsequently restarts the checkpointed value is passed on to the user provided createSSEStream function, which then can use the value to resume the stream with a last-event-id header.  

## Usage
```javascript
 var Consumer = require('sse-checkpointing-consumer');

 new Consumer().consume(createStream)
 	.onEvent(eventHook)
	.checkpoint(checkpointOptions);
    	
 // must return a readable stream that follows the Server-Sent Events spec : http://www.w3.org/TR/eventsource/
 function createSSEStream(lastEventId) {
 	// in order to resume the lastEventId parameter should be used as a last-event-id header when requesting the stream
 }
 
 // eventHook is called with a single event chunk
 // will be retried until hook returns a successful promise or truthy value
 function eventHook(chunk) {
 }
 
 var checkpointOptions = {
 	redisUrl: 'tcp://localhost:6379', // a redis url which specifies host, port and optionally credentials
	messages: 1 // checkpoint rate. e.g. 1: checkpoint after 1 message processed, 5: checkpoint after 5 messages processed
 }
   	
```


## Example - Bridge SSE to AMQP
```javascript
var Consumer = require('sse-checkpointing-consumer'),
    request = require('request');

var jackrabbit = require('jackrabbit');
var rabbit = jackrabbit('amqp://guest:guest@localhost:5672);

var exchange = rabbit.topic('change.events');

new Consumer()
    .consume(function createSSEStream(lastEventId) {
        return grabToken().then(function(token) {
            return request({
                uri: 'http://myapi.example.com/changes/stream?resources=trackingData,equipment',
                headers: {'Authorization': 'Bearer ' + token, 'last-event-id': lastEventId}
            });
        })
    })
    .onEvent(function eventHook(sse) {
        return new Promise(function (resolve) {
            exchange.publish({text: sse.data}, {key: sse.event}).on('drain', resolve);
        })
    })
    .checkpoint({redisUrl: 'redis://guest:guest@localhost:10242', messages: 5});

function grabToken() {
    // fetch a token from the oauth2 authorization server
}
```

