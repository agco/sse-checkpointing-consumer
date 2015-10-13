# SSE Checkpointing Consumer

SSE Checkpointing Consumer implements a stream-based consumer for Server-sent
events with guarantees of hook success with automatic retries and stream
pausing. It optionally uses Redis to store checkpoint messages to be recalled
when creating the stream.

## Example
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

## Methods


