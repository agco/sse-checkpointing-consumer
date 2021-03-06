var hl = require('highland');
var Promise = require('bluebird');
var redis = require('then-redis');
var retry = require('retry');
var stream = require('stream');
var _ = require('lodash');

function consume(streamConstructor) {
    if (!(streamConstructor instanceof Function)) throw new Error('Module requires function');
    this.streamConstructor = streamConstructor;
    return this;
}

function onEvent(dataHandler) {
    if (!this.streamConstructor) throw new Error("A stream constructor must be supplied first");
    this.eventHook = dataHandler;
    return this;
}

function checkpoint(options) {
    if (typeof options === 'undefined') throw new Error("You must supply options");

    if (options) {
        this.messageLimit = options.messages || 3;
        this.checkpointHook = options.callback;
        this.redisClient = redis.createClient(options.redisUrl);
    }

    var consumer = this;
    var dataBuf = '';
    var event = '';
    var id = '';


    var streamOp = retry.operation(consumer.retryOptions);
    streamOp.attempt(attemptStream);
    return this;

    function attemptStream() {
        consumer.stream = getCheckpointId(consumer.streamConstructor)
            .spread(createHighlandStream)
            .then(initiateStream);
    }

    function getCheckpointId(constructor) {
        if (consumer.redisClient) {
            return consumer.redisClient.get('checkpoint')
                .then(function(checkpoint) {
                    var id = checkpoint ? JSON.parse(checkpoint).id : undefined;
                    return Promise.resolve([id, constructor]);
                });
        }
        return Promise.resolve([undefined, constructor]);
    }

    function initHlStreamWithCloseHandler(tryStream) {
        var hlStream = hl(tryStream);
        tryStream.on('close', function () {
            console.error('connection socket has unexpectedly closed, reinitiating the stream')
            hlStream.destroy()
            retryStream()
        });
        return hlStream;
    }

    function createHighlandStream(id, streamConstructor) {
        var tryStream;
        try {
            tryStream = streamConstructor(id);
        } catch(err) {
            throw new Error("Could not establish SSE stream", err);
        }

        if (tryStream instanceof stream) {
            return Promise.resolve(initHlStreamWithCloseHandler(tryStream));
        }
        if (typeof tryStream.then === 'function') {
            return tryStream.then(function(foo) {
                return Promise.resolve(hl(initHlStreamWithCloseHandler(foo)));
            });
        }
        throw new Error("Constructor did not return a readable stream");
    }

    function initiateStream(stream) {
        consumer.stream = stream;
        consumer.stream.resume();

        return consumer.stream
            .stopOnError(retryStream)
            .through(hl.consume(parseJSON))
            .through(hl.consume(tryCallback))
            .each(createRedis)
    }

    function parseJSON(err, line, push, next) {
        var line = line.toString().replace('\n', '');
        if (line === '') {
            if (!dataBuf) return next();
            var data = dataBuf;
            dataBuf = '';
                push(null, {
                    id: id,
                    event: event,
                    data: data
                });
            return next()
        }
        if (line.indexOf('event: ') === 0) event = line.slice(6);
        if(event === ' ticker') return next();
        if (line.indexOf('data: ') === 0) dataBuf += (dataBuf ? '\n' : '') + line.slice(6);
        if (line.indexOf('id: ') === 0) id = line.slice(4);
        next();
    }

    function tryCallback(err, chunk, push, next) {
        if (!consumer.eventHook) {
            push(err, chunk);
            return next();
        }

        var operation = retry.operation(consumer.retryOptions);
        return operation.attempt(function() {
            var hookResult = consumer.eventHook(chunk);
            if (hookResult && hookResult.then) {
                return hookResult
                    .then(function(result) {
                        if (operation.retry(!result)) return;
                        push(err, chunk);
                        return next();
                    })
                    .catch(function(err) {
                        operation.retry(err);
                    });
            } else if (hookResult) {
                push(err, chunk);
                return next();
            } else {
                operation.retry(new Error('Failed event hook'));
            }
        });

    }

    function createRedis(chunk) {
        consumer.lastProcessed = Date.now();
        consumer.messageCount++;
        if (consumer.redisClient && (consumer.messageCount >= consumer.messageLimit)) {
            consumer.messageCount = 0;
            var newCheckpoint = JSON.stringify({lastProcessed: consumer.lastProcessed, id: chunk.id});
            return consumer.redisClient.set('checkpoint', newCheckpoint)
                .then(function() {
                    if (typeof consumer.checkpointHook === 'function')
                        return consumer.checkpointHook(newCheckpoint);
                });
        }
    }

    function retryStream() {
        streamOp.retry(new Error('Failed connection'));
    }
}

function getLastProcessedTime() {return this.lastProcessed;}

module.exports = function(retryOptions) {
    this.retryOptions = {
        retries: 10000,
        minTimeout: 50,
        maxTimeout: 2000,
        randomize: true
    };
    _.assign(this.retryOptions, retryOptions);
    this.messageCount = 0;
    this.checkpoint = checkpoint;
    this.consume = consume;
    this.onEvent = onEvent;
    this.getLastProcessedTime = getLastProcessedTime;
};
