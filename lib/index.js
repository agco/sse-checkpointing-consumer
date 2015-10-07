var hl = require('highland');
var Promise = require('bluebird');
var redis = require('then-redis');
var retry = require('retry');
var stream = require('stream');

function checkpoint(options) {
    if (!options) throw new Error("You must supply options");
    this.messageLimit = options.messages || 3;
    this.redisClient = this.redisClient || redis.createClient(this.redisUrl);
    return this;
}

function consume(streamConstructor) {
    if (!(streamConstructor instanceof Function)) throw new Error('Module requires function');

    var consumer = this;
    var dataBuf = '';
    var event = '';
    var id = '';

    this.stream = getCheckpointId(streamConstructor)
        .spread(createHighlandStream)
        .then(initiateStream);
    return consumer;

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

    function createHighlandStream(id, streamConstructor) {
        var tryStream;
        try {
            tryStream = streamConstructor(id);
        } catch(err) {
            throw new Error("Could not establish SSE stream", err);
        }

        if (tryStream instanceof stream) return Promise.resolve(hl(tryStream));
        if (typeof tryStream.then === 'function') {
            return tryStream.then(function(foo) {
                return Promise.resolve(hl(foo));
            });
        }
        throw new Error("Constructor did not return a readable stream");
    }

    function initiateStream(stream) {
        this.stream = stream;
        this.stream.resume();

        return this.stream
            .through(hl.consume(parseJSON))
            .through(hl.consume(tryCallback))
            .each(createRedis);
    }

    function parseJSON(err, line, push, next) {
        var line = line.toString().replace('\n', '');
        if (line === '') {
            if (!dataBuf) return;
            var data = dataBuf;
            dataBuf = '';
            push(null, {
                id : id,
                event : event,
                data : data
            });
            return next()
        }
        if (line.indexOf('data: ') === 0) dataBuf += (dataBuf ? '\n' : '') + line.slice(6);
        if (line.indexOf('id: ') === 0) id = line.slice(4);
        if (line.indexOf('event: ') === 0) event = line.slice(6);
        next();
    }

    function tryCallback(err, chunk, push, next) {
        if (!consumer.eventHook) {
            push(err, chunk);
            return next();
        }

        var operation = retry.operation({
            retries: 10000,
            minTimeout: 50,
            maxTimeout: 2000,
            randomize: true
        });
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
            return consumer.redisClient.set('checkpoint', newCheckpoint);
        }
    }
}

function onEvent(dataHandler) {
    if (!this.stream) throw new Error("A stream must be consumed first");
    this.eventHook = dataHandler;
    return this;
}

function getLastProcessedTime() {return this.lastProcessed;}

module.exports = function(redisUrl) {
    this.messageCount = 0;
    this.redisClient = redis.createClient(redisUrl);
    this.checkpoint = checkpoint;
    this.consume = consume;
    this.onEvent = onEvent;
    this.getLastProcessedTime = getLastProcessedTime;
};
