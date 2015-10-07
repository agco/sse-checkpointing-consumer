var hl = require('highland');
//var lineSplit = require('highland-line-split');
var Promise = require('bluebird');
var redis = require('then-redis');
var retry = require('retry');
var split = require('split2');
var stream = require('stream');

var destroyed = false;

function destroyStream() {
    if (destroyed) return;
    destroyed = true;
    clearTimeout(timeout);
    if (req) req.abort();
    parse.emit('close');
}

function checkpoint(options) {
    if (!options) throw new Error("You must supply options");
    if (!options.redisUrl) throw new Error("You must supply a Redis url");
    this.messageLimit = options.messages || 3;
    this.redisClient = this.redisClient || redis.createClient(options.redisUrl);
    return this;
}

function consume(streamConstructor) {
    var consumer = this;
    var tryStream;
    if (!(streamConstructor instanceof Function)) throw new Error('Module requires function');

    try {
        tryStream = streamConstructor();
    } catch(err) {
        throw new Error("Could not establish SSE stream", err);
    }

    if (!(tryStream instanceof stream)) throw new Error("Constructor did not return a readable stream");

    this.stream = hl(tryStream);
    this.stream.resume();

    var dataBuf = ''
    var event = ''
    var id = ''

    var parseJSON = hl().consume(function(err, line, push, next) {
        var line = line.toString().replace('\n', '');
        if (line === '') {
            if (!dataBuf) return
            var data = dataBuf
            dataBuf = ''
            push(null, {
                id : id,
                event : event,
                data : data
            });
            return next()
        }
        if (line.indexOf('data: ') === 0) dataBuf += (dataBuf ? '\n' : '') + line.slice(6)
        if (line.indexOf('id: ') === 0) id = line.slice(4)
        if (line.indexOf('event: ') === 0) event = line.slice(6)
        next();
    });

    var tryCallback = hl().consume(function(err, chunk, push, next) {
        if (!consumer.eventHook) {
            push(err, chunk);
            return next();
        }

        var operation = retry.operation();
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

    });

    var createRedis = function(chunk) {
        consumer.messageCount++;
        if (consumer.redisClient && (consumer.messageCount >= consumer.messageLimit)) {
            consumer.messageCount = 0;
            var newCheckpoint = JSON.stringify({lastProcessed: Date.now(), id: chunk.id});
            return consumer.redisClient.set('checkpoint', newCheckpoint);
        }
    };

    this.stream
        .through(parseJSON)
        .through(tryCallback)
        .each(createRedis);

    //this.stream
    //    .pipe(parse, {end:false})
    //    //.on('close', destroyStream)
    //    .on('data', handleEvent.bind(this));

    return consumer;
}

function onEvent(dataHandler) {
    if (!this.stream) throw new Error("A stream must be consumed first");
    if (!this.stream.on ||
        !(typeof this.stream.on === 'function')) throw new Error("The stream must have an `on` listener");

    this.eventHook = dataHandler;
    return this;
}

var parse = split(function(line) {
    if (!line) {
        if (!dataBuf) return;
        var data = dataBuf;
        dataBuf = '';

        return {
            id : id,
            event : event,
            data : data
        }
    }
    if (line.indexOf('data: ') === 0) dataBuf += (dataBuf ? '\n' : '') + line.slice(6);
    if (line.indexOf('id: ') === 0) id = line.slice(4);
    if (line.indexOf('event: ') === 0) event = line.slice(6);
    if (line.indexOf('retry: ') === 0) retry = line.slice(6);

});

function handleEvent(data) {
    var consumer = this;
    var eventHook = this.eventHook || function() {};

    return Promise(eventHook(data))
        .then(createCheckpoint)
        .then();

    function createCheckpoint() {
        consumer.messageCount++;
        if (consumer.redisClient &&
            (consumer.messageCount >= consumer.messageLimit)) {
            consumer.messageCount = 0;
            var newCheckpoint = JSON.stringify({lastProcessed: Date.now(), id: data.id});
            return consumer.redisClient.set('checkpoint', newCheckpoint);
        }
    }
}

module.exports = function() {
    this.messageCount = 0;
    this.checkpoint = checkpoint;
    this.consume = consume;
    this.onEvent = onEvent;
};
