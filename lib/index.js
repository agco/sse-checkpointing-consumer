var redis = require('then-redis');

function checkpoint(options) {
    if (!options) throw new Error("You must supply options");
    if (!options.redisUrl) throw new Error("You must supply a Redis url");
    this.messageLimit = options.messages || 3;
    this.redisClient = this.redisClient || redis.createClient(options.redisUrl);
    return this;
}

function consume(streamConstructor) {
    if (!(streamConstructor instanceof Function)) throw new Error('Module requires function');

    try {
        this.stream = streamConstructor()
    } catch(err) {
        throw new Error("Could not establish SSE stream", err);
    }

    stream.on('data', handleEvent);
    return this;
}

function onEvent(dataHandler) {
    if (!this.stream) throw new Error("A stream must be consumed first");
    console.log(this.stream);
    if (!this.stream.on ||
        !(typeof this.stream.on === 'function')) throw new Error("The stream must have an `on` listener");

    this.eventHooks.push(dataHandler);
    return this;
}

function handleEvent(data) {
    var eventPromises = this.eventHooks.map(function(hook){return hook(data)});
    this.messageCount++;
    if (this.redisClient &&
        (this.messageCount >= this.messageLimit)) {
        return
    }
}

module.exports = function() {
    this.messageCount = 0;
    this.eventHooks = [];
    this.checkpoint = checkpoint;
    this.consume = consume;
    this.onEvent = onEvent;
};
