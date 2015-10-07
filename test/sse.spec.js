//modules
var _ = require('lodash');
var chai = require('chai');
var ess = require('event-source-stream');
var EventSource = require('eventsource');
var expect = chai.expect;
var express = require('express');
var jsc = require('jsverify');
var Promise = require('bluebird');
var request = require('request');
var sinon = require('sinon');
var sinonChai = require('sinon-chai');
var sse = require('tiny-sse');
var redis = require('then-redis');

chai.use(sinonChai);

//locals
var app = express();
var Consumer = require('../lib/index.js');
var consumer;
var port = 3000;
var redisUrl = 'tcp://localhost:6379';
var redisClient = redis.createClient(redisUrl);
var streamURL = 'http://localhost:'+port+'/stream';
var streamThreeURL = 'http://localhost:'+port+'/streamThree';
var streamSixURL = 'http://localhost:'+port+'/streamSix';

describe('SSE Checkpointing Consumer module', function() {

    before(function() {
        createStreamRoute(app, '/stream', 1, 'bar');
        createStreamRoute(app, '/streamThree', 3, 'bar');
        createStreamRoute(app, '/streamSix', 6, 'bar');

        var server = app.listen(port, function() {
            var host = server.address().address;
            var port = server.address().port;
            console.log('Test server listening at http://%s:%s', host, port);
        });
    });

    beforeEach(function() {
        consumer = new Consumer();
    });

    describe('#consume', function() {
        it('is a function', function() {
            expect(consumer.consume).to.be.an.instanceOf(Function);
        });

        it('returns the consumer for chaining', function() {
            expect(consumer.consume(createStream(streamURL))).to.equal(consumer);
        });

        it('throws an error if it\'s not passed a function', function() {
            expect(consumer.consume).to.throw(Error, /Module requires function/);
        });

        it('calls the callback', function() {
            var callback = sinon.spy();
            expect(consumer.consume.bind(consumer, callback)).to.throw(Error);
            expect(callback).to.be.calledOnce;
        });

        it('binds the stream to the object', function() {
            consumer.consume(createStream(streamURL));
            expect(consumer.stream.on).to.be.a.function;
        });

        it('catches callback errors and propagates them', function() {
            function willFail() {throw new Error("Cannot connect to stream")}
            var curriedConsume = consumer.consume.bind(consumer, willFail);
            expect(curriedConsume).to.throw(Error, /Could not establish SSE stream/);
        });
    });

    describe('#onEvent', function() {
        describe('when a stream has not been consumed yet', function() {
            it('throws an error', function() {
                expect(consumer.onEvent).to.throw(Error, /A stream must be consumed first/);
            });
        });

        it('calls the onEvent hook', function(done) {
             consumer.consume(createStream(streamURL))
                 .onEvent(function(data) {
                     expect(JSON.parse(data.data).foo).to.equal('bar');
                     expect(data.id).to.equal("0");
                     done();
                     return true;
                 });
        });

        it('returns the original consumer', function() {
            var testConsumer = consumer
                .consume(createStream(streamURL))
                .onEvent(function() {return true;});
            expect(testConsumer).to.equal(consumer);
        });
    });

    describe('#checkpoint', function() {
        beforeEach(function() {
            return redisClient.flushdb();
        });

        it('throws an error if you do not supply a redis url', function() {
            expect(consumer.checkpoint).to.throw(Error, /You must supply options/);
            expect(consumer.checkpoint.bind(consumer, {})).to.throw(Error, /You must supply a Redis url/);
        });

        it('creates a checkpoint after receiving 1 message', function() {
            consumer
                .consume(createStream(streamThreeURL))
                .checkpoint({
                    redisUrl: redisUrl,
                    messages: 1
                });

            return Promise.delay(50).then(function(){
                return redisClient.get('checkpoint')
                    .then(function(checkpoint) {
                        expect(checkpoint).to.not.be.null;
                    });
            });
        });

        it('creates a checkpoint after receiving 3 messages', function() {
            consumer
                .consume(createStream(streamThreeURL))
                .checkpoint({
                    redisUrl: redisUrl,
                    messages: 1
                });

            return Promise.delay(50).then(function(){
                return redisClient.get('checkpoint')
                    .then(function(checkpoint) {
                        expect(JSON.parse(checkpoint).id).to.equal('2');
                    });
            });
        });

        it('allows the message threshold to be set', function() {
            consumer
                .consume(createStream(streamSixURL))
                .checkpoint({
                    redisUrl: redisUrl,
                    messages: 2
                });

            return Promise.delay(50).then(function(){
                return redisClient.get('checkpoint')
                    .then(function(checkpoint) {
                        expect(JSON.parse(checkpoint).id).to.equal('5');
                    });
            });
        });
    });

    describe('#getLastProcessedTime', function() {

    });

    describe('Test various request modules', function() {


        /*
            Request does not parse event data easily, and testing against
            event-emitter will work better here
        */
        it.skip('returns a stream for requestjs', function(done) {
            request.get(streamURL)
                .on('data', function(data) {
                    done();
                });
        });

        it('returns a stream for event-source-stream', function(done) {
            ess(streamURL, {json: true})
                .on('data', function(data) {
                    expect(JSON.parse(data.data).foo).to.equal('bar');
                    done();
                });
        });

        it.skip('works for eventsource', function(done) {
            var es = new EventSource(streamURL);

            es.onmessage = function(event) {
                expect(event.data).to.exist;
                done();
            }
        });
    });
});

function createStream(url) {
    return function() {
        return request(url);
        //return ess(url, {json: true});
    };
}

function createStreamRoute(app, routeName, times, body) {
    routeName = routeName || '/stream';
    times = times || 1;
    body = body || 'data';

    app.get(routeName, sse.head(), sse.ticker({seconds: 3}), function(req, res) {
        _.times(times, function(id) {
            sse.send({event: 'who knows', data: {foo: body}, id: id.toString()})(req, res);
        });
    });
}
