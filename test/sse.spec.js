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
        consumer = new Consumer(redisUrl);
        return redisClient.flushdb();
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
            consumer.consume(callback);
            return Promise.delay(50)
                .then(function() {
                    expect(callback).to.be.calledOnce;
                });
        });

        it('binds the stream to the object', function() {
            consumer.consume(createStream(streamURL));
            expect(consumer.stream.on).to.be.a.function;
        });

        it('allows callbacks that return a promise', function() {
            consumer.consume(createStream(streamURL, true));
            expect(consumer.stream).to.be.an.instanceOf(Promise);
        });

        it('catches callback errors and propagates them', function() {
            function willFail() {throw new Error("Cannot connect to stream")}
            try {
                consumer.consume(willFail);
            } catch (err) {
                expect(err).to.be.an.instanceOf(Error);
                done();
            }
        });

        describe('when there is an existing id', function() {
            beforeEach(function() {
                consumer
                    .consume(createStream(streamURL, true))
                    .checkpoint({
                        redisUrl: redisUrl,
                        messages: 1
                    });
                return Promise.delay(50);
            });

            it('passes the id to the consume function', function(done) {
                var secondConsumer = new Consumer(redisUrl);

                secondConsumer
                    .consume(checkId)
                    .checkpoint({
                        messages: 1
                    });

                function checkId(id) {
                    expect(id).to.exist;
                    done();
                    return createStream(streamURL);
                }
            });


        });
    });

    describe('#onEvent', function() {
        describe('when a stream has not been consumed yet', function() {
            it('throws an error', function() {
                expect(consumer.onEvent.bind(consumer)).to.throw(Error, /A stream must be consumed first/);
            });
        });

        describe('when a stream has been consumed', function() {
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
    });

    describe('#checkpoint', function() {

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
        it('gets the unix seconds when the last event was processed', function() {
            consumer.consume(createStream(streamSixURL));
            return Promise.delay(50).then(function() {
                expect(consumer.getLastProcessedTime()).to.be.a.Number;
            });
        });
    });
});

function createStream(url, promise) {
    return function() {
        return promise ?
            new Promise(function(res) {res(request(url));}) :
            request(url);
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
