//modules
var chai = require('chai');
var ess = require('event-source-stream');
var expect = chai.expect;
var express = require('express');
var jsc = require('jsverify');
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

describe('SSE Checkpointing Consumer module', function() {

    before(function() {
        app.get('/stream', sse.head(), sse.ticker({seconds: 3}), function(req, res) {
            sse.send({event: 'data', data: {foo: 'bar'}})(req, res);
        });

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
            expect(consumer.consume(function(){})).to.equal(consumer);
        });

        it('throws an error if it\'s not passed a function', function() {
            expect(consumer.consume).to.throw(Error, /Module requires function/);
        });

        it('calls the callback', function() {
            var callback = sinon.spy();
            consumer.consume(callback);
            expect(callback).to.be.calledOnce;
        });

        it('binds the stream to the object', function() {
            function testStream() {return 'stream';}
            consumer.consume(testStream);
            expect(consumer.stream).to.equal('stream');
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
             consumer.consume(createStream)
                 .onEvent(function(data) {
                     expect(data.foo).to.equal('bar');
                     done();
                 });
        });

        it('returns the original consumer', function() {
            var testConsumer = consumer
                .consume(createStream)
                .onEvent(function() {});
            expect(testConsumer).to.equal(consumer);
        });

        it('allows multiple onEvent hooks', function(done) {
            var hookCounter = 0;

            consumer.consume(createStream)
                .onEvent(function() {
                    hookCounter++;
                })
                .onEvent(function() {
                    hookCounter++;
                    expect(hookCounter).to.equal(2);
                    done()
                });
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

        it('creates a checkpoint after receiving x messages', function() {
            consumer
                .consume(createStream)
                .checkpoint({
                    redisUrl: redisUrl,
                    messages: 1
                });

            return redisClient.get('checkpoint')
                .then(function(checkpoint) {
                    expect(checkpoint).to.not.be.null;
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
                    console.log('data', data);
                    done();
                });
        });

        it('returns a stream for event-emitter', function(done) {
            ess(streamURL, {json: true})
                .on('data', function(data) {
                    expect(data.foo).to.equal('bar');
                    done();
                });
        });
    });
});

function createStream() {return ess(streamURL, {json: true});}
