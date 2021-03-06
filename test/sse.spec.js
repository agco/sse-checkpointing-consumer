//modules
var _ = require('lodash');
var chai = require('chai');
var expect = chai.expect;
var express = require('express');
var Promise = require('bluebird');
var request = require('request');
var sinonChai = require('sinon-chai');
var sse = require('tiny-sse');
var redis = require('then-redis');

chai.use(sinonChai);

//locals
var app = express();
var Consumer = require('..');
var consumer;
var port = 3000;
var redisUrl = process.env.REDIS_URL || 'localhost';
var redisClient = redis.createClient(redisUrl);
var streamURL = 'http://localhost:' + port + '/stream';
var streamThreeURL = 'http://localhost:' + port + '/streamThree';
var streamSixURL = 'http://localhost:' + port + '/streamSix';
var streamFailURL = 'http://localhost:' + port + '/streamFail';

describe('SSE Checkpointing Consumer module', function () {

    before(function () {
        createStreamRoute(app, '/stream', 1, 'bar');
        createStreamRoute(app, '/streamThree', 3, 'bar');
        createStreamRoute(app, '/streamSix', 6, 'bar');
        createFailingStreamRoute(app, '/streamFail', 3, 'bar', true);

        var server = app.listen(port, function () {
            var host = server.address().address;
            var port = server.address().port;
            console.log('Test server listening at http://%s:%s', host, port);
        });
    });

    beforeEach(function () {
        consumer = new Consumer();
        return redisClient.flushdb();
    });

    describe('#consume', function () {
        it('is a function', function () {
            expect(consumer.consume).to.be.an.instanceOf(Function);
        });

        it('returns the consumer for chaining', function () {
            expect(consumer.consume(createStream(streamURL))).to.equal(consumer);
        });

        it('throws an error if it\'s not passed a function', function () {
            expect(consumer.consume).to.throw(Error, /Module requires function/);
        });

        it('binds the createStream to the object', function () {
            consumer.consume(createStream(streamURL));
            expect(typeof consumer.streamConstructor).to.equal('function');
        });

        describe('when there is an existing id', function () {
            beforeEach(function (done) {
                consumer
                    .consume(createStream(streamURL, true))
                    .onEvent(emptyHook)
                    .checkpoint({
                        redisUrl: redisUrl,
                        messages: 1,
                        callback: function () {
                            done()
                        }
                    });
            });

            it('passes the id to the consume function', function (done) {
                var secondConsumer = new Consumer();

                secondConsumer
                    .consume(checkId)
                    .onEvent(emptyHook)
                    .checkpoint({
                        redisUrl: redisUrl,
                        messages: 1
                    });

                function checkId(id) {
                    expect(id).to.exist;
                    done();
                    return createStream(streamURL)();
                }
            });


        });
    });

    describe('#onEvent', function () {
        describe('when a stream has not been consumed yet', function () {
            it('throws an error', function () {
                expect(consumer.onEvent.bind(consumer)).to.throw(Error, /A stream constructor must be supplied first/);
            });
        });

        describe('when a stream has been consumed', function () {
            it('calls the onEvent hook', function (done) {
                consumer.consume(createStream(streamURL))
                    .onEvent(function (data) {
                        expect(JSON.parse(data.data).foo).to.equal('bar');
                        expect(data.id).to.equal("0");
                        done();
                        return true;
                    })
                    .checkpoint(false);
            });

            it('returns the original consumer', function () {
                var testConsumer = consumer
                    .consume(createStream(streamURL))
                    .onEvent(function () {
                        return true;
                    });
                expect(testConsumer).to.equal(consumer);
            });
        });
    });

    describe('#checkpoint', function () {

        it('creates a checkpoint after receiving 1 message', function (done) {
            var onceDone = _.once(done);
            consumer
                .consume(createStream(streamThreeURL))
                .onEvent(emptyHook)
                .checkpoint({
                    redisUrl: redisUrl,
                    messages: 1,
                    callback: checkCheckpoint
                });

            function checkCheckpoint(checkpoint) {
                expect(checkpoint).to.not.be.null;
                onceDone();
            }
        });

        it('creates a checkpoint after receiving 3 messages', function (done) {
            var onceDone = _.once(done);
            consumer
                .consume(createStream(streamThreeURL))
                .onEvent(emptyHook)
                .checkpoint({
                    redisUrl: redisUrl,
                    messages: 1,
                    callback: checkCheckpoint
                });

            function checkCheckpoint() {
                return redisClient.get('checkpoint')
                    .then(function (checkpoint) {
                        expect(JSON.parse(checkpoint).id).to.equal('2');
                        onceDone();
                    });
            }
        });

        it('allows the message threshold to be set', function (done) {
            var onceDone = _.once(done);
            consumer
                .consume(createStream(streamSixURL))
                .onEvent(emptyHook)
                .checkpoint({
                    redisUrl: redisUrl,
                    messages: 2,
                    callback: checkCheckpoint
                });

            function checkCheckpoint(checkpoint) {
                if ((JSON.parse(checkpoint).id) === '5') {
                    onceDone();
                }
            }
        });
    });

    describe('#getLastProcessedTime', function () {
        it('gets the unix seconds when the last event was processed', function (done) {
            consumer
                .consume(createStream(streamSixURL))
                .onEvent(emptyHook)
                .checkpoint({
                    redisUrl: redisUrl,
                    messages: 6,
                    callback: checkLastProcessed
                });

            function checkLastProcessed() {
                expect(consumer.getLastProcessedTime()).to.be.a.Number;
                done();
            }
        });
    });

    describe('retries connection on failure', function () {
        it('creates a checkpoint after receiving 3 messages', function (done) {
            var onceDone = _.once(done);
            consumer
                .consume(createStream(streamFailURL))
                .onEvent(emptyHook)
                .checkpoint({
                    redisUrl: redisUrl,
                    messages: 1,
                    callback: checkCheckpoint
                });

            function checkCheckpoint(checkpoint) {
                if (JSON.parse(checkpoint).id === '3') {
                    onceDone();
                }
            }
        });
    });
});

function createStream(url, isPromise) {
    return function () {
        return isPromise ?
            new Promise(function (res) {
                res(request(url));
            }) :
            request(url);
    };
}

function emptyHook() {
    return true;
}

function createStreamRoute(app, routeName, times, body) {
    routeName = routeName || '/stream';
    times = times || 1;
    body = body || 'data';

    app.get(routeName, sse.head(), sse.ticker({seconds: 3}), function (req, res) {
        _.times(times, function (id) {
            sse.send({event: 'who knows', data: {foo: body}, id: id.toString()})(req, res);
        });
        res.end();
    });
}

function createFailingStreamRoute(app, routeName, body) {

    var resume = false;

    app.get(routeName, sse.head(), sse.ticker({seconds: 3}), function (req, res) {
        if (!resume) {
            sse.send({event: 'who knows', data: {foo: 'bar'}, id: '1'})(req, res);
            resume = true;
            var socket = req.connection;
            socket.on('close', function () {
                console.log('socket closed');
            });
            socket.destroy()

        } else {
            sse.send({event: 'who knows', data: {foo: 'bar'}, id: '2'})(req, res);
            sse.send({event: 'who knows', data: {foo: 'bar'}, id: '3'})(req, res);
            res.end();
        }
    });
}
