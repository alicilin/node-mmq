'use strict';
const io = require('socket.io');
const http = require('http');
const express = require('express');
const app = express();
const ioredis = require('socket.io-redis');
const httpServer = http.createServer(app);
const _ = require('lodash');
const validators = require('./validators/Daemon');
const stc = require('./helpers/stc');
const moment = require('moment');
const { MongoClient } = require('mongodb');
const { v5, v4, v1 } = require('uuid');
const uuid = () => v5(v1(), v4());

async function socketAuth(socket, next) {
    if (!socket.handshake.auth['X-NAME']) {
        return socket.disconnect(true);
    }

    if (!socket.handshake.auth['X-CHANNEL']) {
        return socket.disconnect(true);
    }

    if (socket.handshake.auth['X-SECRET'] !== this.secret) {
        return socket.disconnect(true);
    }

    socket.join('SERVICES');
    socket.join(`${socket.handshake.auth['X-NAME']}:${socket.handshake.auth['X-CHANNEL']}`);
    
    let filter = { name: socket.handshake.auth['X-NAME'] };
    let set = { $set: { name: socket.handshake.auth['X-NAME'] } };
    
    (await this.db.collection(this.scoll).updateOne(filter, set, { upsert: true }));
    next();
}

async function appAuth(req, res, next) {
    if (!req.get('X-NAME')) {
        return res.status(500).send({ succes: false, message: 'AUTH_FAIL' });
    }
    
    if (!req.get('X-CHANNEL')) {
        return res.status(500).send({ succes: false, message: 'AUTH_FAIL' });
    }
    
    if (req.get('X-SECRET') !== this.secret) {
        return res.status(500).send({ succes: false, message: 'AUTH_FAIL' });
    }

    next();
}

async function timer() {
    let filter = { delay: { $lte: moment().toDate() }};
    let cursor = this.db.collection(this.qcoll).find(filter).sort([['_id', 'asc']]).limit(10);
    for await (let item of cursor) {
        this.io.to(`${item.receiver}:${item.channel}`).emit('message', true);
    }
}

async function send(req, res) {
    let valid = (await stc(() => validators.send.validateAsync(req.body)));
    if (_.isError(valid)) {
        return res.status(500).send({ success: false, message: valid.message });
    }

    try {
        if (_.isNil(req.body.delay)) {
            req.body.delay = (
                moment()
                    .subtract(10, 'minute')
                    .toDate()
            );
        }

        if (_.isInteger(req.body.delay)) {
            req.body.delay = (
                moment()
                    .add(req.body.delay, 'ms')
                    .toDate()
            );
        }

        if (_.isString(req.body.delay)) {
            req.body.delay = (
                moment(req.body.delay)
                    .toDate()
            );
        }

        if (!_.isDate(req.body.delay)) {
            return res.status(500).send({ success: false, message: 'DELAY_IS_NOT_DATE' });
        }

        if (_.startsWith(req.body.service, '^') || _.startsWith(req.body.service, '*')) {
            let filter = {};
            if (_.startsWith(req.body.service, '^')) {
                filter.name = new RegExp(req.body.service);
            }

            let services = (await this.db.collection(this.scoll).find(filter).toArray());
            let doc = {};
            for (let { name } of services) {
                if (!_.isEqual(name, req.get('X-NAME'))) {
                    _.set(doc, 'channel', req.get('X-CHANNEL'));
                    _.set(doc, 'sender', req.get('X-NAME'));
                    _.set(doc, 'receiver', name);
                    _.set(doc, 'event', req.body.event);
                    _.set(doc, 'retry', req.body.retry);
                    _.set(doc, 'data', req.body.data);
                    _.set(doc, 'uid', uuid());
                    _.set(doc, 'delay', req.body.delay);

                    (await this.db.collection(this.qcoll).insertOne(doc));
                    this.io.to(`${name}:${req.get('X-CHANNEL')}`).emit('message', true);
                }
            }

            return res.status(200).send(_.set(doc, 'receiver', _.map(services, x => x.name)));
        }
        
        if (_.startsWith(req.body.service, '^') === false && _.startsWith(req.body.service, '*') === false) {
            let doc = {};
            let uid = uuid();
            _.set(doc, 'channel', req.get('X-CHANNEL'));
            _.set(doc, 'sender', req.get('X-NAME'));
            _.set(doc, 'receiver', req.body.service);
            _.set(doc, 'event', req.body.event);
            _.set(doc, 'retry', req.body.retry);
            _.set(doc, 'data', req.body.data);
            _.set(doc, 'uid', uid);
            _.set(doc, 'parent', req.body.parent);
            _.set(doc, 'delay', req.body.delay);

            (await this.db.collection(this.qcoll).insertOne(doc));
            this.io.to(`${req.body.service}:${req.get('X-CHANNEL')}`).emit('message', true);
            res.status(200).send(doc);
        }
    } catch (error) {
        res.status(500).send({ succes: false, message: error.message });
    }
}

async function shift(req, res) {
    let valid = (await stc(() => validators.shift.validateAsync(req.body)));
    if (_.isError(valid)) {
        res.status(500).send({ succes: false, message: valid.message });
    }

    try {
        let filter = {
            receiver: req.get('X-NAME'),
            channel: req.get('X-CHANNEL'),
            delay: {
                $lte: (
                    moment()
                        .toDate()
                )
            }
        };

        if (req.body.events) {
            filter.event = _.isArray(req.body.events) ? { $in: req.body.events } : req.body.events;
        }

        if (req.body.senders) {
            filter.sender = _.isArray(req.body.senders) ? { $in: req.body.senders } : req.body.senders;
        }

        if (req.body.filters) {
            filter = _.merge(filter, req.body.filters);
        }

        let options = {
            sort: [
                [
                    '_id',
                    'asc'
                ]
            ]
        };

        let doc = (await this.db.collection(this.qcoll).findOneAndDelete(filter, options));
        res.status(200).send(doc.value || { });
    } catch (error) {
        res.status(500).send({ succes: false, message: error.message });
    }
}

async function log(req, res) {
    let valid = (await stc(() => validators.log.validateAsync(req.body)));
    if (_.isError(valid)) {
        res.status(500).send({ succes: false, message: valid.message });
    }

    try {
        let doc = {};
        _.set(doc, 'sender', req.body.sender);
        _.set(doc, 'receiver', req.get('X-NAME'));
        _.set(doc, 'channel', req.get('X-CHANNEL'));
        _.set(doc, 'event', req.body.event);
        _.set(doc, 'message', req.body.message);
        _.set(doc, 'data', req.body.data);

        (await this.db.collection(this.lcoll).insertOne(doc));
        res.status(200).send({ success: true });
    } catch (error) {
        res.status(500).send({ success: false, message: error.message });
    }
}

async function entry(req, res) {
    res.status(200).send('hello world');
}

class Daemon {
    constructor({ mongo, redis, dbname, ip = 'localhost', port = 9900, secret = 'mmq12', transport = 'polling' }) {
        validators.constructor.validate({ mongo, redis, dbname, ip, port, secret, transport });
        this.options = {
            path: '/io',
            transports: [transport],
            cors: {
                origin: '*',
                methods: ['GET', 'POST']
            }
        };

        this.ip = ip;
        this.port = port;
        this.secret = secret;
        this.db = null;
        this.mongo = new MongoClient(...mongo);
        this.dbname = dbname || 'MMQ';
        this.scoll = this.dbname + '_services';
        this.qcoll = this.dbname + '_queue';
        this.lcoll = this.dbname + '_logs';
        this.redis = redis;
        this.io = io(httpServer, this.options);
        this.io.adapter(ioredis(...redis));
        this.app = app;
        this.app.use(express.urlencoded({ extended: false }));
        this.app.use(express.json());
    }

    async migrations() {
        if (!this.mongo.isConnected()) {
            (await this.mongo.connect());
        }

        this.db = this.mongo.db(this.dbname);
        let collections = (await this.db.listCollections().toArray());
        if (!_.find(collections, x => x.name === this.scoll)) {
            (await this.db.createCollection(this.scoll));
            (await this.db.collection(this.scoll).createIndex('name'));
        }

        if (!_.find(collections, x => x.name === this.lcoll)) {
            (await this.db.createCollection(this.lcoll));
            (await this.db.collection(this.lcoll).createIndex('sender'));
            (await this.db.collection(this.lcoll).createIndex('receiver'));
            (await this.db.collection(this.lcoll).createIndex('channel'));
            (await this.db.collection(this.lcoll).createIndex('event'));
        }

        if (!_.find(collections, x => x.name === this.qcoll)) {
            (await this.db.createCollection(this.qcoll));
            (await this.db.collection(this.qcoll).createIndex('channel'));
            (await this.db.collection(this.qcoll).createIndex('sender'));
            (await this.db.collection(this.qcoll).createIndex('receiver'));
            (await this.db.collection(this.qcoll).createIndex('event'));
            (await this.db.collection(this.qcoll).createIndex('uid'));
            (await this.db.collection(this.qcoll).createIndex('parent'));
            (await this.db.collection(this.qcoll).createIndex('delay'));
        }
    }

    async start() {
        (await this.migrations());
        this.app.use(appAuth.bind(this));
        this.io.use(socketAuth.bind(this));
        this.app.get('/', entry.bind(this));
        this.app.post('/send', send.bind(this));
        this.app.post('/shift', shift.bind(this));
        this.app.post('/log', log.bind(this));
        setInterval(timer.bind(this), 1000);
        httpServer.listen(this.port, this.ip);
        console.log('server started', this.ip, this.port);
    }
}

module.exports = Daemon;