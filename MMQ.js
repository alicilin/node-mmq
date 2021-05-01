'use strict';
const _ = require('lodash');
const sleep = require('./helpers/sleep');
const objectHash = require('object-hash');

class MMQ {
    constructor({ client, channel, servicename, dbname }) {
        this.channel = channel;
        this.servicename = servicename || 'default';
        this.dbname = dbname || 'MMQ';
        this.scoll = this.dbname + '_services';
        this.qcoll = this.dbname + '_queue';
        this.lcoll = this.dbname + '_logs';
        this.lockcoll = this.dbname + '_lock';
        this.db = null;
        this.client = client;
    }

    async connect() {
        if (!this.client.isConnected()) {
            (await this.client.connect());
        }
        
        this.db = this.client.db(this.dbname);
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
            (await this.db.collection(this.qcoll).createIndex('parent'));
            (await this.db.collection(this.qcoll).createIndex('status'));
        }

        if (!_.find(collections, x => x.name === this.lockcoll)) {
            (await this.db.createCollection(this.lockcoll));
            (await this.db.collection(this.lockcoll).createIndex({ key: 1 }, { unique: true }));
        }

        let sfilter = { 
            name: this.servicename 
        };

        let sset = { 
            $set: { 
                name: this.servicename 
            } 
        };

        (await this.db.collection(this.scoll).updateOne(sfilter, sset, { upsert: true }));
        (await this.db.collection(this.lockcoll).deleteMany({ }));
    
    }

    async log({ sender, event, message, data }) {
        let doc = {};
        _.set(doc, 'sender', sender);
        _.set(doc, 'receiver', this.servicename);
        _.set(doc, 'channel', this.channel);
        _.set(doc, 'event', event);
        _.set(doc, 'message', message);
        _.set(doc, 'data', data);

        (await this.db.collection(this.lcoll).insertOne(doc));
        return true;
    }

    async lock(key, ms = 250) {
        let hash = objectHash(key);
        while (true) {
            try {
                (await this.db.collection(this.lockcoll).insertOne({ key: hash }));
                return;
            } catch (error) {
                (await sleep(ms));
                continue;
            }
        }
    }

    async unlock(key) {
        (await this.db.collection(this.lockcoll).deleteOne({ key: objectHash(key) }));
    }

    async next({ senders = null, events = null, filters = {}, shift = true }) {
        let filter = {
            receiver: this.servicename,
            channel: this.channel,
            status: 0
        };

        if (events) filter.event = _.isArray(events) ? { $in: events } : events;
        if (senders) filter.sender = _.isArray(senders) ? { $in: senders } : senders;
        if (filters) filter = _.merge(filter, filters);

        let options = {
            sort: [
                [
                    '_id',
                    'asc'
                ]
            ]
        };

        let collection = this.db.collection(this.qcoll);
        return await (
            shift 
                ? collection.findOneAndDelete(filter, options) 
                : collection.findOneAndUpdate(filter, { $set: { status: 1 } }, options)
        );
    }

    async send({ service = '*', event, retry = 0, status = 0, data = {}, parent = null, waitReply = false }) {
        if (service === '*') {
            let services = (await this.db.collection(this.scoll).find({}).toArray());
            let doc = {};
            for (let { name } of services) {
                if (!_.isEqual(name, this.servicename)) {
                    _.set(doc, 'channel', this.channel);
                    _.set(doc, 'sender', this.servicename);
                    _.set(doc, 'receiver', name);
                    _.set(doc, 'event', event);
                    _.set(doc, 'retry', retry);
                    _.set(doc, 'status', status);
                    _.set(doc, 'data', data);
                    (await this.db.collection(this.qcoll).insertOne(doc));
                }
            }

            return {
                channel: this.channel,
                sender: this.servicename,
                receiver: _.map(services, x => x.name),
                event: event,
                retry: retry,
                status: status,
                data: data
            };
        }

        let doc = {};
        _.set(doc, 'channel', this.channel);
        _.set(doc, 'sender', this.servicename);
        _.set(doc, 'receiver', service);
        _.set(doc, 'event', event);
        _.set(doc, 'retry', retry);
        _.set(doc, 'status', status);
        _.set(doc, 'data', data);
        _.set(doc, 'parent', parent);

        let docid = _.get((await this.db.collection(this.qcoll).insertOne(doc)), 'insertedId');
        if (waitReply) {
            let startms = new Date().getTime();
            let maxWaitms = 2 * 60 * 1000;

            while (true) {
                if (new Date().getTime() - startms > maxWaitms) {
                    return null;
                }

                let { value } = (await this.next({ senders: service, events: event, filters: { parent: docid }, shift: true }));
                if (!_.isNil(value)) {
                    return value;
                }
                
                (await sleep(500));
            }

        }


        return {
            channel: this.channel,
            sender: this.servicename,
            receiver: service,
            event: event,
            retry: retry,
            status: status,
            data: data
        };
    }
}

module.exports = MMQ;
