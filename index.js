'use strict';
const _ = require('lodash');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
class MMQ {
    constructor({ client, channel, servicename, dbname }) {
        this.channel = channel;
        this.servicename = servicename || 'default';
        this.dbname = dbname || 'MMQ';
        this.qcoll = this.dbname + '_queues';
        this.scoll = this.dbname + '_services';
        this.lcoll = this.dbname + '_logs';
        this.pcoll = this.dbname + '_pubsub'
        this.db = null;
        this.client = client;
        this.resolve = null;
        this.pubsubNext = null;
    }

    async connect() {
        if (!this.client.isConnected()) { 
            (await this.client.connect());
            this.db = this.client.db(this.dbname);
            let collections = await this.db.listCollections().toArray();
            if (!collections.find(x => x.name === this.scoll)) {
                (await this.db.createCollection(this.scoll));
                (await this.db.collection(this.scoll).createIndex('name'));
            }

            if (!collections.find(x => x.name === this.qcoll)) {
                (await this.db.createCollection(this.qcoll));
                (await this.db.collection(this.qcoll).createIndex('channel'));
                (await this.db.collection(this.qcoll).createIndex('service'));
                (await this.db.collection(this.qcoll).createIndex('event'));
                (await this.db.collection(this.qcoll).createIndex('status'));
            }

            if (!collections.find(x => x.name === this.lcoll)) {
                (await this.db.createCollection(this.lcoll));
                (await this.db.collection(this.lcoll).createIndex('service'));
                (await this.db.collection(this.lcoll).createIndex('channel'));
                (await this.db.collection(this.lcoll).createIndex('event'));
            }

            if (!collections.find(x => x.name === this.pcoll)) {
                (await this.db.createCollection(this.pcoll, { capped: true, size: 10000000, max: 100000 }));
                (await this.db.collection(this.pcoll).createIndex('channel'));
                (await this.db.collection(this.pcoll).createIndex('service'));
                (await this.db.collection(this.pcoll).createIndex('event'));
            }
        }

        if (this.client.isConnected()) {
            this.db = this.client.db(this.dbname);
        }

        (await this.db.collection(this.scoll).updateOne({ name: this.servicename }, { $set: { name: this.servicename } }, { upsert: true }));
        let lastdoc = await this.db.collection(this.pcoll).findOne({ service: this.servicename, channel: this.channel }, { sort: [ ['_id', 'desc'] ] });
        if (lastdoc) {
            this.pubsubNext = lastdoc._id;
        }
    }

    async log({ event, message, data }) {
        (await this.db.collection(this.lcoll).insertOne({ service: this.servicename, channel: this.channel, event, message, data }));
        return true;
    }

    async next(events = null, shift = true) {
        let filter = { 
            service: this.servicename, 
            channel: this.channel,
            status: 0 
        }

        if (events) filter.event = _.isArray(events) ? { $in: events } : events;
         
        let options = { 
            sort: [ 
                [
                    '_id', 
                    'asc' 
                ] 
            ] 
        }

        let collection = this.db.collection(this.qcoll);
        return await (shift ? collection.findOneAndDelete(filter, options) : collection.findOneAndUpdate(filter, { $set: { status: 1 } }, options));
    }

    async resolvent(events = null) {
        let filter = {
            service: this.servicename,
            channel: this.channel
        }

        let options = {
            tailable: true,
            awaitdata: true,
            numberOfRetries: -1
        }

        if (events) filter.event = _.isArray(events) ? { $in: events } : events;
        if (this.pubsubNext) filter._id = { $gt: this.pubsubNext };

        let cursor = this.db.collection(this.pcoll).find(filter, options);
        while (true) {
            await cursor.next();
            while (true) {
                if (typeof this.resolve !== 'function') {
                    await sleep(100);
                    continue;
                }
                break;
            }

            this.resolve.call(this);    
        }
    }

    setResolve(resolve) {
        this.resolve = resolve;
    }

    async send({ service = '*', event, retry = 0, status = 0, data = { } }) {
        if (service === '*') {
            let services = (await this.db.collection(this.scoll).find({ }).toArray());
            for (let { name } of services) {
                if (name !== this.servicename) {
                    (await this.db.collection(this.qcoll).insertOne({ channel: this.channel, service: name, event, retry, status, data }));
                    (await this.db.collection(this.pcoll).insertOne({ channel: this.channel, service: name, event }));
                }
            }

            return { channel: this.channel, service: services.map(x => x.name), event, retry, status, data };
        }

        (await this.db.collection(this.qcoll).insertOne({ channel: this.channel, service, event, retry, status, data }));
        (await this.db.collection(this.pcoll).insertOne({ channel: this.channel, service, event }));
        return { channel: this.channel, service, event, retry, status, data };
    }
}

class Worker {
    constructor(MMQI, shift = true) {
        this.mmqi = MMQI;
        this.listeners = [];
        this.sleep = sleep;
        this.send = this.mmqi.send.bind(this.mmqi);
        this.shift = shift;
    }

    on(event, cb) {
        this.listeners.push({ event, cb });
        return this;
    }

    off(event, cb) {
        let index = this.listeners.findIndex(listener => event === listener.event && listener.cb === cb);
        this.listeners.splice(index, 1);
        return this;
    }

    async start() {
        this.mmqi.resolvent();
        let empty = 0;
        while (true) {
            let events = _.uniq(this.listeners.map(listener => listener.event));
            if (empty > 3) (await new Promise(resolve => this.mmqi.setResolve(resolve)));
            this.mmqi.setResolve(null);
            let { value } = (await this.mmqi.next(events, this.shift));
            if (!value) {
                empty++;
                continue;
            }
            
            empty = 0;
            for (let listener of this.listeners) {
                if (value.event === listener.event) {
                    let retrynum = 0;
                    while (true) {
                        try {
                            let cbr = listener.cb.call(this, value);
                            if (cbr instanceof Promise) await cbr;
                            break;
                        } catch (error) {
                            if (value.retry > retrynum++) {
                                await sleep(10);
                                continue;
                            }

                            await this.mmqi.log({ event: value.event, data: value.data, message: serror.message });
                            await this.send({ service: value.service, event: value.event, retry: 0, status: 2, data: value.data });
                            break;
                        }
                    }
                }
            }
        }
    }
}

module.exports = { MMQ, Worker };