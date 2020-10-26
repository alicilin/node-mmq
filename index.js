'use strict';
const { times } = require('lodash');
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
                (await this.db.collection(this.qcoll).createIndex('sender'));
                (await this.db.collection(this.qcoll).createIndex('receiver'));
                (await this.db.collection(this.qcoll).createIndex('event'));
                (await this.db.collection(this.qcoll).createIndex('status'));
            }

            if (!collections.find(x => x.name === this.lcoll)) {
                (await this.db.createCollection(this.lcoll));
                (await this.db.collection(this.lcoll).createIndex('sender'));
                (await this.db.collection(this.lcoll).createIndex('receiver'));
                (await this.db.collection(this.lcoll).createIndex('channel'));
                (await this.db.collection(this.lcoll).createIndex('event'));
            }

            if (!collections.find(x => x.name === this.pcoll)) {
                (await this.db.createCollection(this.pcoll, { capped: true, size: 10000000, max: 100000 }));
                (await this.db.collection(this.pcoll).createIndex('channel'));
                (await this.db.collection(this.pcoll).createIndex('sender'));
                (await this.db.collection(this.pcoll).createIndex('receiver'));
                (await this.db.collection(this.pcoll).createIndex('event'));
            }
        }

        if (this.client.isConnected()) {
            this.db = this.client.db(this.dbname);
        }

        (await this.db.collection(this.scoll).updateOne({ name: this.servicename }, { $set: { name: this.servicename } }, { upsert: true }));
        let lastdoc = await this.db.collection(this.pcoll).findOne({ receiver: this.servicename, channel: this.channel }, { sort: [ ['_id', 'desc'] ] });
        if (lastdoc) {
            this.pubsubNext = lastdoc._id;
        }
    }

    async log({ sender, event, message, data }) {
        (await this.db.collection(this.lcoll).insertOne({ sender, receiver: this.servicename, channel: this.channel, event, message, data }));
        return true;
    }

    async next({ senders = null, events = null, shift = true }) {
        let filter = { 
            receiver: this.servicename, 
            channel: this.channel,
            status: 0 
        }

        if (events) filter.event = _.isArray(events) ? { $in: events } : events;
        if (senders) filter.sender = _.isArray(senders) ? { $in: senders } : senders;

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

    async resolvent({ senders = null, events = null, rcb = null }) {
        let filter = {
            receiver: this.servicename,
            channel: this.channel
        }

        let options = {
            tailable: true,
            awaitdata: true,
            numberOfRetries: -1
        }

        if (events) filter.event = _.isArray(events) ? { $in: events } : events;
        if (senders) filter.sender = _.isArray(senders) ? { $in: senders } : senders;
        if (this.pubsubNext) filter._id = { $gt: this.pubsubNext };

        let cursor = this.db.collection(this.pcoll).find(filter, options);
        while (true) {
            await cursor.next();
            while (true) {
                if (typeof rcb.call(this) !== 'function') {
                    await sleep(500);
                    continue;
                }
                break;
            }

            let scb = rcb;
            while (true) {
                scb = scb.call(this);
                if (typeof scb !== 'function') {
                    break;
                }
            }    
        }
    }

    async send({ service = '*', event, retry = 0, status = 0, data = { } }) {
        if (service === '*') {
            let services = (await this.db.collection(this.scoll).find({ }).toArray());
            for (let { name } of services) {
                if (name !== this.servicename) {
                    (await this.db.collection(this.qcoll).insertOne({ channel: this.channel, sender: this.servicename, receiver: name, event, retry, status, data }));
                    (await this.db.collection(this.pcoll).insertOne({ channel: this.channel, sender: this.servicename, receiver: name, event }));
                }
            }

            return { channel: this.channel, sender: this.servicename, receiver: _.map(services, x => x.name), event, retry, status, data };
        }

        (await this.db.collection(this.qcoll).insertOne({ channel: this.channel, sender: this.servicename, receiver: service, event, retry, status, data }));
        (await this.db.collection(this.pcoll).insertOne({ channel: this.channel, sender: this.servicename, receiver: service, event }));
        return { channel: this.channel, sender: this.servicename, receiver: service, event, retry, status, data };
    }
}

class Worker {
    constructor({ MMQI, shift = true, maxWaitSeconds = 60 }) {
        this.mmqi = MMQI;
        this.listeners = [];
        this.send = this.mmqi.send.bind(this.mmqi);
        this.shift = shift;
        this.resolve = null;
        this.maxWaitSeconds = 1000 * maxWaitSeconds;
        this.WaitSeconds = 0;
        this.intervalcb = () => {
            this.WaitSeconds++;
            if (this.WaitSeconds >= this.maxWaitSeconds) {
                if (typeof this.resolve === 'function') {
                    this.resolve.call(this);
                }
                
                this.WaitSeconds = 0;
            }
        }
        this.setInterval = setInterval.bind(this, this.intervalcb, 1);
        this.clearInterval = clearInterval.bind(this);
        this.iid = 0;
        this.empty = 0;
    }

    on(...params) {
        if (params.length === 2) {
            this.listeners.push({ event: params[0], cb: params[1] });
        }
        
        if (params.length === 3) {
            this.listeners.push({ event: params[0], sender: params[1], cb: params[2] });
        }
        
        return this;
    }

    off(...params) {
        if (params.length === 2) {
            _.remove(this.listeners, listener => listener.event === params[0] && listener.cb === params[1]);
        }

        if (params.length === 3) {
            _.remove(this.listeners, listener => listener.event === params[0] && listener.sender === params[1] && listener.cb === params[2]);
        }
        
        return this;
    }

    async start() {
        this.mmqi.resolvent({ rcb: () => this.resolve });
        while (true) {
            let events = _.uniq(_.map(this.listeners, listener => listener.event));
            let senders = _.uniq(_.compact(_.map(this.listeners, listener => listener.sender || null)));
            let filter = { events, shift: this.shift };
            if (senders.length > 0) filter.senders = senders;
            if (this.empty > 2) {
                (this.iid = this.setInterval.call(this));
                (await new Promise(resolve => this.resolve = resolve));
            }
            this.resolve = null;
            this.clearInterval.call(this, this.iid);
            let { value } = (await this.mmqi.next(filter));
            if (!value) {
                this.empty++;
                continue;
            }
            
            this.empty = 0;
            for (let listener of this.listeners) {
                let condition = (
                    (
                        (
                            listener.event instanceof RegExp 
                            && 
                            listener.event.test(value.event)
                        ) 
                        ||
                        value.event === listener.event
                    )
                    &&
                    (
                        !listener.sender
                        ||
                        (
                            (
                                listener.sender instanceof RegExp
                                &&
                                listener.sender.test(value.sender)
                            )
                            ||
                            value.sender === listener.sender
                        )
                    )
                );
                if (condition) {
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

                            await this.mmqi.log({ sender: value.sender, event: value.event, data: value.data, message: serror.message });
                            await this.send({ service: value.receiver, event: value.event, retry: 0, status: 2, data: value.data });
                            break;
                        }
                    }
                }
            }
        }
    }
}

module.exports = { MMQ, Worker };