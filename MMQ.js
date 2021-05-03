'use strict';
const _ = require('lodash');
const moment = require('moment');
const validators = require('./validators/MMQ');
const io = require('socket.io-client');
const nextTick = require('./helpers/next-tick');
const Axios = require('axios').default;
const http = require('http');
const https = require('https');
const sleep = require('./helpers/sleep');
const RPromise = require('./helpers/resolvable-promise');

class MMQ {
    constructor({ channel = 'default', servicename = 'master', ip, port = 9900, secret = 'mmq12', transport = 'polling' }) {
        validators.constructor.validate({ channel, servicename, ip, port, secret, transport });
        this.channel = channel;
        this.servicename = servicename;
        this.ip = ip,
        this.port = port;
        this.secret = secret;
        this.connected = false;
        this.resolver = null;
        this.options = {
            forceNew: true,
            secure: true,
            rejectUnauthorized: false,
            path: '/io',
            autoConnect: true,
            transports: [transport],
            auth: {
                'X-NAME': this.servicename,
                'X-SECRET': this.secret,
                'X-CHANNEL': this.channel
            }
        };

        this.http = Axios.create(
            {
                baseURL: 'http://' + this.ip + ':' + this.port,
                timeout: 600000,
                httpAgent: new http.Agent({ keepAlive: true }),
                httpsAgent: new https.Agent({ keepAlive: true }),
                maxRedirects: 10,
                maxContentLength: Infinity,
                maxBodyLength: Infinity,
                headers: {
                    'X-NAME': this.servicename,
                    'X-SECRET': this.secret,
                    'X-CHANNEL': this.channel
                }
            }
        );

        this.error = () => {
            this.connected = false;
            if(!_.isNil(this.resolver)) {
                this.resolver()
            }
        };

        this.success = () => {
            this.connected = true;
        };

        this.newMessage = () => {
            if (!_.isNil(this.resolver)) {
                this.resolver();
            }
        };

        this.io = io('http://' + this.ip + ':' + this.port, this.options);
        this.io.on('error', this.error.bind(this));
        this.io.on('connect', this.success.bind(this));
        this.io.on('reconnect', this.success.bind(this));
        this.io.on('reconnect_error', this.error.bind(this));
        this.io.on('reconnect_failed', this.error.bind(this));
        this.io.on('message', this.newMessage.bind(this));
    }

    async log({ sender, event, message, data }) {
        try {
            let post = {
                sender: sender, 
                event: event, 
                message: message, 
                data: data
            };
            (await validators.log.validateAsync(post));
            (await this.http.post('/log', post));
            return true;
        } catch (error) {
            if (!_.has(error, 'response.data')) {
                throw new Error(error.message);
            }

            throw new Error(_.get(error, 'response.data.message', null));
        }
    }

    async *next({ senders = null, events = null, filters = {} }) {
        try {
            let post = {
                senders: senders,
                events: events,
                filters: filters
            };

            (await validators.next.validateAsync(post));
            while (true) {
                (await nextTick());
                let value = _.get((await this.http.post('/shift', post)), 'data');
                if (!_.has(value, '_id')) {
                    if (this.connected === false) {
                        (await sleep(1000));
                        continue;
                    }

                    (await RPromise(this.resolver));
                    continue;
                }

                yield value;
            }
        } catch (error) {
            if (!_.has(error, 'response.data')) {
                throw new Error(error.message);
            }

            throw new Error(_.get(error, 'response.data.message', null));
        }
    }

    async send({ service = '*', event, retry = 0, data = {}, parent = null, waitReply = false, delay = null }) {
        try {
            let post = {
                service: service,
                event: event,
                retry: retry,
                data: data,
                parent: parent,
                delay: delay
            };

            (await validators.send.validateAsync({ ...post, waitReply }));
            let response = _.get((await this.http.post('/send', post)), 'data');
            let checkSingle = ( 
                _.startsWith(service, '^') === false 
                && 
                _.startsWith(service, '*') === false
            );

            if (waitReply === true && checkSingle === true) {
                let startms = moment().valueOf();
                let maxWaitms = 2 * 60 * 1000;
                let nextp = {
                    senders: service,
                    events: event,
                    filters: {
                        parent: response.uid
                    }
                };

                let resolver = resolve => {
                    let intid = null;
                    let intcb = () => {
                        if ((moment().valueOf() - startms) > maxWaitms) {
                            clearInterval(intid);
                            resolve(null);
                        }
                    };

                    intid = setInterval(intcb, 1000);
                    this.next(nextp).next()
                        .then(doc => resolve(doc))
                        .finally(() => clearInterval(intid));
                };

                return await (new Promise(resolver));
            }
        } catch (error) {
            if (!_.has(error, 'response.data')) {
                throw new Error(error.message);
            }

            console.log(error.response.data);
            throw new Error(_.get(error, 'response.data.message', null));
        }

    }
}

module.exports = MMQ;
