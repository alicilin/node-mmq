'use strict';
const _ = require('lodash');
const sleep = require('./helpers/sleep');
const validators = require('./validators/Worker');

class Worker {
    constructor({ MMQI }) {
        validators.constructor.validate({ MMQI });
        this.MMQI = MMQI;
        this.listeners = [];
        this.send = this.MMQI.send.bind(this.MMQI);
        this.empty = false;
    }

    on(...params) {
        validators.on.validate(params);
        if (_.size(params) === 2) {
            this.listeners.push({ event: params[0], cb: params[1] });
        }

        if (_.size(params) === 3) {
            this.listeners.push({ event: params[0], sender: params[1], cb: params[2] });
        }

        return this;
    }

    off(...params) {
        validators.off.validate(params);
        if (_.size(params) === 2) {
            _.remove(
                this.listeners, 
                listener => (
                    listener.event === params[0] 
                    && 
                    listener.cb === params[1]
                )
            );
        }

        if (_.size(params) === 3) {
            _.remove(
                this.listeners, 
                listener => (
                    listener.event === params[0] 
                    && 
                    listener.sender === params[1] 
                    && 
                    listener.cb === params[2]
                )
            );
        }

        return this;
    }

    async start() {
        let events = _.uniq(_.map(this.listeners, listener => listener.event));
        let senders = _.uniq(_.compact(_.map(this.listeners, listener => listener.sender || null)));
        let filter = { events };

        if (_.size(senders) > 0) {
            filter.senders = senders;
        }

        for await (let value of this.MMQI.next(filter)) {
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
                    let log = {
                        sender: value.sender,
                        event: value.event,
                        data: value.data,
                    };

                    let runner = async () => {
                        while (true) {
                            try {
                                let cbr = listener.cb.call(this, value);
                                if (cbr instanceof Promise) {
                                    (await cbr);
                                }

                                break;
                            } catch (error) {
                                if (value.retry > retrynum++) {
                                    (await sleep(10));
                                    continue;
                                }

                                (await this.MMQI.log(_.set(log, 'message', error.message)));
                                break;
                            }
                        }
                    };

                    setImmediate(runner.bind(this));
                }
            }
        }
    }
}

module.exports = Worker;