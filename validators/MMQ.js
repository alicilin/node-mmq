'use strict';
const joi = require('joi');
const validators = {
    constructor: joi.object(
        {
            client: joi.any().required(),
            channel: joi.string().required(),
            servicename: joi.string().required(),
            dbname: joi.string().optional().allow(null)
        }
    ),
    log: joi.object(
        {
            sender: joi.string().required(),
            event: joi.string().required(),
            message: joi.string().required(),
            data: joi.any().optional().allow(null)
        }
    ),

    lock: joi.object(
        {
            key: joi.object().unknown(true).required(),
            ms: joi.number().required()
        }
    ),

    next: joi.object(
        {
            senders: (
                joi
                    .alternatives()
                    .try(
                        joi.array().items(joi.string().required()),
                        joi.string().required()
                    )
                    .optional()
                    .allow(null)
            ),
            events: (
                joi
                    .alternatives()
                    .try(
                        joi.array().items(joi.string().required()),
                        joi.string().required()
                    )
                    .optional()
                    .allow(null)
            ),

            filters: joi.object().unknown(true).required(),
            shift: joi.boolean().required()

        }
    ),

    unlock: joi.object().unknown(true).required(),
    send: joi.object(
        {
            service: joi.string().required(),
            event: joi.string().required(),
            retry: joi.number().integer().required(),
            status: joi.number().integer().valid(0, 1, 2, 3).required(),
            data: joi.any().optional().allow(null),
            parent: joi.any().optional().allow(null),
            waitReply: joi.boolean().required(),
            delay: (
                joi
                    .alternatives()
                    .try(
                        joi.number().integer().required(),
                        joi.string().required(),
                        joi.date().required()
                    )
                    .optional()
                    .allow(null)
            )

        }
    )

};

module.exports = validators;