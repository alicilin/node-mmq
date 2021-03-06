'use strict';
const joi = require('joi');
const validators = {
    constructor: joi.object(
        {
            channel: joi.string().required(),
            servicename: joi.string().required(),
            ip: joi.string().required(),
            port: joi.number().integer().min(7000).max(9999).required(),
            secret: joi.string().required(),
            transport: joi.string().required().valid('websocket', 'polling')
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
        }
    ),

    send: joi.object(
        {
            service: joi.string().required(),
            event: joi.string().required(),
            retry: joi.number().integer().required(),
            data: joi.any().required(),
            parent: joi.any().optional().allow(null),
            waitReply: joi.boolean().required(),
            delay: (
                joi
                    .alternatives()
                    .try(
                        joi.number().integer().required(),
                        joi.string().required(),
                    )
                    .optional()
                    .allow(null)
            )

        }
    )

};

module.exports = validators;