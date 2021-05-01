'use strict';
const joi = require('joi');
const _ = require('lodash');
const MMQ = require('../MMQ');
const MMQIValidator = (value, helpers) => value instanceof MMQ ? value : helpers.error('object.invalid');
const validators = {
    constructor: joi.object(
        {
            MMQI: joi.object().custom(MMQIValidator, 'MMQI'),
            shift: joi.boolean().required() 
        }
    ),
    on: (
        joi
            .alternatives()
            .try(
                joi
                    .array()
                    .length(2)
                    .ordered(
                        joi.string().required(),
                        joi.function().required()
                    ),
                joi
                    .array()
                    .length(3)
                    .ordered(
                        joi.string().required(),
                        joi.string().required(),
                        joi.function().required()
                    )
            )
    ),
    off: (
        joi
            .alternatives()
            .try(
                joi
                    .array()
                    .length(2)
                    .ordered(
                        joi.string().required(),
                        joi.function().required()
                    ),
                joi
                    .array()
                    .length(3)
                    .ordered(
                        joi.string().required(),
                        joi.string().required(),
                        joi.function().required()
                    )
            )
    )

};

module.exports = validators;