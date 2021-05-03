'use strict';
const util = require('util');
module.exports = util.promisify(setImmediate);