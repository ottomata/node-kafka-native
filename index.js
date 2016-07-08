var common = require('./lib/common');
var Consumer = require('./lib/consumer').Consumer;
var Producer = require('./lib/producer').Producer;

module.exports = {
    Consumer: Consumer,
    // TODO: implement!
    Producer: Producer,
    Errors: common.Errors,
    Offsets: common.Offsets,
    // TODO: rename RawConsumer, this is a bad name.
    RawConsumer: common.RawConsumer
};
