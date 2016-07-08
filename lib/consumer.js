var common = require('./common');
var Promise = require('bluebird');

function Consumer(options) {
    var self = this;
    self.options = options;

    var RawConsumer = options.raw_consumer || common.RawConsumer;

    self.handle = new RawConsumer(this.options);
}

Consumer.prototype.subscribe = function(topics) {
    var self = this;
    return self.handle.subscribe(topics);
}

Consumer.prototype.unsubscribe = function() {
    var self = this;
    return self.handle.unsubscribe();
}

Consumer.prototype.close = function() {
    var self = this;
    return self.handle.close();
}

Consumer.prototype.poll = function(timeout_ms) {
    var self = this;
    return self.handle.poll(timeout_ms);
}

Consumer.prototype.pollAsync = function(timeout_ms) {
    var self = this;

    return new Promise(function(resolve) {
        resolve(self.handle.poll(timeout_ms));
    })
}


//  TODO: implement assign, unassign, metadata, etc.

//  Doesn't work yet, am a little green here.
// Consumer.prototype.start = function(timeout_ms, cb) {
//     var self = this;
//
//     if (self._running) {
//         return;
//     }
//
//     // Only call cb if we recieved a non empty message from poll()
//     function _receive(msg) {
//         if (Object.keys(msg).length !== 0) {
//             cb(msg);
//         }
//     }
//
//     console.log('starting');
//     self._running = true;
//     return asyncWhile(
//         function(lastMsg) {
//             console.log('checking');
//             return self._running;
//         },
//         function(lastMsg) {
//             console.log('polling');
//             self.pollAsync(timeout_ms)
//             .then(_receive, console.err);
//             console.log('done polling');
//         }
//     )
//
//     // var i = 0;
//     // var _consumeWhileRunning = asyncWhile(
//     //     function() {
//     //         // synchronous conditional
//     //         console.log('checking')
//     //         // return self._running;
//     //         i++;
//     //         return i < 10;
//     //     },
//     //     function() {
//     //         console.log('polling');
//     //         self.pollAsync(timeout_ms)
//     //         .then(_receive, console.err);
//     //         console.log('done polling');
//     //     }
//     // );
//
//     // Recursively calls pollAsync while self._running
//     // function _consume() {
//     //     if (self._running) {
//     //         console.log('running: ', self._running);
//     //         self.pollAsync(timeout_ms)
//     //         .then(_receive, console.error)
//     //         .then(_consume);
//     //     }
//     //     else {
//     //         return;
//     //     }
//     // }
//
//     // console.log('starting');
//     // self._running = true;
//     // _consumeWhileRunning();
//     // console.log('afterc');
// }
//
// Consumer.prototype.stop = function() {
//     var self = this;
//     self._running = false;
//     console.log('stoppped: ', self._running);
// }


module.exports = {
    Consumer: Consumer
};