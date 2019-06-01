'use strict';

// hgetall converts its replies to an Object. If the reply is empty, null is returned.
// These function are only called with internal data and have therefore always the same instanceof X
function reply_to_object (reply) {
    // The reply might be a string or a buffer if this is called in a transaction (multi)
    if (reply.length === 0 || !(reply instanceof Array)) {
        return null;
    }

    const obj = {};

    for (let i = 0; i < reply.length; i += 2) {
        obj[reply[i].toString('binary')] = reply[i + 1];
    }

    return obj;
}

function reply_to_strings (reply) {
    if (reply instanceof Buffer) {
        return reply.toString();
    }

    if (reply instanceof Array) {
        const res = new Array(reply.length);

        for (let i = 0; i < reply.length; i++) {
            // Recusivly call the function as slowlog returns deep nested replies
            res[i] = reply_to_strings(reply[i]);
        }

        return res;
    }

    return reply;
}

function print (err, reply) {
    if (err) {
        // A error always begins with Error:
        console.log(err.toString());
        return;
    }

    console.log(`Reply: ${reply}`);
}

let camelCase;
// Deep clone arbitrary objects with arrays. Can't handle cyclic structures (results in a range error)
// Any attribute with a non primitive value besides object and array will be passed by reference (e.g. Buffers, Maps, Functions)
// All capital letters are going to be replaced with a lower case letter and a underscore infront of it
function clone (obj) {
    if (Array.isArray(obj)) {
        const copy = new Array(obj.length);

        for (let i = 0; i < obj.length; i++) {
            copy[i] = clone(obj[i]);
        }

        return copy;
    }

    if (Object.prototype.toString.call(obj) === '[object Object]') {
        const copy = {};

        const elems = Object.keys(obj);

        let elem;
        while (elems.length) {
            elem = elems.pop();

            if (elem === 'tls') { // special handle tls
                copy[elem] = obj[elem];
                continue;
            }

            // Accept camelCase options and convert them to snake_case
            const snake_case = elem.replace(/[A-Z][^A-Z]/g, '_$&').toLowerCase();

            // If camelCase is detected, pass it to the client, so all variables are going to be camelCased
            // There are no deep nested options objects yet, but let's handle this future proof
            if (snake_case !== elem.toLowerCase()) {
                camelCase = true;
            }

            copy[snake_case] = clone(obj[elem]);
        }

        return copy;
    }

    return obj;
}

function convenienceClone (obj) {
    camelCase = false;
    obj = clone(obj) || {};
    if (camelCase) {
        obj.camel_case = true;
    }
    return obj;
}

function callback_or_emit (self, callback, err, res) {
    if (callback) {
        callback(err, res);
    } else if (err) {
        self.emit('error', err);
    }
}

function reply_in_order (self, callback, err, res, queue) {
    // If the queue is explicitly passed, use that, otherwise fall back to the offline queue first,
    // as there might be commands in both queues at the same time
    let command_obj;

    /* istanbul ignore if: TODO: Remove this as soon as we test Redis 3.2 on travis */
    if (queue) {
        command_obj = queue.peekBack();
    } else {
        command_obj = self.offline_queue.peekBack() || self.command_queue.peekBack();
    }

    if (!command_obj) {
        process.nextTick(() => {
            callback_or_emit(self, callback, err, res);
        });
        return;
    }

    if (command_obj.callback) {
        const fn = command_obj.callback;

        command_obj.callback = (err, data) => {
            fn(err, data);

            callback_or_emit(self, callback, err, res);
        };

        return;
    }

    command_obj.callback = (err, data) => {
        if (err) {
            self.emit('error', err);
        }

        callback_or_emit(self, callback, err, res);
    };
}

const prepare_arguments_variable_length = (args, withObject) => {
    let len = arguments.length;

    let arr;
    let callback;

    let i = 0;

    if (Array.isArray(args[0])) {
        arr = args[0];
        callback = args[1];

        return [arr, callback];
    }

    if (Array.isArray(args[1])) {
        if (len === 3) {
            callback = args[2];
        }

        len = args[1].length;
        arr = new Array(len + 1);

        arr[0] = args[0];

        for (; i < len; ++i) {
            arr[i + 1] = args[1][i];
        }

        return [arr, callback];
    }

    if (
        withObject
        && typeof args[1] === 'object'
        && (
            args.length === 2
            || args.length === 3
            && (
                typeof args[2] === 'function'
                || typeof args[2] === 'undefined'
            )
        )
    ) {
        arr = [args[0]];

        for (const field in args[1]) {
            arr.push(field, args[1][field]);
        }

        callback = args[2];

        return [arr, callback];
    }

    len = args.length;

    // The later should not be the average use case
    if (
        len !== 0
        && (
            typeof args[len - 1] === 'function'
            || typeof args[len - 1] === 'undefined'
        )
    ) {
        len--;
        callback = args[len];
    }

    arr = new Array(len);

    for (; i < len; i += 1) {
        arr[i] = args[i];
    }

    return [arr, callback];
};

const prepare_arguments_optional = (args) => {
    let len = args.length;

    let arr;
    let callback;

    let i = 0;

    if (Array.isArray(args[0])) {
        arr = args[0].slice(0);
        callback = args[1];

        return [arr, callback];
    }

    len = args.length;

    // The later should not be the average use case
    if (
        len !== 0
        && (
            typeof args[len - 1] === 'function'
            || typeof args[len - 1] === 'undefined'
        )
    ) {
        len--;
        callback = args[len];
    }

    arr = new Array(len);

    for (; i < len; i += 1) {
        arr[i] = args[i];
    }

    return [arr, callback];
};

module.exports = {
    err_code: /^([A-Z]+)\s+(.+)$/,
    monitor_regex: /^[0-9]{10,11}\.[0-9]+ \[[0-9]+ .+\]( ".+?")+$/,

    reply_to_strings,
    reply_to_object,
    print,
    callback_or_emit,
    reply_in_order,

    prepare_arguments_variable_length,
    prepare_arguments_optional,

    clone: convenienceClone,
};
