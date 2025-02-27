'use strict';

const tls = require('tls');
const net = require('net');

const Parser = require('redis-parser');
const commands = require('redis-commands');

const EventEmitter = require('events');
const Queue = require('denque');

const utils = require('./lib/utils');
const Command = require('./lib/command');
const errorClasses = require('./lib/customErrors');
const unifyOptions = require('./lib/createClient');
const debug = require('./lib/debug');

const SUBSCRIBE_COMMANDS = {
    subscribe: true,
    unsubscribe: true,
    psubscribe: true,
    punsubscribe: true
};

const noop = () => {};

function handle_detect_buffers_reply (reply, command, buffer_args) {
    if (buffer_args === false || this.message_buffers) {
        // If detect_buffers option was specified, then the reply from the parser will be a buffer.
        // If this command did not use Buffer arguments, then convert the reply to Strings here.
        reply = utils.reply_to_strings(reply);
    }

    if (command === 'hgetall') {
        reply = utils.reply_to_object(reply);
    }

    return reply;
}

const deferredPromise = () => {
    let resolve = null;
    let reject = null;

    const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
    });

    return {
        promise,
        resolve,
        reject
    };
};

exports.debug_mode = /\bredis\b/i.test(process.env.NODE_DEBUG);

// Attention: The second parameter might be removed at will and is not officially supported.
// Do not rely on this
class RedisClient extends EventEmitter {
    constructor (options, stream) {
        super();

        // Copy the options so they are not mutated
        options = utils.clone(options);

        const cnx_options = {};
        const self = this;

        /* istanbul ignore next: travis does not work with stunnel atm. Therefore the tls tests are skipped on travis */
        for (const tls_option in options.tls) {
            cnx_options[tls_option] = options.tls[tls_option];
            // Copy the tls options into the general options to make sure the address is set right
            if (tls_option === 'port' || tls_option === 'host' || tls_option === 'path' || tls_option === 'family') {
                options[tls_option] = options.tls[tls_option];
            }
        }

        if (stream) {
            // The stream from the outside is used so no connection from this side is triggered but from the server this client should talk to
            // Reconnect etc won't work with this. This requires monkey patching to work, so it is not officially supported
            options.stream = stream;
            this.address = '"Private stream"';
        } else if (options.path) {
            cnx_options.path = options.path;
            this.address = options.path;
        } else {
            cnx_options.port = +options.port || 6379;
            cnx_options.host = options.host || '127.0.0.1';
            cnx_options.family = (!options.family && net.isIP(cnx_options.host)) || (options.family === 'IPv6' ? 6 : 4);
            this.address = `${cnx_options.host}:${cnx_options.port}`;
        }
        // Warn on misusing deprecated functions
        if (typeof options.retry_strategy === 'function') {
            if ('max_attempts' in options) {
                self.warn('WARNING: You activated the retry_strategy and max_attempts at the same time. This is not possible and max_attempts will be ignored.');
                // Do not print deprecation warnings twice
                delete options.max_attempts;
            }
            if ('retry_max_delay' in options) {
                self.warn('WARNING: You activated the retry_strategy and retry_max_delay at the same time. This is not possible and retry_max_delay will be ignored.');
                // Do not print deprecation warnings twice
                delete options.retry_max_delay;
            }
        }

        this.connection_options = cnx_options;
        this.connection_id = RedisClient.connection_id++;
        this.connected = false;
        this.ready = false;
        if (options.socket_nodelay === undefined) {
            options.socket_nodelay = true;
        } else if (!options.socket_nodelay) { // Only warn users with this set to false
            self.warn(
                'socket_nodelay is deprecated and will be removed in v.3.0.0.\n' +
                'Setting socket_nodelay to false likely results in a reduced throughput. Please use .batch for pipelining instead.\n' +
                'If you are sure you rely on the NAGLE-algorithm you can activate it by calling client.stream.setNoDelay(false) instead.'
            );
        }
        if (options.socket_keepalive === undefined) {
            options.socket_keepalive = true;
        }
        if (options.socket_initialdelay === undefined) {
            options.socket_initialdelay = 0;
            // set default to 0, which is aligned to https://nodejs.org/api/net.html#net_socket_setkeepalive_enable_initialdelay
        }

        for (const command in options.rename_commands) {
            options.rename_commands[command.toLowerCase()] = options.rename_commands[command];
        }

        options.return_buffers = !!options.return_buffers;
        options.detect_buffers = !!options.detect_buffers;

        // Override the detect_buffers setting if return_buffers is active and print a warning
        if (options.return_buffers && options.detect_buffers) {
            self.warn('WARNING: You activated return_buffers and detect_buffers at the same time. The return value is always going to be a buffer.');
            options.detect_buffers = false;
        }
        if (options.detect_buffers) {
            // We only need to look at the arguments if we do not know what we have to return
            RedisClient.handle_reply = handle_detect_buffers_reply;
        }

        this.should_buffer = false;

        this.max_attempts = options.max_attempts | 0;
        if ('max_attempts' in options) {
            self.warn(
                'max_attempts is deprecated and will be removed in v.3.0.0.\n' +
                'To reduce the number of options and to improve the reconnection handling please use the new `retry_strategy` option instead.\n' +
                'This replaces the max_attempts and retry_max_delay option.'
            );
        }

        this.command_queue = new Queue(); // Holds sent commands to de-pipeline them
        this.offline_queue = new Queue(); // Holds commands issued but not able to be sent
        this.pipeline_queue = new Queue(); // Holds all pipelined commands

        // ATTENTION: connect_timeout should change in v.3.0 so it does not count towards ending reconnection attempts after x seconds
        // This should be done by the retry_strategy. Instead it should only be the timeout for connecting to redis
        this.connect_timeout = +options.connect_timeout || 3600000; // 60 * 60 * 1000 ms
        this.enable_offline_queue = options.enable_offline_queue !== false;

        this.retry_max_delay = +options.retry_max_delay || null;
        if ('retry_max_delay' in options) {
            self.warn(
                'retry_max_delay is deprecated and will be removed in v.3.0.0.\n' +
                'To reduce the amount of options and the improve the reconnection handling please use the new `retry_strategy` option instead.\n' +
                'This replaces the max_attempts and retry_max_delay option.'
            );
        }

        this.initialize_retry_vars();

        this.pub_sub_mode = 0;
        this.subscription_set = {};

        this.monitoring = false;
        this.message_buffers = false;
        this.closing = false;
        // Determine if strings or buffers should be written to the stream
        this.fire_strings = true;
        this.pipeline = false;

        this.server_info = {};

        this.auth_pass = options.auth_pass || options.password;
        this.selected_db = options.db; // Save the selected db here, used when reconnecting

        this.sub_commands_left = 0;
        this.times_connected = 0;

        this.buffers = options.return_buffers || options.detect_buffers;
        this.options = options;

        this.reply = 'ON'; // Returning replies is the default

        this.create_stream();

        // The listeners will not be attached right away, so let's print the deprecation message while the listener is attached
        this.on('newListener', event => {
            if (event === 'idle') {
                this.warn(
                    'The idle event listener is deprecated and will likely be removed in v.3.0.0.\n' +
                    'If you rely on this feature please open a new ticket in node_redis with your use case'
                );
                return;
            }

            if (event === 'drain') {
                this.warn(
                    'The drain event listener is deprecated and will be removed in v.3.0.0.\n' +
                    'If you want to keep on listening to this event please listen to the stream drain event directly.'
                );

                return;
            }

            if ((event === 'message_buffer' || event === 'pmessage_buffer' || event === 'messageBuffer' || event === 'pmessageBuffer') && !this.buffers && !this.message_buffers) {
                if (this.reply_parser.name !== 'javascript') {
                    return this.warn(
                        `You attached the "${event}" listener without the returnBuffers option set to true.\n` +
                        'Please use the JavaScript parser or set the returnBuffers option to true to return buffers.'
                    );
                }
                this.reply_parser.optionReturnBuffers = true;
                this.message_buffers = true;
                RedisClient.handle_reply = handle_detect_buffers_reply;

                return;
            }
        });
    }

    /******************************************************************************

     All functions in here are internal besides the RedisClient constructor
     and the exported functions. Don't rely on them as they will be private
     functions in node_redis v.3

     ******************************************************************************/

    // Attention: the function name "create_stream" should not be changed, as other libraries need this to mock the stream (e.g. fakeredis)
    create_stream () {
        const self = this;

        // Init parser
        this.reply_parser = create_parser(this);

        if (this.options.stream) {
            // Only add the listeners once in case of a reconnect try (that won't work)
            if (this.stream) {
                return;
            }
            this.stream = this.options.stream;
        } else {
            // On a reconnect destroy the former stream and retry
            if (this.stream) {
                this.stream.removeAllListeners();
                this.stream.destroy();
            }

            /* istanbul ignore if: travis does not work with stunnel atm. Therefore the tls tests are skipped on travis */
            if (this.options.tls) {
                this.stream = tls.connect(this.connection_options);
            } else {
                this.stream = net.createConnection(this.connection_options);
            }
        }

        if (this.options.connect_timeout) {
            this.stream.setTimeout(this.connect_timeout, () => {
                // Note: This is only tested if a internet connection is established
                self.retry_totaltime = self.connect_timeout;
                self.connection_gone('timeout');
            });
        }

        /* istanbul ignore next: travis does not work with stunnel atm. Therefore the tls tests are skipped on travis */
        const connect_event = this.options.tls ? 'secureConnect' : 'connect';
        this.stream.once(connect_event, function () {
            this.removeAllListeners('timeout');
            self.times_connected++;
            self.on_connect();
        });

        this.stream.on('data', buffer_from_socket => {
            // The buffer_from_socket.toString() has a significant impact on big chunks and therefore this should only be used if necessary
            debug(`Net read ${self.address} id ${self.connection_id}`); // + ': ' + buffer_from_socket.toString());
            self.reply_parser.execute(buffer_from_socket);
            self.emit_idle();
        });

        this.stream.on('error', err => {
            self.on_error(err);
        });

        this.stream.once('close', hadError => {
            self.connection_gone('close');
        });

        this.stream.once('end', () => {
            self.connection_gone('end');
        });

        this.stream.on('drain', () => {
            self.drain();
        });

        if (this.options.socket_nodelay) {
            this.stream.setNoDelay();
        }

        // Fire the command before redis is connected to be sure it's the first fired command
        if (this.auth_pass !== undefined) {
            this.ready = true;
            // Fail silently as we might not be able to connect
            this.auth(this.auth_pass, err => {
                if (err && err.code !== 'UNCERTAIN_STATE') {
                    self.emit('error', err);
                }
            });
            this.ready = false;
        }
    }

    static handle_reply (reply, command) {
        if (command === 'hgetall') {
            reply = utils.reply_to_object(reply);
        }
        return reply;
    }

    initialize_retry_vars () {
        this.retry_timer = null;
        this.retry_totaltime = 0;
        this.retry_delay = 200;
        this.retry_backoff = 1.7;
        this.attempts = 1;
    }

    warn (msg) {
        const self = this;
        // Warn on the next tick. Otherwise no event listener can be added
        // for warnings that are emitted in the redis client constructor
        process.nextTick(() => {
            if (self.listeners('warning').length !== 0) {
                self.emit('warning', msg);
            } else {
                console.warn('node_redis:', msg);
            }
        });
    }

    // Flush provided queues, erroring any items with a callback first
    flush_and_error (error_attributes, options = {}) {
        const aggregated_errors = [];
        // Flush the command_queue first to keep the order intact
        const queue_names = options.queues || ['command_queue', 'offline_queue'];

        for (let i = 0; i < queue_names.length; i++) {
            // If the command was fired it might have been processed so far
            if (queue_names[i] === 'command_queue') {
                error_attributes.message += ' It might have been processed.';
            } else { // As the command_queue is flushed first, remove this for the offline queue
                error_attributes.message = error_attributes.message.replace(' It might have been processed.', '');
            }

            // Don't flush everything from the queue
            for (
                let command_obj = this[queue_names[i]].shift();
                command_obj;
                command_obj = this[queue_names[i]].shift()
            ) {
                const err = new errorClasses.AbortError(error_attributes);

                if (command_obj.error) {
                    err.stack = err.stack + command_obj.error.stack.replace(/^Error.*?\n/, '\n');
                }

                err.command = command_obj.command.toUpperCase();

                if (command_obj.args && command_obj.args.length) {
                    err.args = command_obj.args;
                }

                if (options.error) {
                    err.origin = options.error;
                }

                if (typeof command_obj.callback === 'function') {
                    command_obj.callback(err);
                } else {
                    aggregated_errors.push(err);
                }
            }
        }
        // Currently this would be a breaking change, therefore it's only emitted in debug_mode
        if (exports.debug_mode && aggregated_errors.length) {
            let error;
            if (aggregated_errors.length === 1) {
                error = aggregated_errors[0];
            } else {
                error_attributes.message = error_attributes.message.replace('It', 'They').replace(/command/i, '$&s');
                error = new errorClasses.AggregateError(error_attributes);
                error.errors = aggregated_errors;
            }
            this.emit('error', error);
        }
    }

    on_error (err) {
        if (this.closing) {
            return;
        }

        err.message = `Redis connection to ${this.address} failed - ${err.message}`;
        debug(err.message);
        this.connected = false;
        this.ready = false;

        // Only emit the error if the retry_stategy option is not set
        if (!this.options.retry_strategy) {
            this.emit('error', err);
        }
        // 'error' events get turned into exceptions if they aren't listened for. If the user handled this error
        // then we should try to reconnect.
        this.connection_gone('error', err);
    }

    on_connect () {
        debug(`Stream connected ${this.address} id ${this.connection_id}`);

        this.connected = true;
        this.ready = false;
        this.emitted_end = false;
        this.stream.setKeepAlive(this.options.socket_keepalive, this.options.socket_initialdelay);
        this.stream.setTimeout(0);

        this.emit('connect');
        this.initialize_retry_vars();

        if (this.options.no_ready_check) {
            this.on_ready();
        } else {
            this.ready_check();
        }
    }

    on_ready () {
        const self = this;

        debug(`on_ready called ${this.address} id ${this.connection_id}`);
        this.ready = true;

        this.cork = () => {
            self.pipeline = true;
            if (self.stream.cork) {
                self.stream.cork();
            }
        };
        this.uncork = () => {
            if (self.fire_strings) {
                self.write_strings();
            } else {
                self.write_buffers();
            }
            self.pipeline = false;
            self.fire_strings = true;
            if (self.stream.uncork) {
                // TODO: Consider using next tick here. See https://github.com/NodeRedis/node_redis/issues/1033
                self.stream.uncork();
            }
        };

        // Restore modal commands from previous connection. The order of the commands is important
        if (this.selected_db !== undefined) {
            this.internal_send_command(new Command('select', [this.selected_db]));
        }
        if (this.monitoring) { // Monitor has to be fired before pub sub commands
            this.internal_send_command(new Command('monitor', []));
        }
        let callback_count = Object.keys(this.subscription_set).length;
        if (!this.options.disable_resubscribing && callback_count) {
            // only emit 'ready' when all subscriptions were made again
            // TODO: Remove the countdown for ready here. This is not coherent with all other modes and should therefore not be handled special
            // We know we are ready as soon as all commands were fired
            const callback = () => {
                callback_count--;
                if (callback_count === 0) {
                    self.emit('ready');
                }
            };
            debug('Sending pub/sub on_ready commands');
            for (const key in this.subscription_set) {
                const command = key.slice(0, key.indexOf('_'));
                const args = this.subscription_set[key];
                this[command]([args], callback);
            }
            this.send_offline_queue();
            return;
        }
        this.send_offline_queue();
        this.emit('ready');
    }

    on_info_cmd (err, res) {
        if (err) {
            if (err.message === "ERR unknown command 'info'") {
                this.on_ready();
                return;
            }
            err.message = `Ready check failed: ${err.message}`;
            this.emit('error', err);
            return;
        }

        /* istanbul ignore if: some servers might not respond with any info data. This is just a safety check that is difficult to test */
        if (!res) {
            debug('The info command returned without any data.');
            this.on_ready();
            return;
        }

        if (!this.server_info.loading || this.server_info.loading === '0') {
            // If the master_link_status exists but the link is not up, try again after 50 ms
            if (this.server_info.master_link_status && this.server_info.master_link_status !== 'up') {
                this.server_info.loading_eta_seconds = 0.05;
            } else {
                // Eta loading should change
                debug('Redis server ready.');
                this.on_ready();
                return;
            }
        }

        let retry_time = +this.server_info.loading_eta_seconds * 1000;
        if (retry_time > 1000) {
            retry_time = 1000;
        }
        debug(`Redis server still loading, trying again in ${retry_time}`);
        setTimeout(self => {
            self.ready_check();
        }, retry_time, this);
    }

    ready_check () {
        const self = this;
        debug('Checking server ready state...');
        // Always fire this info command as first command even if other commands are already queued up
        this.ready = true;
        this.info((err, res) => {
            self.on_info_cmd(err, res);
        });
        this.ready = false;
    }

    send_offline_queue () {
        for (let command_obj = this.offline_queue.shift(); command_obj; command_obj = this.offline_queue.shift()) {
            debug(`Sending offline command: ${command_obj.command}`);
            this.internal_send_command(command_obj);
        }
        this.drain();
    }

    connection_gone (why, error) {
        // If a retry is already in progress, just let that happen
        if (this.retry_timer) {
            return;
        }
        error = error || null;

        debug(`Redis connection is gone from ${why} event.`);
        this.connected = false;
        this.ready = false;
        // Deactivate cork to work with the offline queue
        this.cork = noop;
        this.uncork = noop;
        this.pipeline = false;
        this.pub_sub_mode = 0;

        // since we are collapsing end and close, users don't expect to be called twice
        if (!this.emitted_end) {
            this.emit('end');
            this.emitted_end = true;
        }

        // If this is a requested shutdown, then don't retry
        if (this.closing) {
            debug('Connection ended by quit / end command, not retrying.');
            this.flush_and_error({
                message: 'Stream connection ended and command aborted.',
                code: 'NR_CLOSED'
            }, {
                error
            });
            return;
        }

        if (typeof this.options.retry_strategy === 'function') {
            const retry_params = {
                attempt: this.attempts,
                error
            };
            if (this.options.camel_case) {
                retry_params.totalRetryTime = this.retry_totaltime;
                retry_params.timesConnected = this.times_connected;
            } else {
                retry_params.total_retry_time = this.retry_totaltime;
                retry_params.times_connected = this.times_connected;
            }
            this.retry_delay = this.options.retry_strategy(retry_params);
            if (typeof this.retry_delay !== 'number') {
                // Pass individual error through
                if (this.retry_delay instanceof Error) {
                    error = this.retry_delay;
                }
                this.flush_and_error({
                    message: 'Stream connection ended and command aborted.',
                    code: 'NR_CLOSED'
                }, {
                    error
                });
                this.end(false);
                return;
            }
        }

        if (this.max_attempts !== 0 && this.attempts >= this.max_attempts || this.retry_totaltime >= this.connect_timeout) {
            let message = 'Redis connection in broken state: ';
            if (this.retry_totaltime >= this.connect_timeout) {
                message += 'connection timeout exceeded.';
            } else {
                message += 'maximum connection attempts exceeded.';
            }

            this.flush_and_error({
                message,
                code: 'CONNECTION_BROKEN',
            }, {
                error
            });
            const err = new Error(message);
            err.code = 'CONNECTION_BROKEN';
            if (error) {
                err.origin = error;
            }
            this.emit('error', err);
            this.end(false);
            return;
        }

        // Retry commands after a reconnect instead of throwing an error. Use this with caution
        if (this.options.retry_unfulfilled_commands) {
            this.offline_queue.unshift.apply(this.offline_queue, this.command_queue.toArray());
            this.command_queue.clear();
        } else if (this.command_queue.length !== 0) {
            this.flush_and_error({
                message: 'Redis connection lost and command aborted.',
                code: 'UNCERTAIN_STATE'
            }, {
                error,
                queues: ['command_queue']
            });
        }

        if (this.retry_max_delay !== null && this.retry_delay > this.retry_max_delay) {
            this.retry_delay = this.retry_max_delay;
        } else if (this.retry_totaltime + this.retry_delay > this.connect_timeout) {
            // Do not exceed the maximum
            this.retry_delay = this.connect_timeout - this.retry_totaltime;
        }

        debug(`Retry connection in ${this.retry_delay} ms`);

        this.retry_timer = setTimeout(retry_connection, this.retry_delay, this, error);
    }

    return_error (err) {
        const command_obj = this.command_queue.shift();
        if (command_obj.error) {
            err.stack = command_obj.error.stack.replace(/^Error.*?\n/, `ReplyError: ${err.message}\n`);
        }
        err.command = command_obj.command.toUpperCase();
        if (command_obj.args && command_obj.args.length) {
            err.args = command_obj.args;
        }

        // Count down pub sub mode if in entering modus
        if (this.pub_sub_mode > 1) {
            this.pub_sub_mode--;
        }

        const match = err.message.match(utils.err_code);
        // LUA script could return user errors that don't behave like all other errors!
        if (match) {
            err.code = match[1];
        }

        utils.callback_or_emit(this, command_obj.callback, err);
    }

    drain () {
        this.emit('drain');
        this.should_buffer = false;
    }

    emit_idle () {
        if (this.command_queue.length === 0 && this.pub_sub_mode === 0) {
            this.emit('idle');
        }
    }

    return_reply (reply) {
        if (this.monitoring) {
            let replyStr;
            if (this.buffers && Buffer.isBuffer(reply)) {
                replyStr = reply.toString();
            } else {
                replyStr = reply;
            }
            // If in monitor mode, all normal commands are still working and we only want to emit the streamlined commands
            if (typeof replyStr === 'string' && utils.monitor_regex.test(replyStr)) {
                const timestamp = replyStr.slice(0, replyStr.indexOf(' '));
                const args = replyStr.slice(replyStr.indexOf('"') + 1, -1).split('" "').map(elem => elem.replace(/\\"/g, '"'));
                this.emit('monitor', timestamp, args, replyStr);
                return;
            }
        }
        if (this.pub_sub_mode === 0) {
            normal_reply(this, reply);
        } else if (this.pub_sub_mode !== 1) {
            this.pub_sub_mode--;
            normal_reply(this, reply);
        } else if (!(reply instanceof Array) || reply.length <= 2) {
            // Only PING and QUIT are allowed in this context besides the pub sub commands
            // Ping replies with ['pong', null|value] and quit with 'OK'
            normal_reply(this, reply);
        } else {
            return_pub_sub(this, reply);
        }
    }

    // Do not call internal_send_command directly, if you are not absolutly certain it handles everything properly
    // e.g. monitor / info does not work with internal_send_command only
    internal_send_command (command_obj) {
        let arg;
        let prefix_keys;

        let i = 0;
        let command_str = '';

        const args = command_obj.args;
        let command = command_obj.command;

        const len = args.length;
        let big_data = false;
        const args_copy = new Array(len);

        // Will contain the deferred promise instance
        // and will only be used if no callback is passed
        let out_deferred = null;

        if (process.domain && command_obj.callback) {
            command_obj.callback = process.domain.bind(command_obj.callback);
        }

        if (!command_obj.callback) {
            out_deferred = deferredPromise();

            command_obj.callback = (err, data) => {
                if (err) {
                    out_deferred.reject(err);
                    return;
                }

                out_deferred.resolve(data);
            };
        }

        if (this.ready === false || this.stream.writable === false) {
            // Handle offline commands right away
            handle_offline_command(this, command_obj);

            if (out_deferred) {
                return out_deferred.promise;
            }

            // Indicate buffering
            return false;
        }

        for (i = 0; i < len; i += 1) {
            if (typeof args[i] === 'string') {
                // 30000 seemed to be a good value to switch to buffers after testing and checking the pros and cons
                if (args[i].length > 30000) {
                    big_data = true;
                    args_copy[i] = new Buffer(args[i], 'utf8');
                } else {
                    args_copy[i] = args[i];
                }

                continue;
            }

            // Checking for object instead of Buffer.isBuffer helps us finding data types that we can't handle properly
            if (typeof args[i] === 'object') {
                if (args[i] instanceof Date) { // Accept dates as valid input
                    args_copy[i] = args[i].toString();

                    continue;
                }

                if (args[i] === null) {
                    this.warn(
                        `Deprecated: The ${command.toUpperCase()} command contains a "null" argument.\n` +
                        'This is converted to a "null" string now and will return an error from v.3.0 on.\n' +
                        'Please handle this in your code to make sure everything works as you intended it to.'
                    );

                    args_copy[i] = 'null'; // Backwards compatible :/

                    continue;
                }

                if (Buffer.isBuffer(args[i])) {
                    args_copy[i] = args[i];
                    command_obj.buffer_args = true;
                    big_data = true;

                    continue;
                }

                this.warn(
                    `Deprecated: The ${command.toUpperCase()} command contains a argument of type ${args[i].constructor.name}.\n` +
                    'This is converted to "${args[i].toString()}" by using .toString() now and will return an error from v.3.0 on.\n' +
                    'Please handle this in your code to make sure everything works as you intended it to.'
                );

                args_copy[i] = args[i].toString(); // Backwards compatible :/

                continue;
            }

            if (typeof args[i] === 'undefined') {
                this.warn(
                    `Deprecated: The ${command.toUpperCase()} command contains a "undefined" argument.\n` +
                    'This is converted to a "undefined" string now and will return an error from v.3.0 on.\n' +
                    'Please handle this in your code to make sure everything works as you intended it to.'
                );

                args_copy[i] = 'undefined'; // Backwards compatible :/

                continue;
            }

            // Seems like numbers are converted fast using string concatenation
            args_copy[i] = `${args[i]}`;
        }

        if (this.options.prefix) {
            prefix_keys = commands.getKeyIndexes(command, args_copy);

            for (i = prefix_keys.pop(); i !== undefined; i = prefix_keys.pop()) {
                args_copy[i] = this.options.prefix + args_copy[i];
            }
        }

        if (this.options.rename_commands && this.options.rename_commands[command]) {
            command = this.options.rename_commands[command];
        }

        // Always use 'Multi bulk commands', but if passed any Buffer args, then do multiple writes, one for each arg.
        // This means that using Buffers in commands is going to be slower, so use Strings if you don't already have a Buffer.
        command_str = `*${len + 1}\r\n$${command.length}\r\n${command}\r\n`;

        // Build up a string and send entire command in one write
        if (big_data) {
            debug(`Send command (${command_str}) has Buffer arguments`);

            this.fire_strings = false;
            this.write(command_str);

            for (i = 0; i < len; ++i) {
                arg = args_copy[i];

                if (typeof arg === 'string') {
                    this.write(`$${Buffer.byteLength(arg)}\r\n${arg}\r\n`);
                } else { // buffer
                    this.write(`$${arg.length}\r\n`);
                    this.write(arg);
                    this.write('\r\n');
                }

                debug(`send_command: buffer send ${arg.length} bytes`);
            }
        } else {
            for (i = 0; i < len; ++i) {
                arg = args_copy[i];

                command_str += `$${Buffer.byteLength(arg)}\r\n${arg}\r\n`;
            }

            debug(`Send ${this.address} id ${this.connection_id}: ${command_str}`);
            this.write(command_str);
        }

        if (command_obj.call_on_write) {
            command_obj.call_on_write();
        }

        // Handle `CLIENT REPLY ON|OFF|SKIP`
        // This has to be checked after call_on_write
        /* istanbul ignore else: TODO: Remove this as soon as we test Redis 3.2 on travis */
        if (this.reply === 'ON') {
            this.command_queue.push(command_obj);
        } else {
            // Do not expect a reply
            // Does this work in combination with the pub sub mode?
            if (command_obj.callback) {
                utils.reply_in_order(this, command_obj.callback, null, undefined, this.command_queue);
            }
            if (this.reply === 'SKIP') {
                this.reply = 'SKIP_ONE_MORE';
            } else if (this.reply === 'SKIP_ONE_MORE') {
                this.reply = 'ON';
            }
        }

        if (out_deferred) {
            return out_deferred.promise;
        }

        return !this.should_buffer;
    }

    write_strings () {
        let str = '';
        for (let command = this.pipeline_queue.shift(); command; command = this.pipeline_queue.shift()) {
            // Write to stream if the string is bigger than 4mb. The biggest string may be Math.pow(2, 28) - 15 chars long
            if (str.length + command.length > 4 * 1024 * 1024) {
                this.should_buffer = !this.stream.write(str);
                str = '';
            }
            str += command;
        }
        if (str !== '') {
            this.should_buffer = !this.stream.write(str);
        }
    }

    write_buffers () {
        for (let command = this.pipeline_queue.shift(); command; command = this.pipeline_queue.shift()) {
            this.should_buffer = !this.stream.write(command);
        }
    }

    write (data) {
        if (this.pipeline === false) {
            this.should_buffer = !this.stream.write(data);
            return;
        }
        this.pipeline_queue.push(data);
    }

    // Don't officially expose the command_queue directly but only the length as read only variable
    get command_queue_length () {
        return this.command_queue.length;
    }

    get offline_queue_length () {
        return this.offline_queue.length;
    }

    // Add support for camelCase by adding read only properties to the client
    // All known exposed snake_case variables are added here
    get retryDelay () {
        return this.retry_delay;
    }

    get retryBackoff () {
        return this.retry_backoff;
    }

    get commandQueueLength () {
        return this.command_queue.length;
    }

    get offlineQueueLength () {
        return this.offline_queue.length;
    }

    get shouldBuffer () {
        return this.should_buffer;
    }

    get connectionId () {
        return this.connection_id;
    }

    get serverInfo () {
        return this.server_info;
    }
}

RedisClient.connection_id = 0;

function create_parser (self) {
    return new Parser({
        returnReply (data) {
            self.return_reply(data);
        },
        returnError (err) {
            // Return a ReplyError to indicate Redis returned an error
            self.return_error(err);
        },
        returnFatalError (err) {
            // Error out all fired commands. Otherwise they might rely on faulty data. We have to reconnect to get in a working state again
            // Note: the execution order is important. First flush and emit, then create the stream
            err.message += '. Please report this.';
            self.ready = false;

            self.flush_and_error({
                message: 'Fatal error encountered. Command aborted.',
                code: 'NR_FATAL'
            }, {
                error: err,
                queues: ['command_queue']
            });

            self.emit('error', err);
            self.create_stream();
        },
        returnBuffers: self.buffers || self.message_buffers,
        name: self.options.parser || 'javascript',
        stringNumbers: self.options.string_numbers || false
    });
}

RedisClient.prototype.cork = noop;
RedisClient.prototype.uncork = noop;

const retry_connection = (self, error) => {
    debug('Retrying connection...');

    const reconnect_params = {
        delay: self.retry_delay,
        attempt: self.attempts,
        error
    };

    if (self.options.camel_case) {
        reconnect_params.totalRetryTime = self.retry_totaltime;
        reconnect_params.timesConnected = self.times_connected;
    } else {
        reconnect_params.total_retry_time = self.retry_totaltime;
        reconnect_params.times_connected = self.times_connected;
    }

    self.emit('reconnecting', reconnect_params);

    self.retry_totaltime += self.retry_delay;
    self.attempts += 1;
    self.retry_delay = Math.round(self.retry_delay * self.retry_backoff);
    self.create_stream();
    self.retry_timer = null;
};

function normal_reply (self, reply) {
    const command_obj = self.command_queue.shift();
    if (typeof command_obj.callback === 'function') {
        if (command_obj.command !== 'exec') {
            reply = RedisClient.handle_reply(reply, command_obj.command, command_obj.buffer_args);
        }
        command_obj.callback(null, reply);
    } else {
        debug('No callback for reply');
    }
}

function subscribe_unsubscribe (self, reply, type) {
    // Subscribe commands take an optional callback and also emit an event, but only the _last_ response is included in the callback
    // The pub sub commands return each argument in a separate return value and have to be handled that way
    const command_obj = self.command_queue.get(0);
    const buffer = self.options.return_buffers || self.options.detect_buffers && command_obj.buffer_args;
    const channel = (buffer || reply[1] === null) ? reply[1] : reply[1].toString();
    const count = +reply[2]; // Return the channel counter as number no matter if `string_numbers` is activated or not

    debug(type, channel);

    // Emit first, then return the callback
    if (channel !== null) { // Do not emit or "unsubscribe" something if there was no channel to unsubscribe from
        self.emit(type, channel, count);

        if (type === 'subscribe' || type === 'psubscribe') {
            self.subscription_set[`${type}_${channel}`] = channel;
        } else {
            type = type === 'unsubscribe' ? 'subscribe' : 'psubscribe'; // Make types consistent
            delete self.subscription_set[`${type}_${channel}`];
        }
    }

    if (command_obj.args.length === 1 || self.sub_commands_left === 1 || command_obj.args.length === 0 && (count === 0 || channel === null)) {
        if (count === 0) { // unsubscribed from all channels
            let running_command;
            let i = 1;

            self.pub_sub_mode = 0; // Deactivating pub sub mode

            // This should be a rare case and therefore handling it this way should be good performance wise for the general case
            while (running_command = self.command_queue.get(i)) {
                if (SUBSCRIBE_COMMANDS[running_command.command]) {
                    self.pub_sub_mode = i; // Entering pub sub mode again
                    break;
                }

                i++;
            }
        }

        self.command_queue.shift();

        if (typeof command_obj.callback === 'function') {
            // TODO: The current return value is pretty useless.
            // Evaluate to change this in v.3 to return all subscribed / unsubscribed channels in an array including the number of channels subscribed too
            command_obj.callback(null, channel);
        }

        self.sub_commands_left = 0;

        return;
    }

    if (self.sub_commands_left !== 0) {
        self.sub_commands_left--;
    } else {
        self.sub_commands_left = command_obj.args.length ? command_obj.args.length - 1 : count;
    }
}

function return_pub_sub (self, reply) {
    const type = reply[0].toString();
    if (type === 'message') { // channel, message
        if (!self.options.return_buffers || self.message_buffers) { // backwards compatible. Refactor this in v.3 to always return a string on the normal emitter
            self.emit('message', reply[1].toString(), reply[2].toString());
            self.emit('message_buffer', reply[1], reply[2]);
            self.emit('messageBuffer', reply[1], reply[2]);
        } else {
            self.emit('message', reply[1], reply[2]);
        }

        return;
    }

    if (type === 'pmessage') { // pattern, channel, message
        if (!self.options.return_buffers || self.message_buffers) { // backwards compatible. Refactor this in v.3 to always return a string on the normal emitter
            self.emit('pmessage', reply[1].toString(), reply[2].toString(), reply[3].toString());
            self.emit('pmessage_buffer', reply[1], reply[2], reply[3]);
            self.emit('pmessageBuffer', reply[1], reply[2], reply[3]);
        } else {
            self.emit('pmessage', reply[1], reply[2], reply[3]);
        }

        return;
    }

    subscribe_unsubscribe(self, reply, type);
}

function handle_offline_command (self, command_obj) {
    let command = command_obj.command;

    let err;
    let msg;

    if (self.closing || !self.enable_offline_queue) {
        command = command.toUpperCase();

        if (!self.closing) {
            if (self.stream.writable) {
                msg = 'The connection is not yet established and the offline queue is deactivated.';
            } else {
                msg = 'Stream not writeable.';
            }
        } else {
            msg = 'The connection is already closed.';
        }

        err = new errorClasses.AbortError({
            message: `${command} can't be processed. ${msg}`,
            code: 'NR_CLOSED',
            command
        });

        if (command_obj.args.length) {
            err.args = command_obj.args;
        }

        utils.reply_in_order(self, command_obj.callback, err);

        return;
    }

    debug(`Queueing ${command} for next server connection.`);
    self.offline_queue.push(command_obj);

    self.should_buffer = true;
}

Object.defineProperty(exports, 'debugMode', {
    get () {
        return this.debug_mode;
    },
    set (val) {
        this.debug_mode = val;
    }
});

exports.createClient = function createClient (...args) {
    return new RedisClient(unifyOptions.apply(null, args));
};

exports.RedisClient = RedisClient;
exports.print = utils.print;
exports.Multi = require('./lib/multi');
exports.AbortError = errorClasses.AbortError;
exports.RedisError = Parser.RedisError;
exports.ParserError = Parser.ParserError;
exports.ReplyError = Parser.ReplyError;
exports.AggregateError = errorClasses.AggregateError;

// Add all redis commands / node_redis api to the client
require('./lib/individualCommands');
require('./lib/extendedApi');

//enables adding new commands (for modules and new commands)
exports.addCommand = exports.add_command = require('./lib/commands');
