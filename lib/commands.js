'use strict';

var commands = require('redis-commands');
var Multi = require('./multi');
var RedisClient = require('../').RedisClient;
var Command = require('./command');
const {prepare_arguments_variable_length} = require('./utils');

// Feature detect if a function may change it's name
const changeFunctionName = (() => {
    const fn = function abc () {};

    try {
        Object.defineProperty(fn, 'name', {
            value: 'foobar'
        });

        return true;
    } catch (e) {
        return false;
    }
})();

const addCommand = command => {
    // Some rare Redis commands use special characters in their command name
    // Convert those to a underscore to prevent using invalid function names
    const commandName = command.replace(/(?:^([0-9])|[^a-zA-Z0-9_$])/g, '_$1');

    // Do not override existing functions
    if (!RedisClient.prototype[command]) {
        RedisClient.prototype[command.toUpperCase()] = RedisClient.prototype[command] = function (...args) {
            const [arr, callback] = prepare_arguments_variable_length(args, false);

            return this.internal_send_command(new Command(command, arr, callback));
        };

        // Alias special function names (e.g. NR.RUN becomes NR_RUN and nr_run)
        if (commandName !== command) {
            RedisClient.prototype[commandName.toUpperCase()] = RedisClient.prototype[commandName] = RedisClient.prototype[command];
        }

        if (changeFunctionName) {
            Object.defineProperty(RedisClient.prototype[command], 'name', {
                value: commandName
            });
        }
    }

    // Do not override existing functions
    if (!Multi.prototype[command]) {
        Multi.prototype[command.toUpperCase()] = Multi.prototype[command] = function (...args) {
            const [arr, callback] = prepare_arguments_variable_length(args, false);

            this.queue.push(new Command(command, arr, callback));
            return this;
        };
        // Alias special function names (e.g. NR.RUN becomes NR_RUN and nr_run)
        if (commandName !== command) {
            Multi.prototype[commandName.toUpperCase()] = Multi.prototype[commandName] = Multi.prototype[command];
        }
        if (changeFunctionName) {
            Object.defineProperty(Multi.prototype[command], 'name', {
                value: commandName
            });
        }
    }
};

commands.list.forEach(addCommand);

module.exports = addCommand;
