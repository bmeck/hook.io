
/*
 * hookio/actioner
 * Handles all actions. 'Sink' for actions.
 */

var hookIO = require('../index').hookIO,
  fs = require('fs');


var actions = exports.actions = {};

var updateDefinitions = exports.update = function(callback) {
  var result = {};
  fs.readdir(hookIO.PATH + '/definitions/actions', function(error, files) {
    files.forEach(function(action) {
      action = action.slice(0, -3);
      action = require(hookIO.PATH + '/definitions/actions/' + action);

      try {
        action = action.action;

        result[action.name] = action;
      } catch (error) {}
    });

    actions = exports.actions = result;

    if ('function' === typeof callback)
      callback(actions);

    hookIO.emit('ActionDefinitionsUpdated', actions);
  });
};

hookIO.addListener('ActionTrigger', function(hook, definition) {
  var protocol = definition.protocol[0].toUpperCase() + definition.substr(1);

  hookIO.db.getActionsForHook(hook, function(actions) {
    actions.forEach(function(action) {
      action.set('params', hook.get('params'));

      var actionDefinition = actions[action.get('type')]

      actionDefinition.handle(action, hook, definition);

      hookIO.emit(protocol + 'ActionTrigger', action, actionDefinition);
      hookIO.emit('HookCompleted', hook);
    });
  });
});


// Protocol specific stuff
require('./http');