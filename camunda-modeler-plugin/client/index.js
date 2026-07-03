'use strict';

var registerBpmnJSPlugin = require('camunda-modeler-plugin-helpers').registerBpmnJSPlugin;
var DurgaPropertiesProvider = require('./DurgaPropertiesProvider');

module.exports = {
  __init__: ['durgaPropertiesProvider'],
  durgaPropertiesProvider: ['type', DurgaPropertiesProvider]
};

registerBpmnJSPlugin(module.exports);
