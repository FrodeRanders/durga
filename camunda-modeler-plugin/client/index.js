'use strict';

// Durga authors camunda:* extension properties (Camunda 7 / Platform moddle), so the
// provider is registered with the Platform BPMN editor. Create/open a "Camunda 7" BPMN
// diagram in the Modeler for the Durga groups to appear.
var registerPlatformBpmnJSPlugin = require('camunda-modeler-plugin-helpers').registerPlatformBpmnJSPlugin;
var DurgaPropertiesProvider = require('./DurgaPropertiesProvider');

module.exports = {
  __init__: ['durgaPropertiesProvider'],
  durgaPropertiesProvider: ['type', DurgaPropertiesProvider]
};

registerPlatformBpmnJSPlugin(module.exports);
