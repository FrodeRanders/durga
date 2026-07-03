'use strict';

var is = require('bpmn-js/lib/util/ModelUtil').is;
var getBusinessObject = require('bpmn-js/lib/util/ModelUtil').getBusinessObject;

// Load the generated plugin catalog (bundled at build time)
var PLUGIN_CATALOG = [];
var CATALOG_BY_ID = {};
try {
  PLUGIN_CATALOG = require('../dist/plugin-catalog.json');
  for (var i = 0; i < PLUGIN_CATALOG.length; i++) {
    CATALOG_BY_ID[PLUGIN_CATALOG[i].id] = PLUGIN_CATALOG[i];
  }
} catch (e) { /* catalog not available at dev time */ }

var SYNDROME_OPTIONS = [
  { value: '', label: '<none>' },
  { value: 'HARD_ERROR', label: 'Hard Error (immediate)' },
  { value: 'COUNTED', label: 'Counted (threshold)' },
  { value: 'SLIDING_WINDOW', label: 'Sliding Window (time-bound)' }
];

var EVENT_TYPE_OPTIONS = [
  { value: 'PROCESS_FAILED', label: 'PROCESS_FAILED' },
  { value: 'ACTIVITY_ESCALATED', label: 'ACTIVITY_ESCALATED' },
  { value: 'ACTIVITY_CANCELLED', label: 'ACTIVITY_CANCELLED' },
  { value: 'ACTIVITY_COMPLETED', label: 'ACTIVITY_COMPLETED' }
];

var SEVERITY_OPTIONS = [
  { value: 'WARN', label: 'WARN' },
  { value: 'CRITICAL', label: 'CRITICAL' }
];

function DurgaPropertiesProvider(propertiesPanel, translate) {
  propertiesPanel.registerProvider(500, this);
}

module.exports = DurgaPropertiesProvider;

DurgaPropertiesProvider.$inject = ['propertiesPanel', 'translate'];

DurgaPropertiesProvider.prototype.getGroups = function(element) {
  return function(groups) {
    if (isActivity(element)) {
      groups.push(createPluginSelectGroup(element));
      groups.push(createPluginConfigGroup(element));
      groups.push(createAlarmGroup(element));
    }
    if (is(element, 'bpmn:Process')) {
      groups.push(createProcessAlarmGroup(element));
    }
    return groups;
  };
};

function isActivity(element) {
  return is(element, 'bpmn:ServiceTask') || is(element, 'bpmn:Task') ||
         is(element, 'bpmn:UserTask') || is(element, 'bpmn:ScriptTask') ||
         is(element, 'bpmn:BusinessRuleTask') || is(element, 'bpmn:SendTask') ||
         is(element, 'bpmn:ReceiveTask') || is(element, 'bpmn:ManualTask');
}

// ---- Plugin Select Group ----

function createPluginSelectGroup(element) {
  var prop = getCamundaProperty(element, 'plugin');
  var selectedValue = prop ? prop.value : '';

  var options = [{ value: '', label: '<select plugin>' }];
  var currentCategory = null;

  for (var i = 0; i < PLUGIN_CATALOG.length; i++) {
    var p = PLUGIN_CATALOG[i];
    if (p.category !== currentCategory) {
      currentCategory = p.category;
      options.push({
        value: '',
        label: '── ' + (p.categoryLabel || p.category) + ' ──',
        disabled: true
      });
    }
    var label = p.name + ' (' + p.id + ')';
    if (p.status === 'experimental') label += ' \u26A0';
    if (p.isGenerator) label += ' [generator]';
    options.push({ value: p.id, label: label });
  }

  return createGroup('durga-plugin-select', 'Durga Plugin', [
    {
      id: 'durga-plugin-id',
      label: 'Plugin',
      component: 'select',
      getOptions: function() { return options; },
      getValue: function() { return selectedValue; },
      setValue: function(value) {
        if (value) {
          setCamundaProperty(element, 'plugin', value);
          selectedValue = value;
        } else {
          removeCamundaProperty(element, 'plugin');
          selectedValue = '';
        }
      }
    }
  ], getCamundaProperty(element, 'plugin') != null);
}

// ---- Plugin Config Group (widget-driven) ----

function createPluginConfigGroup(element) {
  var pluginProp = getCamundaProperty(element, 'plugin');
  var pluginId = pluginProp ? pluginProp.value : '';
  var catalogEntry = CATALOG_BY_ID[pluginId];

  if (!catalogEntry || !catalogEntry.widgets || catalogEntry.widgets.length === 0) {
    // No widget schema for this plugin — show plain text field
    var configProp = getCamundaProperty(element, 'pluginConfig');
    return createGroup('durga-plugin-config', 'Plugin Config', [
      {
        id: 'durga-config-raw',
        label: 'Config',
        getValue: function() { return configProp ? configProp.value : ''; },
        setValue: function(value) { setCamundaProperty(element, 'pluginConfig', value); }
      }
    ], configProp != null);
  }

  // Build widget-driven fields
  var schema = catalogEntry;
  var entries = [];

  // Helper to read pluginConfig as a parsed map
  function parseConfig() {
    var raw = (getCamundaProperty(element, 'pluginConfig') || {}).value || '';
    return parseConfigString(raw, schema.delimiter);
  }

  // Helper to write pluginConfig from field map
  function writeConfig(fieldMap) {
    var current = parseConfig();
    for (var key in fieldMap) {
      if (fieldMap.hasOwnProperty(key)) {
        current[key] = fieldMap[key];
      }
    }
    var serialized = serializeConfigString(current, schema.delimiter);
    if (serialized) {
      setCamundaProperty(element, 'pluginConfig', serialized);
    } else {
      removeCamundaProperty(element, 'pluginConfig');
    }
  }

  // Render description
  if (schema.description) {
    entries.push({
      id: 'durga-config-desc',
      label: 'Description',
      component: 'separator',
      description: schema.description
    });
  }

  // Render each widget field
  for (var w = 0; w < schema.widgets.length; w++) {
    var widget = schema.widgets[w];
    entries.push(createWidgetEntry(element, widget, parseConfig, writeConfig));
  }

  return createGroup('durga-plugin-config', 'Plugin Config — ' + schema.name, entries,
    getCamundaProperty(element, 'pluginConfig') != null);
}

function createWidgetEntry(element, widget, parseConfigFn, writeConfigFn) {
  var baseId = 'durga-widget-' + widget.key;
  var configValue = (parseConfigFn() || {})[widget.key] || widget.defaultValue || '';

  if (widget.type === 'textarea' || widget.type === 'expression') {
    return {
      id: baseId, label: widget.label, component: 'textarea',
      description: widget.help || '',
      getValue: function() { return configValue; },
      setValue: function(value) {
        var update = {}; update[widget.key] = value;
        writeConfigFn(update);
        configValue = value;
      },
      layout: { row: widget.type === 'textarea' ? 3 : 1 },
      validate: widget.required ? function(v) { return v ? null : 'Required'; } : undefined
    };
  }

  if (widget.type === 'select') {
    return {
      id: baseId, label: widget.label, component: 'select',
      description: widget.help || '',
      getOptions: function() { return widget.options || []; },
      getValue: function() { return configValue; },
      setValue: function(value) {
        var update = {}; update[widget.key] = value;
        writeConfigFn(update);
        configValue = value;
      }
    };
  }

  if (widget.type === 'boolean') {
    return {
      id: baseId, label: widget.label, component: 'checkbox',
      description: widget.help || '',
      getValue: function() { return configValue === 'true' || configValue === true; },
      setValue: function(value) {
        var update = {}; update[widget.key] = value ? 'true' : 'false';
        writeConfigFn(update);
        configValue = value ? 'true' : 'false';
      }
    };
  }

  if (widget.type === 'number') {
    return {
      id: baseId, label: widget.label, component: 'number',
      description: widget.help || '',
      getValue: function() { return configValue; },
      setValue: function(value) {
        var update = {}; update[widget.key] = value;
        writeConfigFn(update);
        configValue = value;
      }
    };
  }

  // default: text
  return {
    id: baseId, label: widget.label,
    description: widget.help || '',
    getValue: function() { return configValue; },
    setValue: function(value) {
      var update = {}; update[widget.key] = value;
      writeConfigFn(update);
      configValue = value;
    },
    validate: widget.required ? function(v) { return v ? null : 'Required'; } : undefined
  };
}

// ---- Config string parsing / serialization ----

function parseConfigString(raw, delimiter) {
  var map = {};
  if (!raw) return map;

  if (delimiter === 'template') {
    // config is "template=<value>", everything after first = is the template
    var eq = raw.indexOf('=');
    if (eq > 0 && raw.substring(0, eq).trim() === 'template') {
      map['template'] = raw.substring(eq + 1);
    } else {
      map['template'] = raw;
    }
    return map;
  }

  if (!delimiter) {
    // expression-based: the whole string is the expression
    map['expression'] = raw;
    return map;
  }

  var splitter = delimiter === 'space' ? /\s+/ : ';';
  var parts = raw.split(splitter);
  for (var i = 0; i < parts.length; i++) {
    var part = parts[i].trim();
    if (!part) continue;
    var eqIdx = part.indexOf('=');
    if (eqIdx > 0) {
      map[part.substring(0, eqIdx).trim()] = part.substring(eqIdx + 1).trim();
    } else {
      // key without value (boolean flag)
      map[part] = 'true';
    }
  }
  return map;
}

function serializeConfigString(map, delimiter) {
  if (!map || Object.keys(map).length === 0) return '';

  if (delimiter === 'template') {
    return 'template=' + (map['template'] || '');
  }

  if (!delimiter) {
    return map['expression'] || '';
  }

  var sep = delimiter === 'space' ? ' ' : '; ';
  var parts = [];
  for (var key in map) {
    if (map.hasOwnProperty(key) && map[key] !== undefined && map[key] !== null && map[key] !== '') {
      parts.push(key + '=' + map[key]);
    }
  }
  return parts.join(sep);
}

// ---- Alarm Groups ----

function createAlarmGroup(element) {
  return createGroup('durga-alarm', 'Durga Alarm (Activity)', [
    createAlarmField(element, 'validate-escalation:syndrome', 'Syndrome', SYNDROME_OPTIONS, true),
    createAlarmField(element, 'validate-escalation:eventType', 'Event Type', EVENT_TYPE_OPTIONS, true),
    createAlarmField(element, 'validate-escalation:threshold', 'Threshold (count)', null, false),
    createAlarmField(element, 'validate-escalation:windowSeconds', 'Window (seconds)', null, false),
    createAlarmField(element, 'validate-escalation:severity', 'Severity', SEVERITY_OPTIONS, true),
    createAlarmField(element, 'validate-escalation:message', 'Message template', null, false)
  ], hasAlarmProps(element));
}

function createProcessAlarmGroup(element) {
  return createGroup('durga-process-alarm', 'Durga Process Alarms', [
    createHeading('Inherited (applies to every activity):'),
    createAlarmField(element, '*default:syndrome', 'Inherited Syndrome', SYNDROME_OPTIONS, false),
    createAlarmField(element, '*default:eventType', 'Inherited Event Type', EVENT_TYPE_OPTIONS, false),
    createAlarmField(element, '*default:threshold', 'Inherited Threshold', null, false),
    createAlarmField(element, '*default:severity', 'Inherited Severity', SEVERITY_OPTIONS, false),
    createAlarmField(element, '*default:message', 'Inherited Message', null, false),
    createHeading('Aggregate (counts across all activities in process):'),
    createAlarmField(element, '$burst:syndrome', 'Aggregate Syndrome', SYNDROME_OPTIONS, false),
    createAlarmField(element, '$burst:eventType', 'Aggregate Event Type', EVENT_TYPE_OPTIONS, false),
    createAlarmField(element, '$burst:threshold', 'Aggregate Threshold', null, false),
    createAlarmField(element, '$burst:windowSeconds', 'Aggr. Window (sec)', null, false),
    createAlarmField(element, '$burst:severity', 'Aggregate Severity', SEVERITY_OPTIONS, false),
    createAlarmField(element, '$burst:message', 'Aggregate Message', null, false)
  ], hasAlarmProps(element));
}

function createAlarmField(element, fieldSuffix, label, options) {
  var propName = 'durga:alarm:' + fieldSuffix;
  var prop = getCamundaProperty(element, propName);

  if (options) {
    return {
      id: 'durga-' + fieldSuffix.replace(/[:*$]/g, '-'),
      label: label,
      component: 'select',
      getOptions: function() { return options; },
      getValue: function() { return prop ? prop.value : ''; },
      setValue: function(value) {
        if (value) { setCamundaProperty(element, propName, value); }
        else { removeCamundaProperty(element, propName); }
      }
    };
  }

  return {
    id: 'durga-' + fieldSuffix.replace(/[:*$]/g, '-'),
    label: label,
    getValue: function() { return prop ? prop.value : ''; },
    setValue: function(value) {
      if (value) { setCamundaProperty(element, propName, value); }
      else { removeCamundaProperty(element, propName); }
    }
  };
}

function createHeading(text) {
  return { id: 'heading-' + text.replace(/\s+/g, '-'), label: text, component: 'separator' };
}

function hasAlarmProps(element) {
  if (element.get('extensionElements')) {
    var vals = element.get('extensionElements').get('values') || [];
    for (var i = 0; i < vals.length; i++) {
      if (vals[i].$type === 'camunda:Properties') {
        return true;
      }
    }
  }
  return false;
}

// ---- Group builder ----

function createGroup(id, label, entries, isEdited) {
  return { id: id, label: label, entries: entries };
}

// ---- Camunda extension property helpers ----

function getCamundaPropertiesElement(element) {
  var bo = getBusinessObject(element);
  var extensionElements = bo.get('extensionElements');
  if (!extensionElements) return null;
  var values = extensionElements.get('values');
  if (!values) return null;
  for (var i = 0; i < values.length; i++) {
    if (values[i].$type === 'camunda:Properties') return values[i];
  }
  return null;
}

function getCamundaProperty(element, name) {
  var propsElem = getCamundaPropertiesElement(element);
  if (!propsElem) return null;
  var values = propsElem.get('values');
  if (!values) return null;
  for (var i = 0; i < values.length; i++) {
    if (values[i].$type === 'camunda:Property' && values[i].get('name') === name) {
      return values[i];
    }
  }
  return null;
}

function setCamundaProperty(element, name, value) {
  var bo = getBusinessObject(element);
  var bpmnFactory = bo.$model && bo.$model._model && bo.$model._model.get('factory');
  if (!bpmnFactory) { bpmnFactory = element._model && element._model._factory; }
  var commandStack = element._commandStack;

  var existing = getCamundaProperty(element, name);
  if (existing) {
    if (commandStack) {
      commandStack.execute('element.updateProperties', { element: existing, properties: { value: value } });
    } else {
      existing.set('value', value);
    }
    return;
  }

  var extensionElements = bo.get('extensionElements');
  if (!extensionElements) {
    extensionElements = bpmnFactory.create('bpmn:ExtensionElements');
    if (commandStack) {
      commandStack.execute('element.updateModdleProperties', { element: element, moddleElement: bo, properties: { extensionElements: extensionElements } });
    } else {
      bo.set('extensionElements', extensionElements);
    }
    extensionElements = bo.get('extensionElements');
  }

  extensionElements = bo.get('extensionElements');
  var values = extensionElements.get('values') || [];
  var propsElem = null;
  for (var i = 0; i < values.length; i++) {
    if (values[i].$type === 'camunda:Properties') { propsElem = values[i]; break; }
  }

  if (!propsElem) {
    propsElem = bpmnFactory.create('camunda:Properties');
    var newVals = values.slice();
    newVals.push(propsElem);
    if (commandStack) {
      commandStack.execute('element.updateModdleProperties', { element: element, moddleElement: extensionElements, properties: { values: newVals } });
    } else {
      extensionElements.set('values', newVals);
    }
  }

  var newProp = bpmnFactory.create('camunda:Property', { name: name, value: value });
  var propValues = (propsElem.get('values') || []).slice();
  propValues.push(newProp);
  if (commandStack) {
    commandStack.execute('element.updateModdleProperties', { element: element, moddleElement: propsElem, properties: { values: propValues } });
  } else {
    propsElem.set('values', propValues);
  }
}

function removeCamundaProperty(element, name) {
  var existing = getCamundaProperty(element, name);
  if (!existing) return;
  var propsElem = getCamundaPropertiesElement(element);
  if (!propsElem) return;
  var values = (propsElem.get('values') || []).slice();
  var filtered = values.filter(function(p) { return p.get('name') !== name; });
  var commandStack = element._commandStack;
  if (commandStack) {
    commandStack.execute('element.updateModdleProperties', { element: element, moddleElement: propsElem, properties: { values: filtered } });
  } else {
    propsElem.set('values', filtered);
  }
}
