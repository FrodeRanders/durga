'use strict';

var is = require('bpmn-js/lib/util/ModelUtil').is;
var getBusinessObject = require('bpmn-js/lib/util/ModelUtil').getBusinessObject;
// Use the Camunda Modeler host's live properties-panel instance (and its preact),
// not a bundled copy — otherwise entries register but never render in the host panel.
var propertiesPanel = require('camunda-modeler-plugin-helpers/vendor/@bpmn-io/properties-panel');

var TextFieldEntry = propertiesPanel.TextFieldEntry;
var TextAreaEntry = propertiesPanel.TextAreaEntry;
var SelectEntry = propertiesPanel.SelectEntry;
var CheckboxEntry = propertiesPanel.CheckboxEntry;
var NumberFieldEntry = propertiesPanel.NumberFieldEntry;
var isTextFieldEntryEdited = propertiesPanel.isTextFieldEntryEdited;
var isTextAreaEntryEdited = propertiesPanel.isTextAreaEntryEdited;
var isSelectEntryEdited = propertiesPanel.isSelectEntryEdited;
var isCheckboxEntryEdited = propertiesPanel.isCheckboxEntryEdited;
var isNumberFieldEntryEdited = propertiesPanel.isNumberFieldEntryEdited;

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
  { value: 'SLIDING_WINDOW', label: 'Sliding Window (time-bound)' },
  { value: 'STUCK', label: 'Stuck (idle timeout)' },
  { value: 'CASCADE', label: 'Cascade (stall surge)' },
  { value: 'SLA_LATENCY', label: 'SLA: max latency (wall-clock)' },
  { value: 'SLA_THROUGHPUT', label: 'SLA: min throughput (calls/window)' }
];

var SLA_SYNDROME_OPTIONS = [
  { value: '', label: '<none>' },
  { value: 'SLA_LATENCY', label: 'Max latency (wall-clock)' },
  { value: 'SLA_THROUGHPUT', label: 'Min throughput (calls/window)' }
];

var EVENT_TYPE_OPTIONS = [
  { value: '', label: '<none / not applicable>' },
  { value: 'PROCESS_FAILED', label: 'PROCESS_FAILED' },
  { value: 'ACTIVITY_ESCALATED', label: 'ACTIVITY_ESCALATED' },
  { value: 'ACTIVITY_CANCELLED', label: 'ACTIVITY_CANCELLED' },
  { value: 'ACTIVITY_COMPLETED', label: 'ACTIVITY_COMPLETED' }
];

var SEVERITY_OPTIONS = [
  { value: 'WARN', label: 'WARN' },
  { value: 'CRITICAL', label: 'CRITICAL' }
];

function DurgaPropertiesProvider(propertiesPanelService, translate, commandStack, bpmnFactory) {
  this._commandStack = commandStack;
  this._bpmnFactory = bpmnFactory;
  propertiesPanelService.registerProvider(500, this);
}

module.exports = DurgaPropertiesProvider;

DurgaPropertiesProvider.$inject = ['propertiesPanel', 'translate', 'commandStack', 'bpmnFactory'];

DurgaPropertiesProvider.prototype.getGroups = function(element) {
  var commandStack = this._commandStack;
  var bpmnFactory = this._bpmnFactory;
  return function(groups) {
    if (isActivity(element)) {
      groups.push(createPluginSelectGroup(element, commandStack, bpmnFactory));
      groups.push(createPluginConfigGroup(element, commandStack, bpmnFactory));
      groups.push(createAlarmGroup(element, commandStack, bpmnFactory));
      groups.push(createSlaGroup(element, commandStack, bpmnFactory));
    }
    if (is(element, 'bpmn:Process')) {
      groups.push(createProcessAlarmGroup(element, commandStack, bpmnFactory));
      groups.push(createProcessSlaGroup(element, commandStack, bpmnFactory));
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

function createPluginSelectGroup(element, commandStack, bpmnFactory) {
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
      component: SelectEntry,
      isEdited: isSelectEntryEdited,
      getOptions: function() { return options; },
      getValue: function() { return selectedValue; },
      setValue: function(value) {
        if (value) {
          setCamundaProperty(element, 'plugin', value, commandStack, bpmnFactory);
          selectedValue = value;
        } else {
          removeCamundaProperty(element, 'plugin', commandStack);
          selectedValue = '';
        }
      }
    }
  ], getCamundaProperty(element, 'plugin') != null);
}

// ---- Plugin Config Group (widget-driven) ----

function createPluginConfigGroup(element, commandStack, bpmnFactory) {
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
        component: TextFieldEntry,
        isEdited: isTextFieldEntryEdited,
        getValue: function() { return configProp ? configProp.value : ''; },
        setValue: function(value) { setCamundaProperty(element, 'pluginConfig', value, commandStack, bpmnFactory); }
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
      setCamundaProperty(element, 'pluginConfig', serialized, commandStack, bpmnFactory);
    } else {
      removeCamundaProperty(element, 'pluginConfig', commandStack);
    }
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
      id: baseId, label: widget.label,
      component: TextAreaEntry,
      isEdited: isTextAreaEntryEdited,
      description: widget.help || '',
      getValue: function() { return configValue; },
      setValue: function(value) {
        var update = {}; update[widget.key] = value;
        writeConfigFn(update);
        configValue = value;
      },
      rows: widget.type === 'textarea' ? 3 : 1,
      validate: widget.required ? function(v) { return v ? null : 'Required'; } : undefined
    };
  }

  if (widget.type === 'select') {
    return {
      id: baseId, label: widget.label,
      component: SelectEntry,
      isEdited: isSelectEntryEdited,
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
      id: baseId, label: widget.label,
      component: CheckboxEntry,
      isEdited: isCheckboxEntryEdited,
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
      id: baseId, label: widget.label,
      component: NumberFieldEntry,
      isEdited: isNumberFieldEntryEdited,
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
    component: TextFieldEntry,
    isEdited: isTextFieldEntryEdited,
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

function createAlarmGroup(element, commandStack, bpmnFactory) {
  return createGroup('durga-alarm', 'Durga Alarm (Activity)', [
    createAlarmField(element, 'validate-escalation:syndrome', 'Syndrome', SYNDROME_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, 'validate-escalation:eventType', 'Event Type', EVENT_TYPE_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, 'validate-escalation:threshold', 'Threshold (count)', null, commandStack, bpmnFactory),
    createAlarmField(element, 'validate-escalation:windowSeconds', 'Window / idle timeout (s)', null, commandStack, bpmnFactory),
    createAlarmField(element, 'validate-escalation:severity', 'Severity', SEVERITY_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, 'validate-escalation:message', 'Message template', null, commandStack, bpmnFactory)
  ], hasAlarmProps(element));
}

function createProcessAlarmGroup(element, commandStack, bpmnFactory) {
  return createGroup('durga-process-alarm', 'Durga Process Alarms', [
    createAlarmField(element, '*default:syndrome', 'Inherited Syndrome', SYNDROME_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, '*default:eventType', 'Inherited Event Type', EVENT_TYPE_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, '*default:threshold', 'Inherited Threshold', null, commandStack, bpmnFactory),
    createAlarmField(element, '*default:windowSeconds', 'Inherited Window / idle timeout (s)', null, commandStack, bpmnFactory),
    createAlarmField(element, '*default:severity', 'Inherited Severity', SEVERITY_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, '*default:message', 'Inherited Message', null, commandStack, bpmnFactory),
    createAlarmField(element, '$burst:syndrome', 'Aggregate Syndrome', SYNDROME_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, '$burst:eventType', 'Aggregate Event Type', EVENT_TYPE_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, '$burst:threshold', 'Aggregate Threshold', null, commandStack, bpmnFactory),
    createAlarmField(element, '$burst:windowSeconds', 'Aggr. window / timeout (s)', null, commandStack, bpmnFactory),
    createAlarmField(element, '$burst:severity', 'Aggregate Severity', SEVERITY_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, '$burst:message', 'Aggregate Message', null, commandStack, bpmnFactory)
  ], hasAlarmProps(element));
}

// ---- SLA Groups ----
//
// SLA alarms use their own alarm ids so they do not collide with an error alarm on the
// same element. Activity scope measures a single task; process aggregate scope ($) measures
// the whole process end to end.
//   SLA_LATENCY    -> maxLatencyMs (maximum allowed wall-clock duration)
//   SLA_THROUGHPUT -> threshold (minimum calls) + windowSeconds (measurement period)

function createSlaGroup(element, commandStack, bpmnFactory) {
  return createGroup('durga-sla', 'Durga SLA (Activity)', [
    createAlarmField(element, 'sla:syndrome', 'SLA Type', SLA_SYNDROME_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, 'sla:maxLatencyMs', 'Max latency (ms, SLA_LATENCY)', null, commandStack, bpmnFactory),
    createAlarmField(element, 'sla:threshold', 'Min calls / window (SLA_THROUGHPUT)', null, commandStack, bpmnFactory),
    createAlarmField(element, 'sla:windowSeconds', 'Window (s, SLA_THROUGHPUT)', null, commandStack, bpmnFactory),
    createAlarmField(element, 'sla:severity', 'Severity', SEVERITY_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, 'sla:message', 'Message template', null, commandStack, bpmnFactory)
  ], hasAlarmProps(element));
}

function createProcessSlaGroup(element, commandStack, bpmnFactory) {
  return createGroup('durga-process-sla', 'Durga Process SLA (end-to-end)', [
    createAlarmField(element, '$sla:syndrome', 'SLA Type', SLA_SYNDROME_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, '$sla:maxLatencyMs', 'Max latency (ms, SLA_LATENCY)', null, commandStack, bpmnFactory),
    createAlarmField(element, '$sla:threshold', 'Min calls / window (SLA_THROUGHPUT)', null, commandStack, bpmnFactory),
    createAlarmField(element, '$sla:windowSeconds', 'Window (s, SLA_THROUGHPUT)', null, commandStack, bpmnFactory),
    createAlarmField(element, '$sla:severity', 'Severity', SEVERITY_OPTIONS, commandStack, bpmnFactory),
    createAlarmField(element, '$sla:message', 'Message template', null, commandStack, bpmnFactory)
  ], hasAlarmProps(element));
}

function createAlarmField(element, fieldSuffix, label, options, commandStack, bpmnFactory) {
  var propName = 'durga:alarm:' + fieldSuffix;
  var prop = getCamundaProperty(element, propName);

  if (options) {
    return {
      id: 'durga-' + fieldSuffix.replace(/[:*$]/g, '-'),
      label: label,
      component: SelectEntry,
      isEdited: isSelectEntryEdited,
      getOptions: function() { return options; },
      getValue: function() { return prop ? prop.value : ''; },
      setValue: function(value) {
        if (value) { setCamundaProperty(element, propName, value, commandStack, bpmnFactory); }
        else { removeCamundaProperty(element, propName, commandStack); }
      }
    };
  }

  return {
    id: 'durga-' + fieldSuffix.replace(/[:*$]/g, '-'),
    label: label,
    component: TextFieldEntry,
    isEdited: isTextFieldEntryEdited,
    getValue: function() { return prop ? prop.value : ''; },
    setValue: function(value) {
      if (value) { setCamundaProperty(element, propName, value, commandStack, bpmnFactory); }
      else { removeCamundaProperty(element, propName, commandStack); }
    }
  };
}

function hasAlarmProps(element) {
  var bo = getBusinessObject(element);
  var extensionElements = bo && bo.get ? bo.get('extensionElements') : null;
  if (extensionElements) {
    var vals = extensionElements.get('values') || [];
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

function setCamundaProperty(element, name, value, commandStack, bpmnFactory) {
  var bo = getBusinessObject(element);

  var existing = getCamundaProperty(element, name);
  if (existing) {
    if (commandStack) {
      commandStack.execute('element.updateModdleProperties', { element: element, moddleElement: existing, properties: { value: value } });
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

function removeCamundaProperty(element, name, commandStack) {
  var existing = getCamundaProperty(element, name);
  if (!existing) return;
  var propsElem = getCamundaPropertiesElement(element);
  if (!propsElem) return;
  var values = (propsElem.get('values') || []).slice();
  var filtered = values.filter(function(p) { return p.get('name') !== name; });
  if (commandStack) {
    commandStack.execute('element.updateModdleProperties', { element: element, moddleElement: propsElem, properties: { values: filtered } });
  } else {
    propsElem.set('values', filtered);
  }
}
