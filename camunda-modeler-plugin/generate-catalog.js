#!/usr/bin/env node
'use strict';

/**
 * Generates a plugin catalog JSON with widget definitions for Camunda Modeler.
 *
 * Reads all plugins/** / *.yml (except catalog.yml), extracts metadata from
 * both the YAML descriptors and the hardcoded widget schemas below, and writes
 * plugin-catalog.json with per-plugin form field definitions.
 */
const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');

const PLUGINS_DIR = path.resolve(
  process.env.DURGA_PLUGIN_CATALOG_DIR || path.join(__dirname, '..', 'plugins')
);
const OUTPUT_DIR = path.resolve(__dirname, 'dist');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'plugin-catalog.json');

const CATEGORY_LABELS = {
  transform:  'Transform',
  validate:   'Validate',
  enrich:     'Enrich',
  route:      'Route',
  aggregate:  'Aggregate',
  inspect:    'Inspect',
  store:      'Store',
  connect:    'Connect (code generator)'
};

// ---- Per-plugin widget schemas ----
// Each entry defines the form fields shown in the Modeler for that plugin's config.
// Field types: text, select, number, boolean, textarea, mapping, expression
// The 'key' is the config key as stored in the semicolon-delimited config string.

const WIDGET_SCHEMAS = {

  'json-transform': {
    delimiter: null,  // expression-based (comma-separated field mappings)
    fields: [
      { key: 'expression', label: 'Mapping Expression', type: 'textarea',
        placeholder: 'field1, field2:dest2, name:literalValue',
        help: 'Comma-separated field mappings. Fields not mentioned are dropped. Use "." to pass through unchanged.' }
    ]
  },

  'field-filter': {
    delimiter: 'space',
    fields: [
      { key: 'keep', label: 'Keep (whitelist)', type: 'text',
        placeholder: 'name,email,total',
        help: 'Comma-separated fields to retain. Dot-notation supported.' },
      { key: 'drop', label: 'Drop (blacklist)', type: 'text',
        placeholder: 'password,ssn',
        help: 'Comma-separated fields to remove. "keep" wins on conflict.' },
      { key: 'flatten', label: 'Flatten prefix', type: 'text',
        placeholder: 'nested',
        help: 'If set, hoist nested fields under this prefix to top level.' }
    ]
  },

  'type-coercer': {
    delimiter: null,  // expression-based (comma-separated field:type tokens)
    fields: [
      { key: 'expression', label: 'Coercion Expression', type: 'textarea',
        placeholder: 'amount:double, order_id:int, email:string',
        help: 'Comma-separated field:type pairs. Types: string, int, long, double, decimal, boolean.' }
    ]
  },

  'string-template': {
    delimiter: 'template',
    fields: [
      { key: 'template', label: 'Template', type: 'textarea',
        placeholder: 'Order ${order_id} by ${customer.name}',
        help: 'Template with ${field} placeholders. Dot-notation for nested fields. Missing fields become empty string.' }
    ]
  },

  'mask': {
    delimiter: 'semicolon',
    fields: [
      { key: 'fields', label: 'Fields', type: 'text', required: true,
        placeholder: 'email,phone,ssn',
        help: 'Comma-separated field names to mask. Dot-notation supported.' },
      { key: 'mask', label: 'Mask character', type: 'select', defaultValue: '*',
        options: [
          { value: '*', label: '* (asterisk)' },
          { value: '#', label: '# (hash)' },
          { value: 'X', label: 'X' },
          { value: '•', label: '• (bullet)' }
        ] },
      { key: 'preserve', label: 'Preserve boundary chars', type: 'number', defaultValue: '0',
        help: 'Number of characters to keep visible at start and end.' }
    ]
  },

  'regex-extract': {
    delimiter: 'semicolon',
    fields: [
      { key: 'source', label: 'Source field', type: 'text', required: true,
        placeholder: 'message',
        help: 'Dot-notation path to the source field containing text to match.' },
      { key: 'pattern', label: 'Regex pattern', type: 'text', required: true,
        placeholder: '(?<method>GET|POST) (?<path>/\\S+)',
        help: 'Java regex with named capture groups (?<name>...).' },
      { key: 'target', label: 'Target field', type: 'text',
        placeholder: 'parsed',
        help: 'Dot-notation path for extracted groups. Defaults to top-level.' },
      { key: 'all', label: 'Extract all matches', type: 'boolean', defaultValue: 'false' }
    ]
  },

  'json-flatten': {
    delimiter: 'semicolon',
    fields: [
      { key: 'direction', label: 'Direction', type: 'select', defaultValue: 'flatten',
        options: [
          { value: 'flatten', label: 'Flatten (nested → dot-notation)' },
          { value: 'unflatten', label: 'Unflatten (dot-notation → nested)' }
        ] },
      { key: 'separator', label: 'Key separator', type: 'text', defaultValue: '.',
        help: 'Character(s) used to join/flatten key segments.' },
      { key: 'maxDepth', label: 'Max depth', type: 'number',
        help: 'Maximum nesting depth to flatten. Blank = unlimited.' }
    ]
  },

  'uuid-inject': {
    delimiter: 'semicolon',
    fields: [
      { key: 'fields', label: 'Target fields', type: 'text', defaultValue: 'id',
        placeholder: 'trace_id,correlation_id',
        help: 'Comma-separated field names. Defaults to "id".' },
      { key: 'strategy', label: 'UUID strategy', type: 'select', defaultValue: 'uuid7',
        options: [
          { value: 'uuid7', label: 'UUID v7 (time-ordered, recommended)' },
          { value: 'uuid4', label: 'UUID v4 (random)' },
          { value: 'uuid1', label: 'UUID v1 (time-based)' }
        ] }
    ]
  },

  'timestamp-normalize': {
    delimiter: 'semicolon',
    fields: [
      { key: 'fields', label: 'Fields', type: 'text', required: true,
        placeholder: 'timestamp,created_at',
        help: 'Comma-separated field names to normalize. Dot-notation supported.' },
      { key: 'from', label: 'Source format', type: 'select', defaultValue: 'epoch_ms',
        options: [
          { value: 'epoch_ms', label: 'Epoch milliseconds' },
          { value: 'epoch_s', label: 'Epoch seconds' },
          { value: 'ISO8601', label: 'ISO 8601' },
          { value: 'RFC3339', label: 'RFC 3339' },
          { value: 'custom', label: 'Custom DateTimeFormatter pattern...' }
        ] },
      { key: 'to', label: 'Target format', type: 'select', defaultValue: 'ISO8601',
        options: [
          { value: 'ISO8601', label: 'ISO 8601' },
          { value: 'RFC3339', label: 'RFC 3339' },
          { value: 'epoch_ms', label: 'Epoch milliseconds' },
          { value: 'epoch_s', label: 'Epoch seconds' }
        ] },
      { key: 'zone', label: 'Timezone', type: 'text', defaultValue: 'UTC',
        placeholder: 'UTC',
        help: 'Java ZoneId, e.g. UTC, Europe/Stockholm, America/New_York.' },
      { key: 'removeOnError', label: 'Remove on parse error', type: 'boolean', defaultValue: 'false' }
    ]
  },

  'format-detector': {
    delimiter: 'semicolon',
    fields: [
      { key: 'field', label: 'Output field', type: 'text', defaultValue: 'format',
        help: 'JSON key for the detection result object.' },
      { key: 'includePayload', label: 'Include payload', type: 'boolean', defaultValue: 'false',
        help: 'Include the original payload under "payload" key.' }
    ]
  },

  'object-store-collector': {
    delimiter: 'semicolon',
    fields: [
      { key: 'asset', label: 'Asset name', type: 'text', defaultValue: 'payload',
        help: 'Name in the emitted DataHandle.' },
      { key: 'handleField', label: 'Handle field', type: 'text', defaultValue: 'dataHandle',
        help: 'JSON key for the data handle in output.' },
      { key: 'store', label: 'Store root', type: 'text',
        placeholder: 'file:///data/objects',
        help: 'Root directory for file storage (file: URI or local path).' },
      { key: 'prefix', label: 'Prefix', type: 'text', defaultValue: 'objects',
        help: 'Subdirectory prefix under root.' },
      { key: 'layout', label: 'Naming layout', type: 'text',
        placeholder: 'const:tenantA/date/field:region',
        help: 'Directory scheme under the prefix; the filename is always a UUID. '
          + 'Slash-separated tokens, freely combined: '
          + 'date | date:hour | date:minute (yyyy/MM/dd[/HH][/mm], UTC); '
          + 'field:<path> (sanitized payload value — content/business-concept naming; _unknown if absent); '
          + 'const:<text> or a bare literal (fixed segment). Empty = flat structure.' },
      { key: 'includeFormat', label: 'Include format', type: 'boolean', defaultValue: 'true' },
      { key: 'includeOriginal', label: 'Include original', type: 'boolean', defaultValue: 'false' }
    ]
  },

  'object-store-extractor': {
    delimiter: 'semicolon',
    fields: [
      { key: 'handleField', label: 'Handle field', type: 'text', defaultValue: 'dataHandle',
        help: 'Dot-notation path to the DataHandle object in the input JSON.' }
    ]
  },

  'json-schema-validator': {
    delimiter: 'semicolon',
    fields: [
      { key: 'required', label: 'Required fields (compact)', type: 'text',
        placeholder: 'order_id,amount,customer_email',
        help: 'Comma-separated required field names. For full schema validation, use a JSON object.' }
    ]
    // Note: Full JSON schema validation is done via a JSON config string (starts with {).
    // The compact mode only supports "required".
  },

  'kv-enricher': {
    delimiter: 'semicolon',
    fields: [
      { key: 'keyField', label: 'Key field', type: 'text', defaultValue: '_id',
        placeholder: 'customer_email',
        help: 'Dot-notation path to the lookup key.' },
      { key: 'inline', label: 'Inline data', type: 'textarea',
        placeholder: '{alice@example.com:{"tier":"gold"}, bob@example.com:{"tier":"silver"}}',
        help: 'Map of key→JSON objects. Format: {key1:value1, key2:value2}.' }
    ]
  },

  'field-router': {
    delimiter: 'space',
    fields: [
      { key: 'field', label: 'Routing field', type: 'text', defaultValue: 'status',
        placeholder: 'status',
        help: 'Dot-notation path to the field used for routing decisions.' },
      { key: 'routes', label: 'Route map', type: 'textarea',
        placeholder: '{approved:high_value, rejected:low_value}',
        help: 'Map of field values to channel names. Format: {value1:channel1, value2:channel2}.' },
      { key: 'default', label: 'Default route', type: 'text', defaultValue: 'default',
        placeholder: 'default',
        help: 'Fallback channel when no route matches.' }
    ]
  },

  'window-counter': {
    delimiter: 'space',
    fields: [
      { key: 'window', label: 'Window (seconds)', type: 'number', defaultValue: '60',
        help: 'Tumbling window duration in seconds.' },
      { key: 'groupBy', label: 'Group by field', type: 'text',
        placeholder: 'status',
        help: 'Dot-notation path to an optional grouping field.' }
    ]
  }
};

// Connect plugins have no runtime config — they're code generators
WIDGET_SCHEMAS['kafka-connect-source'] = { delimiter: 'semicolon', fields: [
  { key: 'connectorClass', label: 'Connector class', type: 'text', required: true,
    placeholder: 'io.confluent.connect.jdbc.JdbcSourceConnector' },
  { key: 'tasksMax', label: 'Max tasks', type: 'number', defaultValue: '1' }
]};

WIDGET_SCHEMAS['kafka-connect-sink'] = { delimiter: 'semicolon', fields: [
  { key: 'connectorClass', label: 'Connector class', type: 'text', required: true,
    placeholder: 'io.confluent.connect.s3.S3SinkConnector' },
  { key: 'tasksMax', label: 'Max tasks', type: 'number', defaultValue: '1' }
]};

// ---- Catalog generation ----

function readYamlFiles(dir) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...readYamlFiles(full));
    } else if (entry.isFile() && entry.name.endsWith('.yml') && entry.name !== 'catalog.yml') {
      files.push(full);
    }
  }
  return files;
}

function parseDescriptor(yamlPath) {
  const content = fs.readFileSync(yamlPath, 'utf8');
  const doc = yaml.load(content);
  if (!doc || !doc.id) return null;

  const widgetSchema = WIDGET_SCHEMAS[doc.id] || { delimiter: 'semicolon', fields: [] };

  return {
    id: doc.id,
    name: doc.name || doc.id,
    category: doc.category || 'unknown',
    version: doc.version || '0.0.0',
    status: doc.status || 'unknown',
    description: (doc.description || '').replace(/\s+/g, ' ').trim(),
    categoryLabel: CATEGORY_LABELS[doc.category] || doc.category,
    isGenerator: doc.category === 'connect',
    delimiter: widgetSchema.delimiter,
    widgets: widgetSchema.fields
  };
}

function generate() {
  if (!fs.existsSync(PLUGINS_DIR)) {
    throw new Error(
      `Plugin descriptor directory not found: ${PLUGINS_DIR}. ` +
      'Set DURGA_PLUGIN_CATALOG_DIR to the directory containing catalog.yml and plugin descriptors.'
    );
  }

  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const yamlFiles = readYamlFiles(PLUGINS_DIR);
  const catalog = [];

  for (const yamlFile of yamlFiles.sort()) {
    const entry = parseDescriptor(yamlFile);
    if (entry && entry.id) {
      catalog.push(entry);
    }
  }

  const categoryOrder = Object.keys(CATEGORY_LABELS);
  catalog.sort((a, b) => {
    const ai = categoryOrder.indexOf(a.category);
    const bi = categoryOrder.indexOf(b.category);
    if (ai !== bi) return (ai >= 0 ? ai : 99) - (bi >= 0 ? bi : 99);
    return a.name.localeCompare(b.name);
  });

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(catalog, null, 2), 'utf8');
  console.log(`Read plugin descriptors from: ${PLUGINS_DIR}`);
  console.log(`Generated plugin catalog: ${catalog.length} plugins → ${OUTPUT_FILE}`);
}

generate();
