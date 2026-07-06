# Durga Camunda Modeler Plugin

A plugin for [Camunda Modeler](https://camunda.com/download/modeler/) that adds
Durga-specific configuration to BPMN elements.

## Features

- **Plugin picker** — dropdown of all 18 Durga plugins, grouped by category, with status indicators
- **Per-plugin config forms** — structured fields tailored to each plugin (mask char, UUID strategy, time windows, etc.)
- **Alarm configuration** — fault detection rules at activity, process-inherited, and process-aggregate levels
- **Auto-fill** — selecting a plugin pre-fills its config with default values and required fields

## Install

### Prerequisites

- Node.js 18+ and npm
- Camunda Modeler 5.x ([download](https://camunda.com/download/modeler/))

### Build

```bash
cd camunda-modeler-plugin
npm install
npm run build
```

This generates `dist/client.js` and bundles the plugin catalog from the repository-level
`../plugins/*.yml` descriptors. To use a different descriptor directory, set
`DURGA_PLUGIN_CATALOG_DIR=/path/to/plugins` before running the build.

### Install into Camunda Modeler

1. Locate your Camunda Modeler plugins directory:

   | Platform | Path |
   |----------|------|
   | macOS    | `~/Library/Application Support/camunda-modeler/resources/plugins` |
   | Linux    | `~/.config/camunda-modeler/resources/plugins` |
   | Windows  | `%APPDATA%\camunda-modeler\resources\plugins` |

2. Copy the plugin directory:

   ```bash
   # macOS example
   PLUGINS_DIR="$HOME/Library/Application Support/camunda-modeler/resources/plugins"
   mkdir -p "$PLUGINS_DIR/durga-plugin"
   cp index.js "$PLUGINS_DIR/durga-plugin/"
   cp -r dist "$PLUGINS_DIR/durga-plugin/"
   ```

   The final structure should be:
   ```
   camunda-modeler/resources/plugins/durga-plugin/
   ├── index.js
   └── dist/
       ├── client.js
       └── plugin-catalog.json
   ```

3. Restart Camunda Modeler.

4. Verify: create or open a **Camunda 7 (Platform)** BPMN diagram and select any task
   element — the properties panel should show **"Durga Plugin"**, **"Durga Alarm
   (Activity)"**, and **"Durga SLA (Activity)"** groups.

> **Camunda 7 vs Camunda 8:** Durga stores its settings as `camunda:*` extension
> properties (the Camunda 7 / Platform moddle), so the plugin registers only with the
> Platform BPMN editor. The Durga groups will **not** appear on a Camunda 8 (Cloud/Zeebe)
> diagram — start a "Camunda 7" BPMN diagram instead.

> **A greyed-out "Durga Pipeline Configuration" entry under the Plugins menu is normal.**
> It is only an indicator that the plugin loaded; this plugin contributes properties-panel
> groups, not a clickable menu action. If the entry is present but no Durga groups show when
> a task is selected, confirm you are on a Camunda 7 diagram and that `dist/client.js` is the
> current build.

### Rebuild after adding plugins

When new plugin descriptors are added to `../plugins/`, rebuild:

```bash
npm run build
```

Then copy the updated `dist/` directory to the Modeler plugins folder and restart.

## Usage

### Assigning a Durga plugin to a task

1. Select a BPMN task element (Service Task, User Task, etc.)
2. In the properties panel, expand **"Durga Plugin"**
3. Choose a plugin from the dropdown
4. Configure its parameters in the **"Plugin Config"** section — each plugin shows its own fields

### Configuring fault detection alarms

1. On a task: expand **"Durga Alarm (Activity)"** to set per-activity alarms
2. On the process (click empty canvas area): expand **"Durga Process Alarms"** to set inherited defaults and aggregate alarms

Available syndromes:

| Syndrome | Fires when | Uses |
|----------|-----------|------|
| `HARD_ERROR` | first matching event | `eventType` |
| `COUNTED` | matching-event count exceeds threshold | `eventType`, `threshold` |
| `SLIDING_WINDOW` | matching events in a window exceed threshold | `eventType`, `threshold`, `windowSeconds` |
| `STUCK` | an active instance is idle longer than the timeout | `windowSeconds` (idle timeout) |
| `CASCADE` | more instances stall within a window than the threshold | `threshold`, `windowSeconds` |
| `SLA_LATENCY` | a task/process takes longer than the agreed maximum | `maxLatencyMs` |
| `SLA_THROUGHPUT` | completions per window fall below the agreed minimum | `threshold` (min), `windowSeconds` |

`STUCK` and `CASCADE` are absence-of-progress syndromes evaluated by the monitor's stall
detector, so they do **not** require an `eventType`. The monitor also ships built-in `STUCK`
and `CASCADE` defaults that apply with no configuration; a process-level config of the same
syndrome overrides the built-in default for that scope.

### Configuring performance SLAs

1. On a task: expand **"Durga SLA (Activity)"** — choose `SLA_LATENCY` (set *Max latency (ms)*)
   or `SLA_THROUGHPUT` (set *Min calls / window* and *Window (s)*).
2. On the process (click empty canvas area): expand **"Durga Process SLA (end-to-end)"** for a
   whole-process latency ceiling or completion-rate floor.

### Fields written

All configuration is stored as Camunda extension properties:

```xml
<camunda:property name="plugin" value="json-transform" />
<camunda:property name="pluginConfig" value="expression=id:order_id, customer.name" />
<camunda:property name="durga:alarm:validate-escalation:syndrome" value="HARD_ERROR" />
```

These are parsed by Durga's `BpmnAlarmConfigParser` at runtime.

## Project structure

```
camunda-modeler-plugin/
├── index.js                        # Plugin entry (exports name + script)
├── client/
│   ├── index.js                    # Client-side registration
│   └── DurgaPropertiesProvider.js  # Properties panel groups + widget logic
├── generate-catalog.js             # Build-time catalog generator (reads ../plugins/*.yml or DURGA_PLUGIN_CATALOG_DIR)
├── dist/                           # Build output
│   ├── client.js                   # Bundled plugin (webpack)
│   └── plugin-catalog.json         # Generated catalog with widget schemas
├── package.json
└── webpack.config.js
```

`generate-catalog.js` does not modify `plugins/catalog.yml`; it reads descriptor YAML and writes
`dist/plugin-catalog.json` for the Camunda Modeler UI.
