package org.gautelis.durga.tools;

import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.bpm.model.bpmn.instance.BoundaryEvent;
import org.camunda.bpm.model.bpmn.instance.BusinessRuleTask;
import org.camunda.bpm.model.bpmn.instance.CallActivity;
import org.camunda.bpm.model.bpmn.instance.BaseElement;
import org.camunda.bpm.model.bpmn.instance.DataAssociation;
import org.camunda.bpm.model.bpmn.instance.DataInputAssociation;
import org.camunda.bpm.model.bpmn.instance.DataObject;
import org.camunda.bpm.model.bpmn.instance.DataObjectReference;
import org.camunda.bpm.model.bpmn.instance.DataOutputAssociation;
import org.camunda.bpm.model.bpmn.instance.DataStoreReference;
import org.camunda.bpm.model.bpmn.instance.EndEvent;
import org.camunda.bpm.model.bpmn.instance.ErrorEventDefinition;
import org.camunda.bpm.model.bpmn.instance.EscalationEventDefinition;
import org.camunda.bpm.model.bpmn.instance.FlowNode;
import org.camunda.bpm.model.bpmn.instance.IntermediateCatchEvent;
import org.camunda.bpm.model.bpmn.instance.IntermediateThrowEvent;
import org.camunda.bpm.model.bpmn.instance.ItemAwareElement;
import org.camunda.bpm.model.bpmn.instance.ItemDefinition;
import org.camunda.bpm.model.bpmn.instance.ManualTask;
import org.camunda.bpm.model.bpmn.instance.MessageEventDefinition;
import org.camunda.bpm.model.bpmn.instance.MultiInstanceLoopCharacteristics;
import org.camunda.bpm.model.bpmn.instance.Process;
import org.camunda.bpm.model.bpmn.instance.ReceiveTask;
import org.camunda.bpm.model.bpmn.instance.ScriptTask;
import org.camunda.bpm.model.bpmn.instance.SendTask;
import org.camunda.bpm.model.bpmn.instance.SequenceFlow;
import org.camunda.bpm.model.bpmn.instance.ServiceTask;
import org.camunda.bpm.model.bpmn.instance.SignalEventDefinition;
import org.camunda.bpm.model.bpmn.instance.StartEvent;
import org.camunda.bpm.model.bpmn.instance.SubProcess;
import org.camunda.bpm.model.bpmn.instance.Task;
import org.camunda.bpm.model.bpmn.instance.TimeCycle;
import org.camunda.bpm.model.bpmn.instance.TimeDate;
import org.camunda.bpm.model.bpmn.instance.TimeDuration;
import org.camunda.bpm.model.bpmn.instance.TimerEventDefinition;
import org.camunda.bpm.model.bpmn.instance.UserTask;
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaProperties;
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaProperty;
import org.camunda.bpm.model.xml.instance.ModelElementInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Collects BPMN model elements into internal spec descriptors used by the scaffolder.
 */
class BpmnModelCollector {

    private static final Logger LOG = LoggerFactory.getLogger(BpmnModelCollector.class);

    private BpmnModelCollector() {}

    // ---- utility methods ----

    static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "unnamed";
        }
        String normalized = value.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_+", "")
                .replaceAll("_+$", "");
        return normalized.isBlank() ? "unnamed" : normalized;
    }

    static String nameOrId(String name, String id) {
        if (name != null && !name.isBlank()) {
            return name;
        }
        if (id != null && !id.isBlank()) {
            return id;
        }
        return "unnamed";
    }

    static String toClassName(String value) {
        String[] parts = normalize(value).split("_");
        StringBuilder builder = new StringBuilder();
        for (String part : parts) {
            if (part.isBlank()) {
                continue;
            }
            builder.append(part.substring(0, 1).toUpperCase(Locale.ROOT))
                    .append(part.substring(1));
        }
        return builder.isEmpty() ? "Unnamed" : builder.toString();
    }

    // ---- task collection ----

    static List<TaskSpec> collectTaskSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        Map<String, TaskSpec> taskSpecs = new LinkedHashMap<>();

        PluginRegistry pluginRegistry = loadPluginRegistry();

        registerTaskSpecs(model.getModelElementsByType(UserTask.class), TaskKind.USER, taskSpecs, nodes);
        registerTaskSpecs(model.getModelElementsByType(ManualTask.class), TaskKind.MANUAL, taskSpecs, nodes);
        registerTaskSpecs(model.getModelElementsByType(SendTask.class), TaskKind.SEND, taskSpecs, nodes);
        registerTaskSpecs(model.getModelElementsByType(ReceiveTask.class), TaskKind.RECEIVE, taskSpecs, nodes);
        registerTaskSpecs(model.getModelElementsByType(ScriptTask.class), TaskKind.SCRIPT, taskSpecs, nodes);
        registerTaskSpecs(model.getModelElementsByType(BusinessRuleTask.class), TaskKind.BUSINESS_RULE, taskSpecs, nodes);
        for (CallActivity callActivity : model.getModelElementsByType(CallActivity.class)) {
            String name = normalize(nameOrId(callActivity.getName(), callActivity.getId()));
            taskSpecs.put(callActivity.getId(), new TaskSpec(callActivity.getId(), name, TaskKind.CALL));
        }

        for (ServiceTask task : model.getModelElementsByType(ServiceTask.class)) {
            if (taskSpecs.containsKey(task.getId())) {
                continue;
            }
            TaskSpec customSpec = resolveCustomTask(task);
            if (customSpec != null) {
                String name = normalize(nameOrId(task.getName(), task.getId()));
                taskSpecs.put(task.getId(), customSpec);
                nodes.put(task.getId(), new NodeInfo(task.getId(), name, NodeType.TASK, TaskKind.CUSTOM));
                continue;
            }
            TaskSpec pluginSpec = resolvePluginTask(task, pluginRegistry);
            if (pluginSpec != null) {
                String name = normalize(nameOrId(task.getName(), task.getId()));
                taskSpecs.put(task.getId(), pluginSpec);
                nodes.put(task.getId(), new NodeInfo(task.getId(), name, NodeType.TASK, TaskKind.PLUGIN));
            } else {
                registerTaskSpec(task, TaskKind.SERVICE, taskSpecs, nodes);
            }
        }

        for (Task task : model.getModelElementsByType(Task.class)) {
            if (taskSpecs.containsKey(task.getId())) {
                continue;
            }
            if (task instanceof ServiceTask) {
                continue;
            }
            String name = normalize(nameOrId(task.getName(), task.getId()));
            LOG.warn("Unknown BPMN task type for '{}' (id={}), generating generic auto-completing worker", name, task.getId());
            taskSpecs.put(task.getId(), new TaskSpec(task.getId(), name, TaskKind.GENERIC));
            nodes.put(task.getId(), new NodeInfo(task.getId(), name, NodeType.TASK, TaskKind.GENERIC));
        }
        return new ArrayList<>(taskSpecs.values());
    }

    static PluginRegistry loadPluginRegistry() {
        Path pluginDir = Path.of("plugins");
        if (Files.exists(pluginDir)) {
            try {
                return PluginRegistry.load(pluginDir);
            } catch (IOException e) {
                LOG.warn("Failed to load plugin registry: {}", e.getMessage());
            }
        }
        java.net.URL catalogUrl = BpmnScaffolder.class.getResource("/plugins/catalog.yml");
        if (catalogUrl != null) {
            try {
                return PluginRegistry.load(catalogUrl);
            } catch (IOException e) {
                LOG.warn("Failed to load plugin registry from classpath: {}", e.getMessage());
            }
        }
        return new PluginRegistry();
    }

    static TaskSpec resolvePluginTask(ServiceTask task, PluginRegistry registry) {
        String pluginId = null;
        String pluginConfig = null;
        if (task.getExtensionElements() != null) {
            CamundaProperties props = task.getExtensionElements()
                    .getElementsQuery()
                    .filterByType(CamundaProperties.class)
                    .singleResult();
            if (props != null && props.getCamundaProperties() != null) {
                for (CamundaProperty prop : props.getCamundaProperties()) {
                    String name = prop.getCamundaName();
                    String value = prop.getCamundaValue();
                    if ("plugin".equals(name)) {
                        pluginId = value;
                    } else if ("pluginConfig".equals(name)) {
                        pluginConfig = value;
                    }
                }
            }
        }
        if (pluginId == null || pluginId.isBlank()) {
            return null;
        }
        if (!registry.contains(pluginId)) {
            LOG.warn("Plugin '{}' not found in registry for task {}, falling back to generic worker", pluginId, task.getId());
            return null;
        }
        PluginDescriptor desc = registry.get(pluginId);
        String name = normalize(nameOrId(task.getName(), task.getId()));
        return new TaskSpec(task.getId(), name, TaskKind.PLUGIN, pluginId, pluginConfig,
                desc.implementation.className);
    }

    static TaskSpec resolveCustomTask(ServiceTask task) {
        String pluginId = null;
        String pluginConfig = null;
        String customImpl = null;
        String customSource = null;
        String customHash = null;
        if (task.getExtensionElements() != null) {
            CamundaProperties props = task.getExtensionElements()
                    .getElementsQuery()
                    .filterByType(CamundaProperties.class)
                    .singleResult();
            if (props != null && props.getCamundaProperties() != null) {
                for (CamundaProperty prop : props.getCamundaProperties()) {
                    String name = prop.getCamundaName();
                    String value = prop.getCamundaValue();
                    switch (name) {
                        case "plugin" -> pluginId = value;
                        case "pluginConfig" -> pluginConfig = value;
                        case "customImpl" -> customImpl = value;
                        case "customSource" -> customSource = value;
                        case "customHash" -> customHash = value;
                    }
                }
            }
        }
        if (!"custom".equals(pluginId)) {
            return null;
        }
        String contractName = parseContractName(pluginConfig, task);
        String name = normalize(nameOrId(task.getName(), task.getId()));
        return new TaskSpec(task.getId(), name, TaskKind.CUSTOM, pluginId, pluginConfig,
                null, contractName, customImpl, customSource, customHash);
    }

    static String parseContractName(String pluginConfig, ServiceTask task) {
        if (pluginConfig == null || pluginConfig.isBlank()) {
            String name = normalize(nameOrId(task.getName(), task.getId()));
            return toClassName(name) + "Contract";
        }
        for (String part : pluginConfig.split(";")) {
            part = part.trim();
            if (part.startsWith("interface=")) {
                return part.substring("interface=".length()).trim();
            }
        }
        return pluginConfig.trim();
    }

    static void registerTaskSpec(
            Task task,
            TaskKind kind,
            Map<String, TaskSpec> taskSpecs,
            Map<String, NodeInfo> nodes
    ) {
        String name = normalize(nameOrId(task.getName(), task.getId()));
        taskSpecs.put(task.getId(), new TaskSpec(task.getId(), name, kind));
        nodes.put(task.getId(), new NodeInfo(task.getId(), name, NodeType.TASK, kind));
    }

    static <T extends Task> void registerTaskSpecs(
            Iterable<T> tasks,
            TaskKind kind,
            Map<String, TaskSpec> taskSpecs,
            Map<String, NodeInfo> nodes
    ) {
        for (T task : tasks) {
            registerTaskSpec(task, kind, taskSpecs, nodes);
        }
    }

    // ---- data asset collection ----

    static List<DataObjectSpec> collectDataObjectSpecs(BpmnModelInstance model) {
        Map<String, DataObjectSpec> specs = new LinkedHashMap<>();
        List<String> referencedObjectIds = new ArrayList<>();
        for (DataObjectReference reference : model.getModelElementsByType(DataObjectReference.class)) {
            if (reference.getDataObject() != null) {
                referencedObjectIds.add(reference.getDataObject().getId());
            }
            String name = normalize(nameOrId(reference.getName(), reference.getId()));
            DataObject referenced = reference.getDataObject();
            boolean collection = referenced != null && referenced.isCollection();
            specs.put(reference.getId(), dataObjectSpec(reference.getId(), name, reference,
                    referenced, collection));
        }
        for (DataObject dataObject : model.getModelElementsByType(DataObject.class)) {
            if (referencedObjectIds.contains(dataObject.getId())) {
                continue;
            }
            String name = normalize(nameOrId(dataObject.getName(), dataObject.getId()));
            specs.put(dataObject.getId(), dataObjectSpec(dataObject.getId(), name, dataObject,
                    null, dataObject.isCollection()));
        }
        return new ArrayList<>(specs.values());
    }

    static List<DataStoreSpec> collectDataStoreSpecs(BpmnModelInstance model) {
        List<DataStoreSpec> specs = new ArrayList<>();
        for (DataStoreReference reference : model.getModelElementsByType(DataStoreReference.class)) {
            String name = normalize(nameOrId(reference.getName(), reference.getId()));
            ItemDefinition item = reference.getItemSubject();
            Map<String, String> props = extensionProperties(reference);
            specs.add(new DataStoreSpec(
                    reference.getId(),
                    name,
                    item != null ? item.getId() : null,
                    item != null ? item.getStructureRef() : null,
                    firstNonBlank(props.get("kind"), props.get("type"), inferStoreKind(props.get("uri"))),
                    props.get("uri"),
                    unlimited(reference)
            ));
        }
        return specs;
    }

    static List<DataAssociationSpec> collectDataAssociationSpecs(BpmnModelInstance model) {
        List<DataAssociationSpec> specs = new ArrayList<>();
        for (DataInputAssociation association : model.getModelElementsByType(DataInputAssociation.class)) {
            specs.add(dataAssociationSpec(association, "input"));
        }
        for (DataOutputAssociation association : model.getModelElementsByType(DataOutputAssociation.class)) {
            specs.add(dataAssociationSpec(association, "output"));
        }
        return specs;
    }

    private static DataObjectSpec dataObjectSpec(
            String id,
            String name,
            ItemAwareElement element,
            DataObject referenced,
            boolean collection
    ) {
        ItemDefinition item = element.getItemSubject();
        if (item == null && referenced != null) {
            item = referenced.getItemSubject();
        }
        Map<String, String> props = extensionProperties(element);
        return new DataObjectSpec(
                id,
                name,
                item != null ? item.getId() : null,
                item != null ? item.getStructureRef() : null,
                props.get("mediaType"),
                firstNonBlank(props.get("schema"), item != null ? item.getStructureRef() : null),
                collection
        );
    }

    private static DataAssociationSpec dataAssociationSpec(DataAssociation association, String direction) {
        ModelElementInstance parent = association.getParentElement();
        String taskId = parent instanceof FlowNode flowNode ? flowNode.getId() : null;
        String taskName = parent instanceof FlowNode flowNode
                ? normalize(nameOrId(flowNode.getName(), flowNode.getId()))
                : null;
        List<String> sources = association.getSources().stream()
                .map(BpmnModelCollector::itemAwareName)
                .toList();
        ItemAwareElement target = association.getTarget();
        return new DataAssociationSpec(
                association.getId(),
                taskId,
                taskName,
                direction,
                sources,
                target != null ? itemAwareName(target) : null,
                association.getTransformation() != null
                        ? association.getTransformation().getTextContent()
                        : null
        );
    }

    private static String itemAwareName(ItemAwareElement element) {
        if (element instanceof DataObjectReference reference) {
            return normalize(nameOrId(reference.getName(), reference.getId()));
        }
        if (element instanceof DataObject object) {
            return normalize(nameOrId(object.getName(), object.getId()));
        }
        if (element instanceof DataStoreReference reference) {
            return normalize(nameOrId(reference.getName(), reference.getId()));
        }
        if (element instanceof BaseElement base && base.getId() != null) {
            return normalize(base.getId());
        }
        return "unnamed";
    }

    private static Map<String, String> extensionProperties(ItemAwareElement element) {
        if (!(element instanceof BaseElement base) || base.getExtensionElements() == null) {
            return Map.of();
        }
        CamundaProperties props = base.getExtensionElements()
                .getElementsQuery()
                .filterByType(CamundaProperties.class)
                .singleResult();
        if (props == null || props.getCamundaProperties() == null) {
            return Map.of();
        }
        Map<String, String> values = new LinkedHashMap<>();
        for (CamundaProperty prop : props.getCamundaProperties()) {
            values.put(prop.getCamundaName(), prop.getCamundaValue());
        }
        return values;
    }

    private static boolean unlimited(DataStoreReference reference) {
        return reference.getDataStore() != null
                && Boolean.TRUE.equals(reference.getDataStore().isUnlimited());
    }

    private static String inferStoreKind(String uri) {
        if (uri == null || uri.isBlank()) {
            return null;
        }
        int idx = uri.indexOf(':');
        return idx > 0 ? uri.substring(0, idx).toLowerCase(Locale.ROOT) : null;
    }

    private static String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        return second != null && !second.isBlank() ? second : null;
    }

    private static String firstNonBlank(String first, String second, String third) {
        String value = firstNonBlank(first, second);
        return value != null ? value : firstNonBlank(third, null);
    }

    // ---- timer collection ----

    static List<TimerSpec> collectTimerSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<TimerSpec> timers = new ArrayList<>();
        for (IntermediateCatchEvent event : model.getModelElementsByType(IntermediateCatchEvent.class)) {
            TimerEventDefinition timerDefinition = event.getEventDefinitions().stream()
                    .filter(TimerEventDefinition.class::isInstance)
                    .map(TimerEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (timerDefinition == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            TimerSpec timerSpec = timerSpec(event.getId(), name, timerDefinition);
            timers.add(timerSpec);
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.TIMER));
        }
        return timers;
    }

    static TimerSpec timerSpec(String id, String name, TimerEventDefinition definition) {
        TimeDuration timeDuration = definition.getTimeDuration();
        if (timeDuration != null && timeDuration.getTextContent() != null) {
            return new TimerSpec(id, name, "timeDuration", timeDuration.getTextContent().trim());
        }
        TimeDate timeDate = definition.getTimeDate();
        if (timeDate != null && timeDate.getTextContent() != null) {
            return new TimerSpec(id, name, "timeDate", timeDate.getTextContent().trim());
        }
        TimeCycle timeCycle = definition.getTimeCycle();
        if (timeCycle != null && timeCycle.getTextContent() != null) {
            return new TimerSpec(id, name, "timeCycle", timeCycle.getTextContent().trim());
        }
        return new TimerSpec(id, name, "timeDuration", "PT0S");
    }

    // ---- multi-instance collection ----

    static List<MultiInstanceSpec> collectMultiInstanceSpecs(BpmnModelInstance model) {
        List<MultiInstanceSpec> specs = new ArrayList<>();
        for (Task task : model.getModelElementsByType(Task.class)) {
            if (!(task.getLoopCharacteristics() instanceof MultiInstanceLoopCharacteristics loop)) {
                continue;
            }
            String name = normalize(nameOrId(task.getName(), task.getId()));
            String cardinality = loop.getLoopCardinality() != null ? loop.getLoopCardinality().getTextContent() : null;
            String completion = loop.getCompletionCondition() != null ? loop.getCompletionCondition().getTextContent() : null;
            specs.add(new MultiInstanceSpec(task.getId(), name, loop.isSequential(), cardinality, completion));
            LOG.info("Multi-instance task '{}' (sequential={}, cardinality={})", name, loop.isSequential(), cardinality);
        }
        for (SubProcess sub : model.getModelElementsByType(SubProcess.class)) {
            if (!(sub.getLoopCharacteristics() instanceof MultiInstanceLoopCharacteristics loop)) {
                continue;
            }
            String name = normalize(nameOrId(sub.getName(), sub.getId()));
            String cardinality = loop.getLoopCardinality() != null ? loop.getLoopCardinality().getTextContent() : null;
            String completion = loop.getCompletionCondition() != null ? loop.getCompletionCondition().getTextContent() : null;
            specs.add(new MultiInstanceSpec(sub.getId(), name, loop.isSequential(), cardinality, completion));
            LOG.info("Multi-instance subprocess '{}' (sequential={}, cardinality={})", name, loop.isSequential(), cardinality);
        }
        return specs;
    }

    // ---- event sub-process collection ----

    static List<EventSubProcessSpec> collectEventSubProcessSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<EventSubProcessSpec> specs = new ArrayList<>();
        for (SubProcess subProcess : model.getModelElementsByType(SubProcess.class)) {
            if (!isEventSubProcess(subProcess)) {
                continue;
            }
            String name = normalize(nameOrId(subProcess.getName(), subProcess.getId()));
            StartEvent triggerStart = subProcess.getChildElementsByType(StartEvent.class).stream().findFirst().orElse(null);
            if (triggerStart == null) {
                continue;
            }

            MessageEventDefinition messageDefinition = triggerStart.getEventDefinitions().stream()
                    .filter(MessageEventDefinition.class::isInstance)
                    .map(MessageEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            SignalEventDefinition signalDefinition = triggerStart.getEventDefinitions().stream()
                    .filter(SignalEventDefinition.class::isInstance)
                    .map(SignalEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            TimerEventDefinition timerDefinition = triggerStart.getEventDefinitions().stream()
                    .filter(TimerEventDefinition.class::isInstance)
                    .map(TimerEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            ErrorEventDefinition errorDefinition = triggerStart.getEventDefinitions().stream()
                    .filter(ErrorEventDefinition.class::isInstance)
                    .map(ErrorEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            EscalationEventDefinition escalationDefinition = triggerStart.getEventDefinitions().stream()
                    .filter(EscalationEventDefinition.class::isInstance)
                    .map(EscalationEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (messageDefinition == null && signalDefinition == null && timerDefinition == null
                    && errorDefinition == null && escalationDefinition == null) {
                continue;
            }

            EventTriggerKind triggerKind;
            String triggerName;
            String triggerTopic;
            String timerType = null;
            String timerExpression = null;
            if (messageDefinition != null) {
                triggerKind = EventTriggerKind.MESSAGE;
                triggerName = messageName(name, messageDefinition);
                triggerTopic = processMessageTopic(model, triggerName);
            } else if (signalDefinition != null) {
                triggerKind = EventTriggerKind.SIGNAL;
                triggerName = signalName(name, signalDefinition);
                triggerTopic = processSignalTopic(model, triggerName);
            } else if (errorDefinition != null) {
                String enclosingScopeId = enclosingSubProcessId(subProcess);
                if (enclosingScopeId == null) {
                    continue;
                }
                triggerKind = EventTriggerKind.ERROR;
                triggerName = errorDefinition.getError() != null && errorDefinition.getError().getErrorCode() != null
                        ? normalize(errorDefinition.getError().getErrorCode())
                        : name + "_error";
                triggerTopic = null;
            } else if (escalationDefinition != null) {
                String enclosingScopeId = enclosingSubProcessId(subProcess);
                if (enclosingScopeId == null) {
                    continue;
                }
                triggerKind = EventTriggerKind.ESCALATION;
                triggerName = escalationDefinition.getEscalation() != null
                        && escalationDefinition.getEscalation().getEscalationCode() != null
                        ? normalize(escalationDefinition.getEscalation().getEscalationCode())
                        : name + "_escalation";
                triggerTopic = null;
            } else {
                String enclosingScopeId = enclosingSubProcessId(subProcess);
                if (enclosingScopeId == null) {
                    continue;
                }
                triggerKind = EventTriggerKind.TIMER;
                TimerSpec timerSpec = timerSpec(triggerStart.getId(), name, timerDefinition);
                triggerName = timerSpec.name;
                triggerTopic = null;
                timerType = timerSpec.timerType;
                timerExpression = timerSpec.expression;
            }

            List<String> entryTargetIds = new ArrayList<>();
            List<String> exitSourceIds = new ArrayList<>();
            for (SequenceFlow flow : model.getModelElementsByType(SequenceFlow.class)) {
                FlowNode source = flow.getSource();
                FlowNode target = flow.getTarget();
                if (source instanceof StartEvent && subProcess.getId().equals(enclosingSubProcessId(source))) {
                    entryTargetIds.add(target.getId());
                }
                if (target instanceof EndEvent && subProcess.getId().equals(enclosingSubProcessId(target))) {
                    exitSourceIds.add(source.getId());
                }
            }

            List<String> scopeActivityIds = new ArrayList<>();
            for (FlowNode flowNode : model.getModelElementsByType(FlowNode.class)) {
                if (!isWithinSubProcess(flowNode, subProcess.getId())) {
                    continue;
                }
                if (flowNode instanceof StartEvent || flowNode instanceof EndEvent) {
                    continue;
                }
                scopeActivityIds.add(normalize(nameOrId(flowNode.getName(), flowNode.getId())));
            }
            String enclosingScopeId = enclosingSubProcessId(subProcess);
            List<String> cancellationScopeActivityIds = collectEventSubProcessCancellationScopeActivityIds(
                    model,
                    subProcess.getId(),
                    enclosingScopeId
            );

            specs.add(new EventSubProcessSpec(
                    subProcess.getId(),
                    name,
                    triggerKind,
                    triggerName,
                    triggerTopic,
                    timerType,
                    timerExpression,
                    enclosingScopeId != null ? normalize(nameOrId(
                            ((SubProcess) model.getModelElementById(enclosingScopeId)).getName(),
                            enclosingScopeId
                    )) : null,
                    isInterruptingStart(triggerStart),
                    cancellationScopeActivityIds,
                    distinct(entryTargetIds),
                    distinct(exitSourceIds),
                    distinct(scopeActivityIds)
            ));
        }
        return specs;
    }

    static List<String> collectEventSubProcessCancellationScopeActivityIds(
            BpmnModelInstance model,
            String eventSubProcessId,
            String enclosingScopeId
    ) {
        List<String> activityIds = new ArrayList<>();
        for (FlowNode flowNode : model.getModelElementsByType(FlowNode.class)) {
            if (flowNode instanceof StartEvent || flowNode instanceof EndEvent) {
                continue;
            }
            if (isWithinSubProcess(flowNode, eventSubProcessId)) {
                continue;
            }
            String flowNodeEnclosingScope = enclosingSubProcessId(flowNode);
            if (enclosingScopeId == null) {
                if (flowNodeEnclosingScope != null) {
                    continue;
                }
            } else if (!enclosingScopeId.equals(flowNodeEnclosingScope)) {
                continue;
            }
            activityIds.add(normalize(nameOrId(flowNode.getName(), flowNode.getId())));
        }
        if (enclosingScopeId != null) {
            ModelElementInstance enclosingScope = model.getModelElementById(enclosingScopeId);
            if (enclosingScope instanceof SubProcess subProcess) {
                activityIds.add(normalize(nameOrId(subProcess.getName(), subProcess.getId())));
            }
        }
        return distinct(activityIds);
    }

    // ---- sub-process collection ----

    static List<SubProcessSpec> collectSubProcessSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<SubProcessSpec> specs = new ArrayList<>();
        for (SubProcess subProcess : model.getModelElementsByType(SubProcess.class)) {
            if (isEventSubProcess(subProcess)) {
                continue;
            }
            String name = normalize(nameOrId(subProcess.getName(), subProcess.getId()));
            nodes.put(subProcess.getId(), new NodeInfo(subProcess.getId(), name, NodeType.SUB_PROCESS));
            List<String> entryTargetIds = new ArrayList<>();
            List<String> exitSourceIds = new ArrayList<>();
            for (SequenceFlow flow : model.getModelElementsByType(SequenceFlow.class)) {
                FlowNode source = flow.getSource();
                FlowNode target = flow.getTarget();
                if (source instanceof StartEvent && subProcess.getId().equals(enclosingSubProcessId(source))) {
                    entryTargetIds.add(target.getId());
                }
                if (target instanceof EndEvent && subProcess.getId().equals(enclosingSubProcessId(target))) {
                    exitSourceIds.add(source.getId());
                }
            }
            List<String> scopeActivityIds = new ArrayList<>();
            for (FlowNode flowNode : model.getModelElementsByType(FlowNode.class)) {
                if (!isWithinSubProcess(flowNode, subProcess.getId())) {
                    continue;
                }
                if (flowNode instanceof StartEvent || flowNode instanceof EndEvent) {
                    continue;
                }
                scopeActivityIds.add(normalize(nameOrId(flowNode.getName(), flowNode.getId())));
            }
            specs.add(new SubProcessSpec(subProcess.getId(), name, distinct(entryTargetIds), distinct(exitSourceIds), distinct(scopeActivityIds)));
        }
        return specs;
    }

    // ---- message collection ----

    static List<MessageCatchSpec> collectMessageCatchSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<MessageCatchSpec> specs = new ArrayList<>();
        for (IntermediateCatchEvent event : model.getModelElementsByType(IntermediateCatchEvent.class)) {
            MessageEventDefinition messageDefinition = event.getEventDefinitions().stream()
                    .filter(MessageEventDefinition.class::isInstance)
                    .map(MessageEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (messageDefinition == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String messageName = messageName(name, messageDefinition);
            specs.add(new MessageCatchSpec(event.getId(), name, messageName, processMessageTopic(model, messageName)));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.MESSAGE_CATCH));
        }
        return specs;
    }

    static List<MessageThrowSpec> collectMessageThrowSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<MessageThrowSpec> specs = new ArrayList<>();
        for (IntermediateThrowEvent event : model.getModelElementsByType(IntermediateThrowEvent.class)) {
            MessageEventDefinition messageDefinition = event.getEventDefinitions().stream()
                    .filter(MessageEventDefinition.class::isInstance)
                    .map(MessageEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (messageDefinition == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String messageName = messageName(name, messageDefinition);
            specs.add(new MessageThrowSpec(event.getId(), name, messageName, processMessageTopic(model, messageName)));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.MESSAGE_THROW));
        }
        return specs;
    }

    static String messageName(String fallbackName, MessageEventDefinition definition) {
        if (definition.getMessage() != null && definition.getMessage().getName() != null && !definition.getMessage().getName().isBlank()) {
            return normalize(definition.getMessage().getName());
        }
        return fallbackName;
    }

    static String processMessageTopic(BpmnModelInstance model, String messageName) {
        Process process = model.getModelElementsByType(Process.class).stream().findFirst().orElse(null);
        String processId = process != null ? normalize(process.getId()) : "process";
        return processId + "_" + messageName + "_message";
    }

    static List<String> collectMessageEvents(
            List<MessageCatchSpec> catches,
            List<MessageThrowSpec> throwsEvents,
            List<EventSubProcessSpec> eventSubProcesses
    ) {
        List<String> events = new ArrayList<>();
        catches.forEach(spec -> events.add(spec.name));
        for (MessageThrowSpec spec : throwsEvents) {
            if (!events.contains(spec.name)) {
                events.add(spec.name);
            }
        }
        for (EventSubProcessSpec spec : eventSubProcesses) {
            if (spec.triggerKind == EventTriggerKind.MESSAGE && !events.contains(spec.triggerName)) {
                events.add(spec.triggerName);
            }
        }
        return events;
    }

    static List<String> collectMessageTopics(
            List<MessageCatchSpec> catches,
            List<MessageThrowSpec> throwsEvents,
            List<EventSubProcessSpec> eventSubProcesses
    ) {
        List<String> topics = new ArrayList<>();
        for (MessageCatchSpec spec : catches) {
            if (!topics.contains(spec.topic)) {
                topics.add(spec.topic);
            }
        }
        for (MessageThrowSpec spec : throwsEvents) {
            if (!topics.contains(spec.topic)) {
                topics.add(spec.topic);
            }
        }
        for (EventSubProcessSpec spec : eventSubProcesses) {
            if (spec.triggerKind == EventTriggerKind.MESSAGE && !topics.contains(spec.triggerTopic)) {
                topics.add(spec.triggerTopic);
            }
        }
        return topics;
    }

    // ---- signal collection ----

    static List<SignalCatchSpec> collectSignalCatchSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<SignalCatchSpec> specs = new ArrayList<>();
        for (IntermediateCatchEvent event : model.getModelElementsByType(IntermediateCatchEvent.class)) {
            SignalEventDefinition signalDefinition = event.getEventDefinitions().stream()
                    .filter(SignalEventDefinition.class::isInstance)
                    .map(SignalEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (signalDefinition == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String signalName = signalName(name, signalDefinition);
            specs.add(new SignalCatchSpec(event.getId(), name, signalName, processSignalTopic(model, signalName)));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.SIGNAL_CATCH));
        }
        return specs;
    }

    static List<SignalThrowSpec> collectSignalThrowSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<SignalThrowSpec> specs = new ArrayList<>();
        for (IntermediateThrowEvent event : model.getModelElementsByType(IntermediateThrowEvent.class)) {
            SignalEventDefinition signalDefinition = event.getEventDefinitions().stream()
                    .filter(SignalEventDefinition.class::isInstance)
                    .map(SignalEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (signalDefinition == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String signalName = signalName(name, signalDefinition);
            specs.add(new SignalThrowSpec(event.getId(), name, signalName, processSignalTopic(model, signalName)));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.SIGNAL_THROW));
        }
        return specs;
    }

    static String signalName(String fallbackName, SignalEventDefinition definition) {
        if (definition.getSignal() != null && definition.getSignal().getName() != null && !definition.getSignal().getName().isBlank()) {
            return normalize(definition.getSignal().getName());
        }
        return fallbackName;
    }

    static String processSignalTopic(BpmnModelInstance model, String signalName) {
        Process process = model.getModelElementsByType(Process.class).stream().findFirst().orElse(null);
        String processId = process != null ? normalize(process.getId()) : "process";
        return processId + "_" + signalName + "_signal";
    }

    static List<String> collectSignalEvents(
            List<SignalCatchSpec> catches,
            List<SignalThrowSpec> throwsEvents,
            List<EventSubProcessSpec> eventSubProcesses
    ) {
        List<String> events = new ArrayList<>();
        catches.forEach(spec -> events.add(spec.name));
        for (SignalThrowSpec spec : throwsEvents) {
            if (!events.contains(spec.name)) {
                events.add(spec.name);
            }
        }
        for (EventSubProcessSpec spec : eventSubProcesses) {
            if (spec.triggerKind == EventTriggerKind.SIGNAL && !events.contains(spec.triggerName)) {
                events.add(spec.triggerName);
            }
        }
        return events;
    }

    static List<String> collectSignalTopics(
            List<SignalCatchSpec> catches,
            List<SignalThrowSpec> throwsEvents,
            List<EventSubProcessSpec> eventSubProcesses
    ) {
        List<String> topics = new ArrayList<>();
        for (SignalCatchSpec spec : catches) {
            if (!topics.contains(spec.topic)) {
                topics.add(spec.topic);
            }
        }
        for (SignalThrowSpec spec : throwsEvents) {
            if (!topics.contains(spec.topic)) {
                topics.add(spec.topic);
            }
        }
        for (EventSubProcessSpec spec : eventSubProcesses) {
            if (spec.triggerKind == EventTriggerKind.SIGNAL && !topics.contains(spec.triggerTopic)) {
                topics.add(spec.triggerTopic);
            }
        }
        return topics;
    }

    // ---- call activity collection ----

    static List<CallActivitySpec> collectCallActivitySpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<CallActivitySpec> specs = new ArrayList<>();
        for (CallActivity callActivity : model.getModelElementsByType(CallActivity.class)) {
            String name = normalize(nameOrId(callActivity.getName(), callActivity.getId()));
            String calledElement = callActivity.getCalledElement() != null && !callActivity.getCalledElement().isBlank()
                    ? normalize(callActivity.getCalledElement())
                    : name + "_called_process";
            specs.add(new CallActivitySpec(callActivity.getId(), name, calledElement));
            nodes.put(callActivity.getId(), new NodeInfo(callActivity.getId(), name, NodeType.CALL_ACTIVITY, TaskKind.CALL));
        }
        return specs;
    }

    // ---- boundary event collection ----

    static List<BoundaryTimerSpec> collectBoundaryTimerSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<BoundaryTimerSpec> specs = new ArrayList<>();
        for (BoundaryEvent event : model.getModelElementsByType(BoundaryEvent.class)) {
            TimerEventDefinition timerDefinition = event.getEventDefinitions().stream()
                    .filter(TimerEventDefinition.class::isInstance)
                    .map(TimerEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (timerDefinition == null || event.getAttachedTo() == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String attachedActivityId = normalize(nameOrId(event.getAttachedTo().getName(), event.getAttachedTo().getId()));
            TimerSpec timerSpec = timerSpec(event.getId(), name, timerDefinition);
            specs.add(new BoundaryTimerSpec(
                    event.getId(),
                    name,
                    timerSpec.timerType,
                    timerSpec.expression,
                    attachedActivityId,
                    event.cancelActivity()
            ));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.BOUNDARY_TIMER));
        }
        return specs;
    }

    static List<BoundaryErrorSpec> collectBoundaryErrorSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<BoundaryErrorSpec> specs = new ArrayList<>();
        for (BoundaryEvent event : model.getModelElementsByType(BoundaryEvent.class)) {
            ErrorEventDefinition errorDefinition = event.getEventDefinitions().stream()
                    .filter(ErrorEventDefinition.class::isInstance)
                    .map(ErrorEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (errorDefinition == null || event.getAttachedTo() == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String attachedActivityId = normalize(nameOrId(event.getAttachedTo().getName(), event.getAttachedTo().getId()));
            String errorCode = errorDefinition.getError() != null && errorDefinition.getError().getErrorCode() != null
                    ? normalize(errorDefinition.getError().getErrorCode())
                    : name + "_error";
            specs.add(new BoundaryErrorSpec(event.getId(), name, attachedActivityId, errorCode, event.cancelActivity()));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.BOUNDARY_ERROR));
        }
        return specs;
    }

    static List<BoundaryEscalationSpec> collectBoundaryEscalationSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<BoundaryEscalationSpec> specs = new ArrayList<>();
        for (BoundaryEvent event : model.getModelElementsByType(BoundaryEvent.class)) {
            EscalationEventDefinition escalationDefinition = event.getEventDefinitions().stream()
                    .filter(EscalationEventDefinition.class::isInstance)
                    .map(EscalationEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (escalationDefinition == null || event.getAttachedTo() == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String attachedActivityId = normalize(nameOrId(event.getAttachedTo().getName(), event.getAttachedTo().getId()));
            String escalationCode = escalationDefinition.getEscalation() != null
                    && escalationDefinition.getEscalation().getEscalationCode() != null
                    ? normalize(escalationDefinition.getEscalation().getEscalationCode())
                    : name + "_escalation";
            specs.add(new BoundaryEscalationSpec(event.getId(), name, attachedActivityId, escalationCode, event.cancelActivity()));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.BOUNDARY_ESCALATION));
        }
        return specs;
    }

    // ---- tree-walking helpers ----

    static String enclosingSubProcessId(ModelElementInstance element) {
        ModelElementInstance current = element != null ? element.getParentElement() : null;
        while (current != null) {
            if (current instanceof SubProcess subProcess) {
                return subProcess.getId();
            }
            current = current.getParentElement();
        }
        return null;
    }

    static boolean isWithinSubProcess(ModelElementInstance element, String subProcessId) {
        ModelElementInstance current = element != null ? element.getParentElement() : null;
        while (current != null) {
            if (current instanceof SubProcess subProcess && subProcessId.equals(subProcess.getId())) {
                return true;
            }
            current = current.getParentElement();
        }
        return false;
    }

    static boolean isEventSubProcess(SubProcess subProcess) {
        if (subProcess == null) {
            return false;
        }
        String value = subProcess.getAttributeValue("triggeredByEvent");
        return Boolean.parseBoolean(value);
    }

    static boolean isInterruptingStart(StartEvent startEvent) {
        if (startEvent == null) {
            return false;
        }
        String value = startEvent.getAttributeValue("isInterrupting");
        return value == null || value.isBlank() || Boolean.parseBoolean(value);
    }

    // ---- sub-process linking helpers ----

    static void linkSubProcessEntries(
            NodeInfo sourceInfo,
            List<SubProcessSpec> subProcessSpecs,
            String subProcessId,
            FlowInfo flowInfo,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource
    ) {
        if (sourceInfo == null) {
            return;
        }
        SubProcessSpec spec = findSubProcessSpec(subProcessSpecs, subProcessId);
        if (spec == null) {
            return;
        }
        for (String entryTargetId : spec.entryTargetIds) {
            NodeInfo targetInfo = nodes.get(entryTargetId);
            if (targetInfo == null) {
                continue;
            }
            sourceInfo.outgoingIds.add(targetInfo.id);
            targetInfo.incomingIds.add(sourceInfo.id);
            flowsBySource.computeIfAbsent(sourceInfo.id, key -> new ArrayList<>())
                    .add(new FlowInfo(flowInfo.id, targetInfo.id, flowInfo.condition));
        }
    }

    static void linkSubProcessExits(
            NodeInfo targetInfo,
            List<SubProcessSpec> subProcessSpecs,
            String subProcessId,
            FlowInfo flowInfo,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource
    ) {
        if (targetInfo == null) {
            return;
        }
        SubProcessSpec spec = findSubProcessSpec(subProcessSpecs, subProcessId);
        if (spec == null) {
            return;
        }
        for (String exitSourceId : spec.exitSourceIds) {
            NodeInfo sourceInfo = nodes.get(exitSourceId);
            if (sourceInfo == null) {
                continue;
            }
            sourceInfo.outgoingIds.add(targetInfo.id);
            targetInfo.incomingIds.add(sourceInfo.id);
            flowsBySource.computeIfAbsent(sourceInfo.id, key -> new ArrayList<>())
                    .add(new FlowInfo(flowInfo.id, targetInfo.id, flowInfo.condition));
        }
    }

    static SubProcessSpec findSubProcessSpec(List<SubProcessSpec> subProcessSpecs, String subProcessId) {
        for (SubProcessSpec spec : subProcessSpecs) {
            if (spec.id.equals(subProcessId)) {
                return spec;
            }
        }
        return null;
    }

    // ---- general helpers ----

    static List<String> distinct(List<String> values) {
        List<String> distinct = new ArrayList<>();
        for (String value : values) {
            if (!distinct.contains(value)) {
                distinct.add(value);
            }
        }
        return distinct;
    }

    static List<String> combineNames(List<String> primary, List<String> secondary) {
        List<String> combined = new ArrayList<>(primary);
        for (String value : secondary) {
            if (!combined.contains(value)) {
                combined.add(value);
            }
        }
        return combined;
    }

    static List<String> collectTerminalOutputs(Map<String, NodeInfo> nodes) {
        List<String> outputs = new ArrayList<>();
        for (NodeInfo node : nodes.values()) {
            if (node.type == NodeType.END) {
                continue;
            }
            for (String targetId : node.outgoingIds) {
                NodeInfo target = nodes.get(targetId);
                if (target != null && target.type == NodeType.END) {
                    outputs.add(node.name + "_output");
                }
            }
        }
        return outputs;
    }
}
