#!/usr/bin/env python3
"""Add missing BPMNEdge elements to invoice BPMN models. Preserves all existing XML."""

import xml.etree.ElementTree as ET
import sys, os, re

BPMN_NS = 'http://www.omg.org/spec/BPMN/20100524/MODEL'
BPMNDI_NS = 'http://www.omg.org/spec/BPMN/20100524/DI'
DC_NS = 'http://www.omg.org/spec/DD/20100524/DC'
DI_NS = 'http://www.omg.org/spec/DD/20100524/DI'

# Must be called before any ET.parse() to preserve namespace prefixes
ET.register_namespace('', BPMN_NS)
ET.register_namespace('bpmndi', BPMNDI_NS)
ET.register_namespace('omgdc', DC_NS)
ET.register_namespace('omgdi', DI_NS)
ET.register_namespace('xsi', 'http://www.w3.org/2001/XMLSchema-instance')

def add_bpmnedges(filepath):
    tree = ET.parse(filepath)
    root = tree.getroot()

    # Find process (in default namespace)
    process = root.find(f'{{{BPMN_NS}}}process')
    if process is None:
        print(f"  No process found")
        return 0

    # Collect sequence flows
    seq_flows = process.findall(f'{{{BPMN_NS}}}sequenceFlow')
    if not seq_flows:
        print(f"  No sequence flows")
        return 0

    # Find or create BPMNDiagram/Plane
    diagram = root.find(f'{{{BPMNDI_NS}}}BPMNDiagram')
    if diagram is None:
        diagram = ET.SubElement(root, f'{{{BPMNDI_NS}}}BPMNDiagram')
        diagram.set('id', 'BPMNDiagram_auto')
    plane = diagram.find(f'{{{BPMNDI_NS}}}BPMNPlane')
    if plane is None:
        plane = ET.SubElement(diagram, f'{{{BPMNDI_NS}}}BPMNPlane')
        plane.set('id', 'BPMNPlane_auto')

    # Build shape dictionary: element_id -> (x, y, w, h)
    shapes = {}
    for shape in plane.findall(f'{{{BPMNDI_NS}}}BPMNShape'):
        elem_id = shape.get('bpmnElement')
        if not elem_id:
            continue
        bounds = shape.find(f'{{{DC_NS}}}Bounds')
        if bounds is None:
            continue
        x = float(bounds.get('x', 0))
        y = float(bounds.get('y', 0))
        w = float(bounds.get('width', 100))
        h = float(bounds.get('height', 80))
        shapes[elem_id] = (x, y, w, h)

    # Existing edges (by bpmnElement)
    existing = set()
    for edge in plane.findall(f'{{{BPMNDI_NS}}}BPMNEdge'):
        be = edge.get('bpmnElement')
        if be:
            existing.add(be)

    added = 0
    for sf in seq_flows:
        sf_id = sf.get('id')
        if sf_id in existing:
            continue
        src = sf.get('sourceRef')
        tgt = sf.get('targetRef')
        if src not in shapes or tgt not in shapes:
            continue

        sx, sy, sw, sh = shapes[src]
        tx, ty, tw, th = shapes[tgt]

        # Waypoints: source bottom-center -> target top-center
        w1 = (int(sx + sw/2), int(sy + sh))
        w2 = (int(tx + tw/2), int(ty))

        edge = ET.SubElement(plane, f'{{{BPMNDI_NS}}}BPMNEdge')
        edge.set('id', sf_id + '_di')
        edge.set('bpmnElement', sf_id)

        wp = ET.SubElement(edge, f'{{{DI_NS}}}waypoint')
        wp.set('x', str(w1[0])); wp.set('y', str(w1[1]))
        wp = ET.SubElement(edge, f'{{{DI_NS}}}waypoint')
        wp.set('x', str(w2[0])); wp.set('y', str(w2[1]))

        added += 1

    if added > 0:
        tree.write(filepath, encoding='UTF-8', xml_declaration=True)

    return added


def main():
    base = sys.argv[1] if len(sys.argv) > 1 else 'durga-tools/src/test/resources/bpmn'
    files = sorted(f for f in os.listdir(base) if f.startswith('invoice_') and f.endswith('.bpmn'))
    if not files:
        print(f"No invoice_*.bpmn files found in {base}")
        return
    print(f"Found {len(files)} invoice models")
    total = 0
    for f in files:
        fp = os.path.join(base, f)
        n = add_bpmnedges(fp)
        total += n
        print(f"  {f}: {n} edges added")
    print(f"\nTotal: {total} edges added across {len(files)} files")


if __name__ == '__main__':
    main()
