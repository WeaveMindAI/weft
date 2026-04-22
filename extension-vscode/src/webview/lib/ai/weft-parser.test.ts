import { describe, it, expect } from 'vitest';
import { parseWeft } from './weft-parser';

/** Wrap raw weft code in a 4-backtick fence so parseWeft can extract it. */
function weft(code: string) {
	return '````weft\n' + code.trim() + '\n````';
}

/** Parse weft and return the first project (convenience). */
function parse(code: string) {
	const result = parseWeft(weft(code));
	expect(result.projects.length).toBeGreaterThan(0);
	return result.projects[0];
}

/** Parse weft and expect no errors. Returns the project.
 *  Filters out validation errors (e.g. "required input port not connected")
 *  that aren't relevant to parser-syntax tests. Pure parse/enrichment errors
 *  still fail the test. */
function parseOk(code: string) {
	const p = parse(code);
	const nonValidation = p.errors.filter(e =>
		!e.message.includes('required input port')
		&& !e.message.includes('must be connected')
		&& !e.message.includes('Config is required')
		&& !e.message.includes('at least one of [')
	);
	if (nonValidation.length > 0) {
		throw new Error(`Expected no errors, got: ${nonValidation.map(e => `line ${e.line}: ${e.message}`).join(', ')}`);
	}
	return p;
}

describe('parseWeft', () => {
	// ─── Basic Parsing ──────────────────────────────────────────────────

	it('parses a basic project with metadata', () => {
		const p = parseOk(`
# Project: Test
# Description: A test project

node = Text { value: "hello" }
		`);
		expect(p.project.name).toBe('Test');
		expect(p.project.description).toBe('A test project');
		expect(p.project.nodes.length).toBe(1);
		expect(p.project.nodes[0].id).toBe('node');
	});

	it('parses bare node', () => {
		const p = parseOk(`
# Project: Bare
node = Text
		`);
		expect(p.project.nodes.length).toBe(1);
		expect(p.project.nodes[0].nodeType).toBe('Text');
	});

	it('parses node with ports', () => {
		const p = parseOk(`
# Project: Ports
worker = ExecPython(
    data: String,
    context: String?
) -> (
    result: String,
    score: Number?
) {
    code: "return {}"
}
		`);
		const node = p.project.nodes[0];
		expect(node.inputs.length).toBeGreaterThanOrEqual(2);
		const dataPort = node.inputs.find(i => i.name === 'data');
		const contextPort = node.inputs.find(i => i.name === 'context');
		expect(dataPort).toBeDefined();
		expect(dataPort!.required).toBe(true);
		expect(contextPort).toBeDefined();
		expect(contextPort!.required).toBe(false);
		expect(node.outputs.length).toBeGreaterThanOrEqual(2);
		expect(node.outputs.find(o => o.name === 'result')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'score')!.required).toBe(false);
	});

	it('parses node with no config', () => {
		const p = parseOk(`
# Project: NoConfig
pass = ExecPython(data: String) -> (result: String)
		`);
		const node = p.project.nodes[0];
		expect(node.inputs.find(i => i.name === 'data')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'result')).toBeDefined();
	});

	it('parses empty inputs', () => {
		const p = parseOk(`
# Project: EmptyIn
gen = ExecPython() -> (result: String) {
    code: "return {}"
}
		`);
		const node = p.project.nodes[0];
		expect(node.outputs.find(o => o.name === 'result')).toBeDefined();
	});

	// ─── Port Types ─────────────────────────────────────────────────────

	it('parses complex port types', () => {
		const p = parseOk(`
# Project: Types
node = ExecPython(
    text: String,
    nums: List[Number],
    data: Dict[String, String]
) -> (
    result: String | Number,
    items: List[List[String]]
) {}
		`);
		const node = p.project.nodes[0];
		expect(node.inputs.find(i => i.name === 'nums')!.portType).toBe('List[Number]');
		expect(node.inputs.find(i => i.name === 'data')!.portType).toBe('Dict[String, String]');
		expect(node.outputs.find(o => o.name === 'result')!.portType).toBe('String | Number');
		expect(node.outputs.find(o => o.name === 'items')!.portType).toBe('List[List[String]]');
	});

	it('parses Media type alias', () => {
		const p = parseOk(`
# Project: Media
node = ExecPython(input: Media) -> (result: String) {
    code: "return {}"
}
		`);
		const node = p.project.nodes[0];
		const inputPort = node.inputs.find(i => i.name === 'input');
		expect(inputPort).toBeDefined();
		// Media is either expanded to the union or kept as "Media" depending on parser stage
		const pt = inputPort!.portType;
		expect(pt === 'Media' || pt.includes('Image')).toBe(true);
	});

	it('parses Null in union type', () => {
		const p = parseOk(`
# Project: NullType
node = ExecPython(data: String | Null) -> (result: String | Null) {}
		`);
		const node = p.project.nodes[0];
		expect(node.inputs.find(i => i.name === 'data')!.portType).toContain('Null');
		expect(node.outputs.find(o => o.name === 'result')!.portType).toContain('Null');
	});

	it('parses TypeVar ports', () => {
		const p = parseOk(`
# Project: TypeVar
node = ExecPython(data: T) -> (result: T) {}
		`);
		const node = p.project.nodes[0];
		expect(node.inputs.find(i => i.name === 'data')!.portType).toBe('T');
		expect(node.outputs.find(o => o.name === 'result')!.portType).toBe('T');
	});

	it('parses MustOverride (no type annotation)', () => {
		// MustOverride ports on custom ports produce validation errors,
		// but the port should still be parsed with MustOverride type.
		const p = parse(`
# Project: MustOverride
node = ExecPython(data) -> (result) {}
		`);
		const node = p.project.nodes[0];
		expect(node.inputs.find(i => i.name === 'data')!.portType).toBe('MustOverride');
		expect(node.outputs.find(o => o.name === 'result')!.portType).toBe('MustOverride');
	});

	// ─── Connections ────────────────────────────────────────────────────

	it('parses connections with = syntax', () => {
		const p = parseOk(`
# Project: Connections
a = Text { value: "hi" }
b = Debug {}
b.data = a.value
		`);
		expect(p.project.edges.length).toBe(1);
		const edge = p.project.edges[0];
		expect(edge.target).toBe('b');
		expect(edge.targetHandle).toBe('data');
		expect(edge.source).toBe('a');
		expect(edge.sourceHandle).toBe('value');
	});

	it('parses multiple connections', () => {
		const p = parseOk(`
# Project: MultiConn
a = Text { value: "hi" }
cfg = LlmConfig { model: "anthropic/claude-sonnet-4.6" }
b = LlmInference() -> (response: String) {}
c = Debug(data: String) {}
b.prompt = a.value
b.config = cfg.config
c.data = b.response
		`);
		expect(p.project.edges.length).toBe(3);
	});

	// ─── Groups ─────────────────────────────────────────────────────────

	it('parses basic group', () => {
		const p = parseOk(`
# Project: Group
preprocessor = Group(raw: String) -> (result: String) {
    clean = ExecPython(text: String) -> (output: String) {
        code: "return {'output': text}"
    }
    clean.text = self.raw
    self.result = clean.output
}
		`);
		// parseWeft keeps groups as Group nodes (passthrough expansion happens
		// later in resolveAndValidateTypes). Check the group and its children.
		const group = p.project.nodes.find(n => n.id === 'preprocessor');
		expect(group).toBeDefined();
		expect(group!.nodeType).toBe('Group');
		expect(group!.inputs.find(i => i.name === 'raw')).toBeDefined();
		expect(group!.outputs.find(o => o.name === 'result')).toBeDefined();
		// Inner node should be prefixed
		const inner = p.project.nodes.find(n => n.id === 'preprocessor.clean');
		expect(inner).toBeDefined();
	});

	it('parses nested groups', () => {
		const p = parseOk(`
# Project: Nested
outer = Group(data: String) -> (result: String) {
    inner = Group(x: String) -> (y: String) {
        proc = ExecPython(input: String) -> (output: String) {
            code: "return {'output': input}"
        }
        proc.input = self.x
        self.y = proc.output
    }
    inner.x = self.data
    self.result = inner.y
}
		`);
		const outer = p.project.nodes.find(n => n.id === 'outer');
		expect(outer).toBeDefined();
		expect(outer!.nodeType).toBe('Group');
		const inner = p.project.nodes.find(n => n.id === 'outer.inner');
		expect(inner).toBeDefined();
		expect(inner!.nodeType).toBe('Group');
		const proc = p.project.nodes.find(n => n.id === 'outer.inner.proc');
		expect(proc).toBeDefined();
	});

	it('parses group with self connections', () => {
		const p = parseOk(`
# Project: Self
grp = Group(data: String) -> (result: String) {
    worker = ExecPython(input: String) -> (output: String) {
        code: "return {'output': input}"
    }
    worker.input = self.data
    self.result = worker.output
}
		`);
		// self.data connections are stored with __inner suffix before passthrough expansion
		const edgeIn = p.project.edges.find(e =>
			e.source === 'grp' && e.sourceHandle === 'raw__inner' ||
			e.target === 'grp.worker' && e.targetHandle === 'input'
		);
		expect(edgeIn).toBeDefined();
	});

	it('self is a reserved word', () => {
		const p = parse(`
# Project: Reserved
self = Debug(data: String) {}
		`);
		expect(p.errors.some(e => e.message.toLowerCase().includes('self') || e.message.toLowerCase().includes('reserved'))).toBe(true);
	});

	// ─── Config Values ──────────────────────────────────────────────────

	it('parses boolean config values', () => {
		const p = parseOk(`
# Project: Bool
node = ExecPython {
    enabled: true
    disabled: false
}
		`);
		const config = p.project.nodes[0].config as Record<string, unknown>;
		expect(config.enabled).toBe(true);
		expect(config.disabled).toBe(false);
	});

	it('rejects mock and mocked config keys', () => {
		const p = parse(`
# Project: Mock
node = ExecPython {
    mock: {"body": "hello"}
    mocked: true
}
		`);
		expect(p.errors.some(e => e.message.includes("'mock' is not a valid config key"))).toBe(true);
		expect(p.errors.some(e => e.message.includes("'mocked' is not a valid config key"))).toBe(true);
	});

	it('parses numeric config values', () => {
		const p = parseOk(`
# Project: Numbers
node = ExecPython {
    count: 42
    rate: 0.75
}
		`);
		const config = p.project.nodes[0].config as Record<string, unknown>;
		expect(config.count).toBe(42);
		expect(config.rate).toBe(0.75);
	});

	it('parses quoted string config values', () => {
		const p = parseOk(`
# Project: Strings
node = ExecPython {
    prompt: "hello world"
}
		`);
		const config = p.project.nodes[0].config as Record<string, unknown>;
		expect(config.prompt).toBe('hello world');
	});

	it('parses JSON array in config', () => {
		const p = parseOk(`
# Project: JsonArr
node = ExecPython {
    items: ["a", "b", "c"]
}
		`);
		const config = p.project.nodes[0].config as Record<string, unknown>;
		expect(Array.isArray(config.items)).toBe(true);
		expect((config.items as string[]).length).toBe(3);
	});

	it('parses multiline JSON array in config', () => {
		const p = parseOk(`
# Project: MultiJson
node = HumanQuery {
    label: "Test"
    fields: [{
        "fieldType": "display",
        "key": "name"
    }, {
        "fieldType": "text_input",
        "key": "notes"
    }]
}
		`);
		const config = p.project.nodes[0].config as Record<string, unknown>;
		expect(Array.isArray(config.fields)).toBe(true);
		expect((config.fields as unknown[]).length).toBe(2);
	});

	// ─── Triple Backtick Multiline ──────────────────────────────────────

	it('parses triple backtick multiline', () => {
		const p = parseOk(`
# Project: Backtick
node = ExecPython {
    code: ${'```'}
print("line1")
print("line2")
    ${'```'}
}
		`);
		const config = p.project.nodes[0].config as Record<string, unknown>;
		const code = config.code as string;
		expect(code).toContain('print("line1")');
		expect(code).toContain('print("line2")');
	});

	it('parses inline triple backtick', () => {
		const p = parseOk(`
# Project: InlineBT
node = ExecPython {
    code: ${'```'}print("hello")${'```'}
}
		`);
		const config = p.project.nodes[0].config as Record<string, unknown>;
		expect(config.code).toBe('print("hello")');
	});

	// ─── Labels ─────────────────────────────────────────────────────────

	it('parses label from config', () => {
		const p = parseOk(`
# Project: Label
node = ExecPython {
    label: "My Worker"
    code: "return {}"
}
		`);
		expect(p.project.nodes[0].label).toBe('My Worker');
	});

	it('parses label in one-liner', () => {
		const p = parseOk(`
# Project: LabelOneLiner
node = ExecPython { label: "Quick", code: "return {}" }
		`);
		expect(p.project.nodes[0].label).toBe('Quick');
	});

	// ─── @require_one_of ────────────────────────────────────────────────

	it('parses @require_one_of', () => {
		const p = parseOk(`
# Project: OOR
node = ExecPython(
    text: String?,
    audio: Audio?,
    @require_one_of(text, audio)
) -> (result: String) {
    code: "return {}"
}
		`);
		const node = p.project.nodes[0];
		expect(node.features?.oneOfRequired?.length).toBe(1);
		expect(node.features?.oneOfRequired?.[0]).toEqual(['text', 'audio']);
	});

	// ─── Post-Config Output Ports ───────────────────────────────────────

	it('parses post-config output ports', () => {
		const p = parseOk(`
# Project: PostConfig
input = Text { value: "test" }
cfg = LlmConfig { model: "anthropic/claude-sonnet-4.6" }
node = LlmInference {
    parseJson: true
} -> (
    summary: String,
    score: Number?
)
node.prompt = input.value
node.config = cfg.config
		`);
		const node = p.project.nodes.find(n => n.id === 'node')!;
		expect(node.outputs.find(o => o.name === 'summary')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'score')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'score')!.required).toBe(false);
	});

	it('parses post-config outputs on same line as }', () => {
		const p = parseOk(`
# Project: PostConfigSameLine
input = Text { value: "test" }
cfg = LlmConfig { model: "anthropic/claude-sonnet-4.6" }
node = LlmInference {
    parseJson: true
} -> (summary: String)
node.prompt = input.value
node.config = cfg.config
		`);
		const node = p.project.nodes.find(n => n.id === 'node')!;
		expect(node.outputs.find(o => o.name === 'summary')).toBeDefined();
	});

	it('combines pre and post config outputs', () => {
		const p = parseOk(`
# Project: PreAndPost
node = ExecPython(data: String) -> (result: String) {
    code: "return {}"
} -> (extra: Number)
		`);
		const node = p.project.nodes[0];
		expect(node.outputs.some(o => o.name === 'result')).toBe(true);
		expect(node.outputs.some(o => o.name === 'extra')).toBe(true);
	});

	it('parses post-config outputs on one-liner config', () => {
		const p = parseOk(`
# Project: PostConfigOneLiner
input = Text { value: "test" }
cfg = LlmConfig { model: "anthropic/claude-sonnet-4.6" }
draft = LlmInference -> (response: JsonDict) { label: "Draft", parseJson: true } -> (subject: String, body: String)
draft.prompt = input.value
draft.config = cfg.config
		`);
		const node = p.project.nodes.find(n => n.id === 'draft')!;
		expect(node).toBeDefined();
		expect(node.outputs.find(o => o.name === 'response')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'subject')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'body')).toBeDefined();
	});

	it('parses post-config outputs on one-liner with multiple pre-config ports', () => {
		const p = parseOk(`
# Project: PostConfigOneLinerMulti
input = Text { value: "test" }
cfg = LlmConfig { model: "anthropic/claude-sonnet-4.6" }
qualify = LlmInference -> (response: JsonDict) { label: "Qualify", parseJson: true } -> (is_promising: Boolean, reason: String, summary: String)
qualify.prompt = input.value
qualify.config = cfg.config
		`);
		const node = p.project.nodes.find(n => n.id === 'qualify')!;
		expect(node).toBeDefined();
		expect(node.outputs.find(o => o.name === 'response')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'is_promising')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'reason')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'summary')).toBeDefined();
	});

	it('parses post-config outputs on multi-line config with } -> on last line', () => {
		const p = parseOk(`
# Project: PostConfigMultiLine
input = Text { value: "test" }
cfg = LlmConfig { model: "anthropic/claude-sonnet-4.6" }
qualify = LlmInference -> (response: JsonDict) {
  label: "Qualify"
  parseJson: true
} -> (is_promising: Boolean, reason: String, summary: String)
qualify.prompt = input.value
qualify.config = cfg.config
		`);
		const node = p.project.nodes.find(n => n.id === 'qualify')!;
		expect(node).toBeDefined();
		expect(node.outputs.find(o => o.name === 'response')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'is_promising')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'reason')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'summary')).toBeDefined();
	});

	// ─── Error Cases ────────────────────────────────────────────────────

	it('reports error on unknown type', () => {
		const p = parse(`
# Project: BadType
node = FooBarBaz {}
		`);
		expect(p.errors.some(e => e.message.includes('Unknown node type'))).toBe(true);
	});

	it('reports error on duplicate node ID', () => {
		const p = parse(`
# Project: Dup
node = Text { value: "a" }
node = Text { value: "b" }
		`);
		expect(p.errors.some(e => e.message.toLowerCase().includes('duplicate'))).toBe(true);
	});

	// ─── Empty / Minimal ────────────────────────────────────────────────

	it('handles empty project', () => {
		const p = parseOk(`
# Project: Empty
		`);
		expect(p.project.nodes.length).toBe(0);
		expect(p.project.edges.length).toBe(0);
	});

	it('handles comments only', () => {
		const p = parseOk(`
# Project: CommentsOnly
# Just comments
# More comments
		`);
		expect(p.project.nodes.length).toBe(0);
	});

	// ─── Arrow on Next Line ─────────────────────────────────────────────

	it('parses arrow on next line', () => {
		const p = parseOk(`
# Project: ArrowNext
node = ExecPython(data: String)
-> (result: String) {
    code: "return {}"
}
		`);
		const node = p.project.nodes[0];
		expect(node.inputs.find(i => i.name === 'data')).toBeDefined();
		expect(node.outputs.find(o => o.name === 'result')).toBeDefined();
	});

	// ─── Full Workflow ──────────────────────────────────────────────────

	it('parses a full small workflow', () => {
		const p = parseOk(`
# Project: Full Workflow
# Description: End to end

input = Text { value: "Hello" }

processor = Group(raw: String) -> (clean: String) {
    trimmer = ExecPython(text: String) -> (result: String) {
        code: "return {'result': text.strip()}"
    }
    trimmer.text = self.raw
    self.clean = trimmer.result
}

processor.raw = input.value

cfg = LlmConfig { model: "anthropic/claude-sonnet-4.6" }
llm = LlmInference() -> (response: String) {}
llm.prompt = processor.clean
llm.config = cfg.config

output = Debug(data: String) {}
output.data = llm.response
		`);
		expect(p.project.name).toBe('Full Workflow');
		// Frontend keeps groups as Group nodes (passthrough expansion happens later).
		// input + processor (Group) + processor.trimmer + cfg + llm + output = 6
		expect(p.project.nodes.length).toBe(6);
		// Connections: input→processor, processor.trimmer internal (self edges), processor→llm, cfg→llm, llm→output
		expect(p.project.edges.length).toBeGreaterThanOrEqual(5);
	});

	// ─── Multiline Port Signatures ──────────────────────────────────────

	it('parses deeply split port signature', () => {
		const p = parseOk(`
# Project: DeeplySplit
node = ExecPython(
    a: String,
    b: Number,
    c: List[String]
) -> (
    x: String,
    y: Number
) {
    code: "return {}"
}
		`);
		const node = p.project.nodes[0];
		expect(node.inputs.find(i => i.name === 'c')!.portType).toBe('List[String]');
		expect(node.outputs.find(o => o.name === 'y')!.portType).toBe('Number');
	});

	// ─── Block Extraction ───────────────────────────────────────────────

	it('extracts weft from 4-backtick fence', () => {
		const result = parseWeft('````weft\n# Project: Test\nnode = Debug\n````');
		expect(result.projects.length).toBe(1);
		expect(result.projects[0].project.nodes.length).toBe(1);
	});

	it('rejects 3-backtick fence (only 4-backtick fences are valid)', () => {
		const result = parseWeft('```weft\n# Project: Test\nnode = Debug\n```');
		expect(result.projects.length).toBe(0);
	});

	it('returns error when no weft block found', () => {
		const result = parseWeft('just some text');
		expect(result.projects.length).toBe(0);
		expect(result.errors.length).toBeGreaterThan(0);
	});

	// ─── Same Node ID in Different Groups ───────────────────────────────

	it('allows same node ID in different groups', () => {
		const p = parseOk(`
# Project: SameId
a = Group(data: String) -> (result: String) {
    proc = ExecPython(input: String) -> (output: String) {
        code: "return {'output': input}"
    }
    proc.input = self.data
    self.result = proc.output
}
b = Group(data: String) -> (result: String) {
    proc = ExecPython(input: String) -> (output: String) {
        code: "return {'output': input}"
    }
    proc.input = self.data
    self.result = proc.output
}
		`);
		expect(p.project.nodes.find(n => n.id === 'a.proc')).toBeDefined();
		expect(p.project.nodes.find(n => n.id === 'b.proc')).toBeDefined();
	});

	// ─── Scope ─────────────────────────────────────────────────────────

	it('sets empty scope on top-level nodes', () => {
		const p = parseOk(`
# Project: Scope
node = Text { value: "hello" }
		`);
		const node = p.project.nodes.find(n => n.id === 'node');
		expect(node?.scope).toEqual([]);
	});

	it('sets scope on nodes inside a group', () => {
		const p = parseOk(`
# Project: Scope
grp = Group(data: String) -> (result: String) {
    worker = ExecPython(value: String) -> (output: String) { code: "return {}" }
    worker.value = self.data
    self.result = worker.output
}
		`);
		const worker = p.project.nodes.find(n => n.id === 'grp.worker');
		expect(worker?.scope).toEqual(['grp']);

		const grp = p.project.nodes.find(n => n.id === 'grp');
		expect(grp?.scope).toEqual([]);
	});

	it('sets nested scope chain on deeply nested nodes', () => {
		const p = parseOk(`
# Project: Nested
outer = Group(x: String) -> (y: String) {
    inner = Group(x: String) -> (y: String) {
        deep = ExecPython(value: String) -> (output: String) { code: "return {}" }
        deep.value = self.x
        self.y = deep.output
    }
    inner.x = self.x
    self.y = inner.y
}
		`);
		const deep = p.project.nodes.find(n => n.id === 'outer.inner.deep');
		expect(deep?.scope).toEqual(['outer', 'outer.inner']);

		const inner = p.project.nodes.find(n => n.id === 'outer.inner');
		expect(inner?.scope).toEqual(['outer']);
	});
});

describe('inline line numbers', () => {
	it('reports duplicate-id line after inline block at correct ORIGINAL line', () => {
		// Lines:
		// 1: # Project: Test
		// 2: (blank)
		// 3: writer = LlmInference -> (response: String) { label: "W" }
		// 4: writer.prompt = Template {
		// 5:   template: "hi {{x}}"
		// 6:   x: source.value
		// 7: }.text
		// 8: (blank)
		// 9: source = Text { value: "a" }
		// 10: source = Text { value: "dup" }
		const code = `# Project: Test

writer = LlmInference -> (response: String) { label: "W" }
writer.prompt = Template {
  template: "hi {{x}}"
  x: source.value
}.text

source = Text { value: "a" }
source = Text { value: "dup" }`;
		const p = parse(code);
		const dup = p.errors.find(e => e.message.includes('Duplicate'));
		expect(dup).toBeDefined();
		expect(dup!.line).toBe(10);
	});

	it('reports unknown-node error inside group with inline at correct line', () => {
		// Inline inside a group body.
		// Lines 1-3 header, group starts line 5, inline ~line 10
		const code = `# Project: Test

grp = Group(x: String) -> (y: String) {
  # inner

  src = Text { value: "a" }
  writer = LlmInference -> (response: String) { label: "W" }
  writer.prompt = Template {
    template: "{{name}}"
    name: src.value
  }.text
  self.y = writer.response
}

grp.x = something_unknown.value`;
		const p = parse(code);
		// something_unknown doesn't exist, expect error on line 15
		const err = p.errors.find(e => e.message.includes('something_unknown'));
		if (err) {
			expect(err.line).toBe(15);
		}
	});
});

describe('inline native parser', () => {
	it('parses a simple inline on connection RHS with the new anon id scheme', () => {
		const code = `# Project: Test

source = Text { value: "world" }

writer = LlmInference -> (response: String) { label: "Writer" }
writer.prompt = Template {
  template: "Hello {{name}}"
  name: source.value
}.text`;
		const p = parse(code);
		// Anon id for writer.prompt = Template {...}.text should be writer__prompt.
		const anon = p.project.nodes.find(n => n.id === 'writer__prompt');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('Template');
	});

	it('parses inline in a config field (non-nested) with meaningful anon id', () => {
		// systemPrompt is an input port on LlmConfig, so the inline lands as an edge.
		const code = `# Project: Test

other = Text { value: "world" }

llm_config = LlmConfig {
  apiKey: ""
  systemPrompt: Template {
    template: "Hello {{name}}"
    name: other.value
  }.text
}`;
		const p = parse(code);
		// Anon id should be llm_config__systemPrompt.
		const anon = p.project.nodes.find(n => n.id === 'llm_config__systemPrompt');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('Template');
	});

	it('parses nested inline in config field (Template inside Template)', () => {
		const code = `# Project: Test

other = Text { value: "world" }

llm_config = LlmConfig {
  apiKey: ""
  systemPrompt: Template {
    template: "outer: {{inner}}"
    inner: Template {
      template: "deep: {{x}}"
      x: other.value
    }.text
  }.text
}`;
		const p = parse(code);
		const outer = p.project.nodes.find(n => n.id === 'llm_config__systemPrompt');
		const inner = p.project.nodes.find(n => n.id === 'llm_config__systemPrompt__inner');
		expect(outer).toBeDefined();
		expect(inner).toBeDefined();
		expect(outer!.nodeType).toBe('Template');
		expect(inner!.nodeType).toBe('Template');
	});

	it('rejects post-config outputs on inline expressions', () => {
		const code = `# Project: Test

writer = LlmInference -> (response: String) { label: "W" }
writer.prompt = Template { template: "hi" } -> (out: String).out`;
		const p = parse(code);
		const postConfigErr = p.errors.find(e => e.message.includes('post-config outputs'));
		expect(postConfigErr).toBeDefined();
	});
});

describe('inline parser: realistic AI-like code', () => {
	it('reports lines correctly for multiple sequential inlines inside a group', () => {
		// Mimic the user's Cold Outreach shape: a group with several inline
		// Template usages followed by a connection referencing an unknown node.
		const code = `# Project: Test

per_lead = Group(firstName: String, company: String, email: String) -> () {
  person_search = TavilySearch { searchDepth: "basic" }
  person_search.query = Template {
    template: "{{name}}"
    name: self.firstName
  }.text

  company_search = TavilySearch { searchDepth: "basic" }
  company_search.query = Template {
    template: "{{c}}"
    c: self.company
  }.text

  send = EmailSend { from: "me@x.com" }
  send.to = self.email
  send.config = email_config.config
}`;
		// email_config is out of scope (used inside group without being passed in)
		// so we expect: "Connection references unknown source node: per_lead.email_config"
		// or a scope error. The line for `send.config = email_config.config` is line 18 (1-indexed).
		const p = parse(code);
		const err = p.errors.find(e => e.message.includes('email_config'));
		expect(err).toBeDefined();
		expect(err!.line).toBe(18);
	});
});

describe('parser symmetry', () => {
	it('accepts multi-line port lists without trailing commas (backend parity)', () => {
		const code = `# Project: Test

n = ExecPython(
  a: String
  b: Number
) -> (
  out: String
) {
  code: \`\`\`
return {"out": a}
\`\`\`
}`;
		const p = parse(code);
		const node = p.project.nodes.find(n => n.id === 'n');
		expect(node).toBeDefined();
		const inputNames = node!.inputs.map(p => p.name);
		expect(inputNames).toContain('a');
		expect(inputNames).toContain('b');
		const outputNames = node!.outputs.map(p => p.name);
		expect(outputNames).toContain('out');
	});

	it('accepts triple backtick with no space after colon', () => {
		const code = `# Project: Test

n = ExecPython -> (result: String) {
  code:\`\`\`
return {"result": "x"}
\`\`\`
}`;
		const p = parse(code);
		const node = p.project.nodes.find(n => n.id === 'n');
		expect(node).toBeDefined();
		const code_val = node!.config?.code;
		expect(typeof code_val).toBe('string');
		expect(code_val).toContain('return');
	});
});

	it('rejects duplicate group names at same scope (backend parity)', () => {
		const code = `# Project: Test

g = Group() -> (x: String?) {
  n = Text { value: "a" }
  self.x = n.value
}

g = Group() -> (x: String?) {
  n = Text { value: "b" }
  self.x = n.value
}`;
		const p = parse(code);
		const dupErr = p.errors.find(e => e.message.toLowerCase().includes('duplicate group'));
		expect(dupErr).toBeDefined();
	});

	it('parses multiline JSON object with trailing key in config block (backend parity)', () => {
		const code = `# Project: Test

n = Text -> (value: String) {
  extra: {
    "a": 1,
    "b": 2
  }
  value: "hi"
}`;
		const p = parse(code);
		const node = p.project.nodes.find(n => n.id === 'n');
		expect(node).toBeDefined();
		expect(node!.config?.value).toBe('hi');
		const extra = node!.config?.extra;
		expect(typeof extra).toBe('object');
		expect((extra as Record<string, unknown>)?.a).toBe(1);
		expect((extra as Record<string, unknown>)?.b).toBe(2);
	});

	it('parses post-config output ports on same line (backend parity)', () => {
		const code = `# Project: Test

src = Text { value: "x" }
n = ExecPython(x: String) {
  code: \`\`\`
return {"a": x, "b": x}
\`\`\`
} -> (a: String, b: String)

n.x = src.value`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n');
		expect(n).toBeDefined();
		const outNames = n!.outputs.map(pp => pp.name);
		expect(outNames).toContain('a');
		expect(outNames).toContain('b');
	});

	it('parses post-config output ports on next line (backend parity)', () => {
		const code = `# Project: Test

src = Text { value: "x" }
n = ExecPython(x: String) {
  code: \`\`\`
return {"a": x}
\`\`\`
}
-> (a: String)

n.x = src.value`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n');
		expect(n).toBeDefined();
		const outNames = n!.outputs.map(pp => pp.name);
		expect(outNames).toContain('a');
	});

	it('parses one-liner declaration with port signature (backend parity)', () => {
		const code = `# Project: Test

src = Text { value: "hi" }
n = ExecPython(x: String) -> (y: String) { code: "return {'y': x}" }

n.x = src.value`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n');
		expect(n).toBeDefined();
		expect(n!.inputs.map(pp => pp.name)).toContain('x');
		expect(n!.outputs.map(pp => pp.name)).toContain('y');
		expect(n!.config?.code).toBe("return {'y': x}");
	});

	it('parses all config value types identically to backend', () => {
		const code = `# Project: Test

n = Text {
  bool_t: true
  bool_f: false
  int_val: 42
  neg_int: -17
  float_val: 3.14
  neg_float: -2.5
  str_quoted: "hello"
  str_escaped: "hello \\"world\\""
  arr_json: [1, 2, 3]
  obj_json: {"a": 1, "b": "two"}
}`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n');
		expect(n).toBeDefined();
		const c = n!.config as Record<string, unknown>;
		expect(c.bool_t).toBe(true);
		expect(c.bool_f).toBe(false);
		expect(c.int_val).toBe(42);
		expect(c.neg_int).toBe(-17);
		expect(Math.abs((c.float_val as number) - 3.14)).toBeLessThan(0.001);
		expect(Math.abs((c.neg_float as number) + 2.5)).toBeLessThan(0.001);
		expect(c.str_quoted).toBe('hello');
		expect(c.str_escaped).toBe('hello "world"');
		expect(Array.isArray(c.arr_json)).toBe(true);
		expect(typeof c.obj_json).toBe('object');
	});

	it('parses @require_one_of in all three positions (backend parity)', () => {
		const code = `# Project: Test

src1 = Text { value: "a" }

n1 = ExecPython(
  a: String?
  b: String?
  @require_one_of(a, b)
) -> (r: String) {
  code: "return {'r': a or b}"
}
n1.a = src1.value

n2 = ExecPython(x: String?, y: String?) -> (r: String) {
  code: "return {'r': x or y}"
  @require_one_of(x, y)
}
n2.x = src1.value

g = Group(a: String?, b: String?, @require_one_of(a, b)) -> (r: String?) {
  pick = ExecPython(a: String?, b: String?) -> (r: String) {
    code: "return {'r': a or b}"
  }
  pick.a = self.a
  pick.b = self.b
  self.r = pick.r
}
g.a = src1.value`;
		const p = parse(code);
		const n1 = p.project.nodes.find(n => n.id === 'n1');
		const n2 = p.project.nodes.find(n => n.id === 'n2');
		const g = p.project.nodes.find(n => n.id === 'g');
		expect(n1?.features?.oneOfRequired).toEqual([['a', 'b']]);
		expect(n2?.features?.oneOfRequired).toEqual([['x', 'y']]);
		expect(g?.features?.oneOfRequired).toEqual([['a', 'b']]);
	});

	it('parses multi-line label with escaped quotes (backend parity)', () => {
		const code = `# Project: Test

n = Text {
  label: "Say \\"hello\\" there"
  value: "x"
}`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n');
		expect(n?.label).toBe('Say "hello" there');
	});

	it('parses bare Type.port inline in config block (no braces)', () => {
		// Bare form: no `{}` on the inline Type. Creates an anon with
		// default config and wires its .port output to the host key.
		const code = `# Project: Test

host = LlmInference {
  model: "gpt-4"
  prompt: Text.value
}`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'host__prompt');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('Text');
	});

	it('parses bare Type.port on RHS of connection', () => {
		const code = `# Project: Test

host = LlmInference { model: "gpt-4" }
host.prompt = Text.value`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'host__prompt');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('Text');
	});

	it('one-liner config detects inline expression and creates anon', () => {
		// Bare inline form in a one-liner.
		const code = `# Project: Test

n = LlmInference { model: "gpt-4", prompt: Text.value }`;
		const p = parse(code);
		const anon = p.project.nodes.find(nn => nn.id === 'n__prompt');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('Text');
		const edge = p.project.edges.find(e => e.source === 'n__prompt' && e.target === 'n');
		expect(edge).toBeDefined();
		expect(edge!.targetHandle).toBe('prompt');
	});

	it('one-liner config with config-block inline (nested braces)', () => {
		// Inline with config-block body, all on one line. The brace-aware
		// splitter keeps Template { ... }.text as one pair.
		const code = `# Project: Test

n = LlmInference { model: "gpt-4", prompt: Template { template: "hi" }.text }`;
		const p = parse(code);
		const anon = p.project.nodes.find(nn => nn.id === 'n__prompt');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('Template');
		expect((anon!.config as Record<string, unknown>)?.template).toBe('hi');
	});

	it('one-liner inline on non-port key is rejected by frontend validation', () => {
		// `label` is metadata, not a port. The parser creates the anon + edge
		// (brace-aware splitter), then frontend validation should reject the
		// edge because `label` doesn't exist as an input port on the host.
		const code = `# Project: Test

n = Text { label: Template { template: "hi" }.text, value: "v" }`;
		const p = parse(code);
		// Parser should have created the anon node.
		const anon = p.project.nodes.find(nn => nn.id === 'n__label');
		expect(anon).toBeDefined();
		// Frontend validation should emit an error about the label edge
		// targeting a non-existent port on n.
		const labelErr = p.errors.find(e => e.message.toLowerCase().includes('label'));
		expect(labelErr).toBeDefined();
	});

	it('port wiring via dotted ref in regular multi-line node body', () => {
		const code = `# Project: T

src = Text { value: "hi {{x}}" }
greeting = Template {
  template: src.value
}`;
		const p = parse(code);
		const greeting = p.project.nodes.find(n => n.id === 'greeting');
		expect(greeting).toBeDefined();
		expect((greeting!.config as Record<string, unknown>)?.template).toBeUndefined();
		const edge = p.project.edges.find(e =>
			e.source === 'src' && e.sourceHandle === 'value'
			&& e.target === 'greeting' && e.targetHandle === 'template'
		);
		expect(edge).toBeDefined();
	});

	it('port wiring via dotted ref in one-liner node body', () => {
		const code = `# Project: T

src = Text { value: "hi {{x}}" }
greeting = Template { template: src.value }`;
		const p = parse(code);
		const greeting = p.project.nodes.find(n => n.id === 'greeting');
		expect(greeting).toBeDefined();
		expect((greeting!.config as Record<string, unknown>)?.template).toBeUndefined();
		const edge = p.project.edges.find(e =>
			e.source === 'src' && e.sourceHandle === 'value'
			&& e.target === 'greeting' && e.targetHandle === 'template'
		);
		expect(edge).toBeDefined();
	});

	it('port wiring self.x in group child (multi-line)', () => {
		const code = `# Project: T

src = Text { value: "hi {{x}}" }
grp = Group(text: String) -> (out: String?) {
  greeting = Template {
    template: self.text
  }
  self.out = greeting.text
}
grp.text = src.value
out = Debug { label: "out" }
out.data = grp.out`;
		const p = parse(code);
		const greeting = p.project.nodes.find(n => n.id === 'grp.greeting');
		expect(greeting).toBeDefined();
		expect((greeting!.config as Record<string, unknown>)?.template).toBeUndefined();
	});

	it('port wiring on non-port key rejected (Text.value is not an input port)', () => {
		const code = `# Project: T

src = Text { value: "hi" }
grp = Group(text: String) -> (out: String?) {
  test = Text {
    value: self.text
  }
  self.out = test.value
}
grp.text = src.value`;
		const p = parse(code);
		const err = p.errors.find(e => e.message.toLowerCase().includes('value'));
		expect(err).toBeDefined();
	});

	it('required port filled from config is accepted by frontend validation', () => {
		const code = `# Project: T

greeting = Template {
  template: "Hello {{name}}"
}
out = Debug { label: "out" }
out.data = greeting.text`;
		const p = parse(code);
		const requiredErr = p.errors.find(e => e.message.includes('template') && e.message.includes('required'));
		expect(requiredErr).toBeUndefined();
	});

	it('required port not filled is rejected by frontend validation', () => {
		const code = `# Project: T

greeting = Template { label: "bad" }
out = Debug { label: "out" }
out.data = greeting.text`;
		const p = parse(code);
		const requiredErr = p.errors.find(e =>
			e.message.includes('template') && e.message.includes('required')
		);
		expect(requiredErr).toBeDefined();
	});

	it('connection-line literal fills target node config (string)', () => {
		const code = `# Project: T

greeting = Template { label: "g" }
greeting.template = "Hello {{name}}"`;
		const p = parse(code);
		const g = p.project.nodes.find(n => n.id === 'greeting');
		expect(g).toBeDefined();
		expect((g!.config as Record<string, unknown>)?.template).toBe('Hello {{name}}');
		const edge = p.project.edges.find(e => e.target === 'greeting' && e.targetHandle === 'template');
		expect(edge).toBeUndefined();
	});

	it('connection-line literal supports number/bool/JSON', () => {
		const code = `# Project: T

n = LlmInference { model: "gpt-4", prompt: "hi" }
n.temperature = 0.8
n.parseJson = true`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n');
		expect(n).toBeDefined();
		expect((n!.config as Record<string, unknown>)?.temperature).toBe(0.8);
		expect((n!.config as Record<string, unknown>)?.parseJson).toBe(true);
	});

	it('connection-line literal: last write wins', () => {
		const code = `# Project: T

n = Template { template: "first" }
n.template = "second"`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n');
		expect((n!.config as Record<string, unknown>)?.template).toBe('second');
	});

	it('connection-line literal on non-configurable port rejected', () => {
		// LlmInference's `config` input is marked configurable: false.
		const code = `# Project: T

llm = LlmInference { model: "gpt-4", prompt: "hi" }
llm.config = "not a config"`;
		const p = parse(code);
		const err = p.errors.find(e => e.message.toLowerCase().includes('wired-only') || e.message.toLowerCase().includes('cannot be set from config'));
		expect(err).toBeDefined();
	});

	it('connection-line literal triple-backtick multi-line', () => {
		const code = `# Project: T

n = ExecPython(x: String) -> (out: String) { label: "run" }
n.code = \`\`\`
return {"out": x}
\`\`\`
n.x = "hi"`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n');
		expect(n).toBeDefined();
		const code_val = (n!.config as Record<string, unknown>)?.code as string;
		expect(typeof code_val).toBe('string');
		expect(code_val).toContain('return');
		expect(code_val).not.toContain('\`\`\`');
		expect((n!.config as Record<string, unknown>)?.x).toBe('hi');
	});

	it('connection-line literal multi-line JSON object', () => {
		const code = `# Project: T

n = Template { template: "hi" }
n.data = {
  "a": 1,
  "b": "two"
}`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n');
		expect(n).toBeDefined();
		const data = (n!.config as Record<string, unknown>)?.data;
		expect(typeof data).toBe('object');
		expect((data as Record<string, unknown>)?.a).toBe(1);
		expect((data as Record<string, unknown>)?.b).toBe('two');
	});

describe('combo: inline expression shape coverage', () => {
	it('full-form inline with ports + config + wiring on connection RHS', () => {
		const code = `# Project: T

src = Text { value: "hello" }
host = Debug { label: "h" }
host.data = ExecPython (
  test: String
) -> (
  out: String
) {
  test: src.value
  code: \`\`\`
return {"out": test}
\`\`\`
}.out`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'host__data');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('ExecPython');
		expect(anon!.inputs.some(pp => pp.name === 'test')).toBe(true);
		expect(anon!.outputs.some(pp => pp.name === 'out')).toBe(true);
		expect((anon!.config as Record<string, unknown>)?.code).toBeDefined();
		const wiringIn = p.project.edges.find(e => e.source === 'src' && e.target === 'host__data' && e.targetHandle === 'test');
		expect(wiringIn).toBeDefined();
		const wiringOut = p.project.edges.find(e => e.source === 'host__data' && e.target === 'host' && e.targetHandle === 'data');
		expect(wiringOut).toBeDefined();
	});

	it('nested inline on connection RHS synthesizes undeclared port as required', () => {
		// An edge targeting an undeclared key on a canAddInputPorts node
		// (Template) synthesizes the port with required: true and a
		// TypeVar that narrows to the source's type.
		const code = `# Project: T

src = Text { value: "world" }
host = Debug { label: "h" }
host.data = Template {
  template: Template {
    template: "Hello {{x}}"
    x: src.value
  }.text
}.text`;
		const p = parse(code);
		expect(p.errors).toEqual([]);
		const innerAnon = p.project.nodes.find(n => n.id === 'host__data__template');
		expect(innerAnon).toBeDefined();
		const xPort = innerAnon!.inputs.find(pp => pp.name === 'x');
		expect(xPort).toBeDefined();
		expect(xPort!.required).toBe(true);
		expect(xPort!.portType).toBe('String');
	});

	it('nested inline on connection RHS with explicit port declaration', () => {
		// Correct form: declare `x` in the inner Template's port signature.
		const code = `# Project: T

src = Text { value: "world" }
host = Debug { label: "h" }
host.data = Template {
  template: Template(x: String) {
    template: "Hello {{x}}"
    x: src.value
  }.text
}.text`;
		const p = parse(code);
		expect(p.project.nodes.some(n => n.id === 'host__data')).toBe(true);
		expect(p.project.nodes.some(n => n.id === 'host__data__template')).toBe(true);
		expect(p.project.edges.some(e => e.source === 'src' && e.target === 'host__data__template')).toBe(true);
	});

	it('bare inline in group body', () => {
		const code = `# Project: T

outer = Group() -> (out: String?) {
  sink = Debug { label: "s" }
  sink.data = Text.value
  self.out = sink.data
}`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'outer.sink__data');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('Text');
	});

	it('post-config output on inline RHS is rejected', () => {
		const code = `# Project: T

host = Debug { label: "h" }
host.data = ExecPython {
  code: \`\`\`
return {"out": "x"}
\`\`\`
} -> (out: String).out`;
		const p = parse(code);
		const err = p.errors.find(e => e.message.toLowerCase().includes('post-config'));
		expect(err).toBeDefined();
	});

	it('self-wiring inside inline body on connection RHS', () => {
		const code = `# Project: T

src = Text { value: "hi" }
grp = Group(thing: String) -> (out: String?) {
  dst = Debug { label: "d" }
  dst.data = Template {
    template: "{{x}}"
    x: self.thing
  }.text
  self.out = dst.data
}
grp.thing = src.value`;
		const p = parse(code);
		expect(p.project.nodes.some(n => n.id === 'grp.dst__data')).toBe(true);
	});

	it('mixed literal and wiring in inline body on RHS synthesizes edge port as required', () => {
		// `x: src.value` on a canAddInputPorts Template without a signature
		// declaration synthesizes `x` as a required port with type narrowed
		// from src.value.
		const code = `# Project: T

src = Text { value: "world" }
host = Debug { label: "h" }
host.data = Template {
  template: "Hello {{x}}"
  x: src.value
}.text`;
		const p = parse(code);
		expect(p.errors).toEqual([]);
		const anon = p.project.nodes.find(n => n.id === 'host__data');
		expect(anon).toBeDefined();
		const xPort = anon!.inputs.find(pp => pp.name === 'x');
		expect(xPort).toBeDefined();
		expect(xPort!.required).toBe(true);
		expect(xPort!.portType).toBe('String');
	});

	it('mixed literal and wiring in inline body on RHS with explicit port', () => {
		const code = `# Project: T

src = Text { value: "world" }
host = Debug { label: "h" }
host.data = Template(x: String) {
  template: "Hello {{x}}"
  x: src.value
}.text`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'host__data');
		expect(anon).toBeDefined();
		expect((anon!.config as Record<string, unknown>)?.template).toBe('Hello {{x}}');
		expect(p.project.edges.some(e => e.source === 'src' && e.target === 'host__data' && e.targetHandle === 'x')).toBe(true);
	});

	it('bare inline inside inline body on RHS', () => {
		const code = `# Project: T

host = Debug { label: "h" }
host.data = Template {
  template: "Hello {{x}}"
  x: Text.value
}.text`;
		const p = parse(code);
		expect(p.project.nodes.some(n => n.id === 'host__data')).toBe(true);
		expect(p.project.nodes.some(n => n.id === 'host__data__x')).toBe(true);
	});

	it('full-form inline inside config block', () => {
		const code = `# Project: T

src = Text { value: "hi" }
host = Debug {
  label: "host"
  data: ExecPython (
    test: String
  ) -> (
    out: String
  ) {
    test: src.value
    code: \`\`\`
return {"out": test}
\`\`\`
  }.out
}`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'host__data');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('ExecPython');
	});

	it('nested inline inside config block', () => {
		const code = `# Project: T

src = Text { value: "world" }
host = Debug {
  label: "host"
  data: Template {
    template: Template {
      template: "Hello {{x}}"
      x: src.value
    }.text
  }.text
}`;
		const p = parse(code);
		expect(p.project.nodes.some(n => n.id === 'host__data')).toBe(true);
		expect(p.project.nodes.some(n => n.id === 'host__data__template')).toBe(true);
	});

	it('bare inline inside config block', () => {
		const code = `# Project: T

host = Debug {
  label: "host"
  data: Text.value
}`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'host__data');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('Text');
	});

	it('triple-nested inline on RHS', () => {
		const code = `# Project: T

src = Text { value: "hi" }
host = Debug { label: "h" }
host.data = Template {
  template: Template {
    template: Template {
      template: "deep {{y}}"
      y: src.value
    }.text
  }.text
}.text`;
		const p = parse(code);
		expect(p.project.nodes.some(n => n.id === 'host__data')).toBe(true);
		expect(p.project.nodes.some(n => n.id === 'host__data__template')).toBe(true);
		expect(p.project.nodes.some(n => n.id === 'host__data__template__template')).toBe(true);
	});

	it('triple-backtick inside inline body on RHS', () => {
		const code = `# Project: T

host = Debug { label: "h" }
host.data = ExecPython(x: String) -> (out: String) {
  x: "hi"
  code: \`\`\`
return {"out": x}
\`\`\`
}.out`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'host__data');
		expect(anon).toBeDefined();
		const code_val = (anon!.config as Record<string, unknown>)?.code as string;
		expect(code_val).toContain('return');
		expect(code_val).not.toContain('\`\`\`');
	});

	it('multi-line JSON inside inline body on RHS', () => {
		const code = `# Project: T

host = Debug { label: "h" }
host.data = ExecPython -> (out: JsonDict) {
  params: {
    "a": 1,
    "b": "two"
  }
  code: \`\`\`
return {"out": params}
\`\`\`
}.out`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'host__data');
		expect(anon).toBeDefined();
		const params = (anon!.config as Record<string, unknown>)?.params;
		expect(typeof params).toBe('object');
		expect((params as Record<string, unknown>)?.a).toBe(1);
	});

	it('connection literal plus inline in body same node', () => {
		const code = `# Project: T

src = Text { value: "w" }
host = LlmInference {
  model: "gpt-4"
  prompt: Template {
    template: "Hello {{x}}"
    x: src.value
  }.text
}
host.systemPrompt = "You are a helpful assistant"`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'host__prompt');
		expect(anon).toBeDefined();
		const host = p.project.nodes.find(n => n.id === 'host');
		expect((host!.config as Record<string, unknown>)?.systemPrompt).toBe('You are a helpful assistant');
	});

	it('inline expression inside inline body on group self', () => {
		const code = `# Project: T

src = Text { value: "hi" }
grp = Group(thing: String) -> (out: String?) {
  self.out = Template {
    template: "{{x}}"
    x: self.thing
  }.text
}
grp.thing = src.value`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'grp.self__out');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('Template');
	});

	it('inline with only output port signature', () => {
		const code = `# Project: T

host = Debug { label: "h" }
host.data = ExecPython -> (out: String) {
  code: \`\`\`
return {"out": "hi"}
\`\`\`
}.out`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'host__data');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('ExecPython');
	});

	it('multi-line JSON connection literal inside group body', () => {
		const code = `# Project: T

grp = Group() -> (out: JsonDict?) {
  n = ExecPython -> (out: JsonDict) {
    code: \`\`\`
return {"out": "x"}
\`\`\`
  }
  n.params = {
    "a": 1,
    "b": [1, 2, 3]
  }
  self.out = n.out
}`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'grp.n');
		expect(n).toBeDefined();
		const params = (n!.config as Record<string, unknown>)?.params;
		expect(typeof params).toBe('object');
		expect((params as Record<string, unknown>)?.a).toBe(1);
	});

	it('self-wiring in inline body inside group child config block', () => {
		const code = `# Project: T

src = Text { value: "hi" }
grp = Group(thing: String) -> (out: String?) {
  dst = Debug {
    label: "d"
    data: Template {
      template: "{{x}}"
      x: self.thing
    }.text
  }
  self.out = dst.data
}
grp.thing = src.value`;
		const p = parse(code);
		const anon = p.project.nodes.find(n => n.id === 'grp.dst__data');
		expect(anon).toBeDefined();
		expect(anon!.nodeType).toBe('Template');
	});

	it('one-liner style with multi-line backtick body', () => {
		const code = `# Project: T

n = ExecPython -> (out: JsonDict) { code: \`\`\`
return {"out": "x"}
\`\`\` }`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n');
		expect(n).toBeDefined();
		const code_val = (n!.config as Record<string, unknown>)?.code as string;
		expect(code_val).toContain('return');
		expect(code_val).not.toContain('\`\`\`');
	});
});

	it('group inlining on connection RHS is rejected', () => {
		const code = `# Project: T

host = Debug { label: "h" }
host.data = Group() -> (out: String?) {
  n = Text { value: "hi" }
  self.out = n.value
}.out`;
		const p = parse(code);
		const err = p.errors.find(e => e.message.toLowerCase().includes('group'));
		expect(err).toBeDefined();
	});

	it('group inlining in config block is rejected', () => {
		const code = `# Project: T

host = Debug {
  label: "h"
  data: Group() -> (out: String?) {
    n = Text { value: "hi" }
    self.out = n.value
  }.out
}`;
		const p = parse(code);
		const err = p.errors.find(e => e.message.toLowerCase().includes('group'));
		expect(err).toBeDefined();
	});

	it('bare group inlining is rejected', () => {
		const code = `# Project: T

host = Debug { label: "h" }
host.data = Group.out`;
		const p = parse(code);
		// Either errors list has a group mention, or no anon host__data exists.
		const err = p.errors.find(e => e.message.toLowerCase().includes('group'));
		const anon = p.project.nodes.find(n => n.id === 'host__data');
		expect(err !== undefined || anon === undefined).toBe(true);
	});

	it('deeply nested RHS (4 levels)', () => {
		const code = `# Project: T

src = Text { value: "hi" }
host = Debug { label: "h" }
host.data = Template {
  template: Template {
    template: Template {
      template: Template {
        template: "deepest {{y}}"
        y: src.value
      }.text
    }.text
  }.text
}.text`;
		const p = parse(code);
		expect(p.project.nodes.some(n => n.id === 'host__data')).toBe(true);
		expect(p.project.nodes.some(n => n.id === 'host__data__template')).toBe(true);
		expect(p.project.nodes.some(n => n.id === 'host__data__template__template')).toBe(true);
		expect(p.project.nodes.some(n => n.id === 'host__data__template__template__template')).toBe(true);
	});

	it('deeply nested inside config with multiline JSON + triple-backtick at deepest level', () => {
		const code = `# Project: T

src = Text { value: "world" }
host = Debug {
  label: "host"
  data: ExecPython(a: String) -> (out: String) {
    a: Template {
      template: Template {
        template: "Hello {{x}}"
        x: src.value
      }.text
    }.text
    code: \`\`\`
return {"out": a}
\`\`\`
    params: {
      "key": "value",
      "list": [1, 2, 3]
    }
  }.out
}`;
		const p = parse(code);
		expect(p.project.nodes.some(n => n.id === 'host__data')).toBe(true);
		expect(p.project.nodes.some(n => n.id === 'host__data__a')).toBe(true);
		expect(p.project.nodes.some(n => n.id === 'host__data__a__template')).toBe(true);
		const outer = p.project.nodes.find(n => n.id === 'host__data')!;
		const cfg = outer.config as Record<string, unknown>;
		expect(cfg.code).toBeDefined();
		expect((cfg.code as string).includes('return')).toBe(true);
		expect(typeof cfg.params).toBe('object');
	});

	it('external ref inside inline body inside group with explicit port declaration', () => {
		// Port wiring from a root-scope node inside an inline body that's
		// inside a group body. The port must be explicitly declared in
		// the anon's signature; the edge source stays unprefixed (not
		// `grp.src`). Uniform rule, no carve-out.
		const code = `# Project: T

src = Text { value: "hi" }
grp = Group() -> (out: String?) {
  dst = Debug {
    label: "d"
    data: Template(x: String) {
      template: "{{x}}"
      x: src.value
    }.text
  }
  self.out = dst.data
}`;
		const p = parse(code);
		const wired = p.project.edges.find(e =>
			e.source === 'src' && e.sourceHandle === 'value'
			&& e.target === 'grp.dst__data' && e.targetHandle === 'x'
		);
		expect(wired).toBeDefined();
	});

	it('maximalist: 5-level nested RHS with every value type at every level', () => {
		const code = `# Project: T

src = Text { value: "world" }
grp = Group(thing: String) -> (out: String?) {
  root_host = Debug {
    label: "level0"
    data: ExecPython(a: String, extra: String) -> (out: String) {
      code: \`\`\`
L0 code
return {"out": a}
\`\`\`
      meta: {
        "level": 0,
        "tags": ["a", "b"]
      }
      note: "level-0 literal"
      extra: src.value
      a: ExecPython(b: String, extra: String) -> (out: String) {
        code: \`\`\`
L1 code
return {"out": b}
\`\`\`
        meta: {
          "level": 1,
          "nested": {"x": 1}
        }
        note: "level-1 literal"
        extra: src.value
        b: ExecPython(c: String, extra: String) -> (out: String) {
          code: \`\`\`
L2 code
return {"out": c}
\`\`\`
          meta: {
            "level": 2
          }
          note: "level-2 literal"
          extra: src.value
          c: ExecPython(d: String, extra: String) -> (out: String) {
            code: \`\`\`
L3 code
return {"out": d}
\`\`\`
            meta: {
              "level": 3
            }
            note: "level-3 literal"
            extra: src.value
            d: ExecPython(e: String) -> (out: String) {
              code: \`\`\`
L4 code
return {"out": e}
\`\`\`
              meta: {
                "level": 4
              }
              note: "level-4 literal"
              e: self.thing
            }.out
          }.out
        }.out
      }.out
    }.out
  }
  self.out = root_host.data
}
grp.thing = src.value`;
		const p = parse(code);

		const expectedIds = [
			'grp.root_host__data',
			'grp.root_host__data__a',
			'grp.root_host__data__a__b',
			'grp.root_host__data__a__b__c',
			'grp.root_host__data__a__b__c__d',
		];
		for (const id of expectedIds) {
			const node = p.project.nodes.find(n => n.id === id);
			expect(node, `expected anon ${id} missing`).toBeDefined();
			expect(node!.nodeType).toBe('ExecPython');
		}

		// note + code + meta at every level
		for (let level = 0; level < expectedIds.length; level++) {
			const node = p.project.nodes.find(n => n.id === expectedIds[level])!;
			const cfg = node.config as Record<string, unknown>;
			expect(cfg.note, `level ${level}: note`).toBe(`level-${level} literal`);
			const code = cfg.code as string;
			expect(code, `level ${level}: code defined`).toBeDefined();
			expect(code).toContain(`L${level} code`);
			expect(code).not.toContain('```');
			const meta = cfg.meta as Record<string, unknown>;
			expect(typeof meta).toBe('object');
			expect(meta.level).toBe(level);
		}

		// Levels 0..3 have `extra: src.value` port wiring
		for (let level = 0; level < 4; level++) {
			const id = expectedIds[level];
			const wired = p.project.edges.find(e =>
				e.source === 'src' && e.sourceHandle === 'value'
				&& e.target === id && e.targetHandle === 'extra'
			);
			expect(wired, `level ${level}: src.value -> ${id}.extra`).toBeDefined();
		}

		// Level 4 has `e: self.thing` which routes through the group's
		// input interface. In the frontend model this is source='grp' +
		// sourcePort='thing__inner' + sourceIsSelf=true (backend uses a
		// separate 'grp__in' node; same meaning, different representation).
		const level4 = expectedIds[4];
		const selfWired = p.project.edges.find(e =>
			e.source === 'grp' && e.sourceHandle === 'thing__inner'
			&& e.target === level4 && e.targetHandle === 'e'
		);
		expect(selfWired, 'expected grp.thing__inner -> level4.e').toBeDefined();

		// Chain: each level's .out -> parent's a/b/c/d port
		const chain = [
			['grp.root_host__data__a', 'grp.root_host__data', 'a'],
			['grp.root_host__data__a__b', 'grp.root_host__data__a', 'b'],
			['grp.root_host__data__a__b__c', 'grp.root_host__data__a__b', 'c'],
			['grp.root_host__data__a__b__c__d', 'grp.root_host__data__a__b__c', 'd'],
		];
		for (const [child, parent, port] of chain) {
			const wired = p.project.edges.find(e =>
				e.source === child && e.sourceHandle === 'out'
				&& e.target === parent && e.targetHandle === port
			);
			expect(wired, `${child} -> ${parent}.${port}`).toBeDefined();
		}

		// Final hop
		const finalEdge = p.project.edges.find(e =>
			e.source === 'grp.root_host__data' && e.sourceHandle === 'out'
			&& e.target === 'grp.root_host' && e.targetHandle === 'data'
		);
		expect(finalEdge).toBeDefined();
	});

describe('port synthesis rule', () => {
	it('rule: edge to catalog config field is an error', () => {
		// Text has catalog field `value`. Wiring into it should fail.
		const code = `# Project: T

upstream = Text { value: "x" }
n = Text { value: "default" }
n.value = upstream.value`;
		const p = parse(code);
		const err = p.errors.find(e => e.message.includes('has no input port: value'));
		expect(err).toBeDefined();
	});

	it('rule: edge to catalog output port is an error', () => {
		const code = `# Project: T

upstream = Text { value: "x" }
n = Template { template: "hi" }
n.text = upstream.value`;
		const p = parse(code);
		const err = p.errors.find(e => e.message.includes('has no input port: text'));
		expect(err).toBeDefined();
	});

	it('rule: literal to catalog output port is an error', () => {
		const code = `# Project: T

n = Template { template: "hi" }
n.text = "cannot set output"`;
		const p = parse(code);
		const err = p.errors.find(e => e.message.includes("cannot assign a literal to output port 'text'"));
		expect(err).toBeDefined();
	});

	it('rule: literal synthesizes input port when canAddInputPorts is true', () => {
		const code = `# Project: T

n = Template { template: "Hello {{name}}", name: "world" }`;
		const p = parseOk(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		const namePort = n.inputs.find(pp => pp.name === 'name');
		expect(namePort).toBeDefined();
		expect(namePort!.required).toBe(false);
		expect(namePort!.portType).toBe('String');
	});

	it('rule: literal synthesizes ports with different inferred types', () => {
		const code = `# Project: T

n = Template {
  template: "hi"
  count: 42
  enabled: true
  tags: ["a", "b"]
  meta: { "key": "value" }
}`;
		const p = parseOk(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.inputs.find(pp => pp.name === 'count')!.portType).toBe('Number');
		expect(n.inputs.find(pp => pp.name === 'enabled')!.portType).toBe('Boolean');
		expect(n.inputs.find(pp => pp.name === 'tags')!.portType).toContain('List');
		expect(n.inputs.find(pp => pp.name === 'meta')!.portType).toContain('Dict');
	});

	it('rule: literal on connection line synthesizes port', () => {
		const code = `# Project: T

n = Template { template: "Hello {{name}}" }
n.name = "world"`;
		const p = parseOk(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.inputs.some(pp => pp.name === 'name')).toBe(true);
	});

	it('rule: literal rejected on fixed-port node', () => {
		// Text has canAddInputPorts: false and no field `not_a_field`.
		const code = `# Project: T

n = Text { value: "hi", not_a_field: "oops" }`;
		const p = parse(code);
		const err = p.errors.find(e => e.message.includes("cannot add custom input port 'not_a_field'"));
		expect(err).toBeDefined();
	});

	it('rule: literal on catalog config field is not synthesized', () => {
		const code = `# Project: T

n = Text { value: "hello world" }`;
		const p = parseOk(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.inputs.some(pp => pp.name === 'value')).toBe(false);
	});
});

describe('port synthesis rule: list literals', () => {
	it('list of strings inferred as List[String]', () => {
		const code = `# Project: T

n = Template { template: "hi", tags: ["a", "b", "c"] }`;
		const p = parseOk(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.inputs.find(pp => pp.name === 'tags')!.portType).toBe('List[String]');
	});

	it('list of numbers inferred as List[Number]', () => {
		const code = `# Project: T

n = Template { template: "hi", nums: [1, 2, 3] }`;
		const p = parseOk(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.inputs.find(pp => pp.name === 'nums')!.portType).toBe('List[Number]');
	});

	it('multi-line list literal on connection line', () => {
		const code = `# Project: T

n = Template { template: "hi" }
n.tags = [
  "a",
  "b",
  "c"
]`;
		const p = parseOk(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.inputs.find(pp => pp.name === 'tags')!.portType).toBe('List[String]');
	});

	it('list of mixed types produces a List with union element', () => {
		const code = `# Project: T

n = Template { template: "hi", mixed: ["a", 1, true] }`;
		const p = parseOk(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.inputs.find(pp => pp.name === 'mixed')!.portType).toContain('List');
	});

	it('empty list stays a List type', () => {
		const code = `# Project: T

n = Template { template: "hi", empty: [] }`;
		const p = parseOk(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.inputs.find(pp => pp.name === 'empty')!.portType).toContain('List');
	});

	it('list of dicts', () => {
		const code = `# Project: T

n = Template { template: "hi", items: [{"a": 1}, {"a": 2}] }`;
		const p = parseOk(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.inputs.find(pp => pp.name === 'items')!.portType).toContain('List');
	});
});

describe('port synthesis rule: edge-driven ports', () => {
	// An edge targeting an undeclared port on a canAddInputPorts node
	// synthesizes the port with `required: true` and a fresh TypeVar
	// that narrows to the edge source's type. Counterpart to literal-driven
	// synthesis, which produces `required: false` ports.

	it('edge to undeclared port synthesizes required String', () => {
		const code = `# Project: T

src = Text { value: "hi" }
n = Template { template: "{{x}}" }
n.x = src.value`;
		const p = parseOk(code);
		const x = p.project.nodes.find(n => n.id === 'n')!.inputs.find(pp => pp.name === 'x')!;
		expect(x.required).toBe(true);
		expect(x.portType).toBe('String');
	});

	it('inline body wiring to undeclared port synthesizes required port', () => {
		const code = `# Project: T

src_name = Text { value: "alice" }
src_title = Text { value: "engineer" }
review = Template { template: "{{lead_info}}" }
review.lead_info = Template {
  template: "{{name}} - {{title}}"
  name: src_name.value
  title: src_title.value
}.text`;
		const p = parseOk(code);
		const anon = p.project.nodes.find(n => n.id === 'review__lead_info')!;
		const name = anon.inputs.find(pp => pp.name === 'name')!;
		const title = anon.inputs.find(pp => pp.name === 'title')!;
		expect(name.required).toBe(true);
		expect(name.portType).toBe('String');
		expect(title.required).toBe(true);
		expect(title.portType).toBe('String');
	});

	it('explicit String? declaration keeps port optional when edge-driven', () => {
		const code = `# Project: T

src = Text { value: "hi" }
n = Template(x: String?) { template: "{{x}}" }
n.x = src.value`;
		const p = parseOk(code);
		const x = p.project.nodes.find(n => n.id === 'n')!.inputs.find(pp => pp.name === 'x')!;
		expect(x.required).toBe(false);
	});

	it('literal declares optional, later edge does not upgrade to required', () => {
		const code = `# Project: T

src = Text { value: "hi" }
n = Template {
  template: "{{x}}"
  x: "default"
}
n.x = src.value`;
		const p = parseOk(code);
		const x = p.project.nodes.find(n => n.id === 'n')!.inputs.find(pp => pp.name === 'x')!;
		expect(x.required).toBe(false);
	});

	it('edge to undeclared port on non-canAddInputPorts node is a parse error', () => {
		const code = `# Project: T

src = Text { value: "hi" }
n = LlmInference -> (response: String) { label: "n" }
n.madeUpPort = src.value`;
		const p = parse(code);
		expect(p.errors.some(e => e.message.includes('madeUpPort'))).toBe(true);
	});
});

describe('null literal handling', () => {
	// Weft accepts bare `null` as a JSON null literal. The parser stores it
	// as null in config (not as the string "null"). Synthesis passes skip
	// null-valued keys so no port is created. The parser emits a warning
	// so the user sees that the null assignment was ignored, but the
	// project still compiles.

	it('bare `null` parses as JSON null, not the string "null"', () => {
		const code = `# Project: T

n = Template {
  template: "hi"
  x: null
}`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.config.x).toBeNull();
		expect(n.config.x).not.toBe('null');
	});

	it('null literal does not synthesize a port', () => {
		const code = `# Project: T

n = Template {
  template: "hi {{x}}"
  x: null
}`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.inputs.some(pp => pp.name === 'x')).toBe(false);
	});

	it('null literal emits a warning', () => {
		const code = `# Project: T

n = Template {
  template: "hi"
  x: null
}`;
		const p = parse(code);
		expect(p.warnings.some(w => w.message.includes("'x: null'") && w.message.includes('ignored'))).toBe(true);
	});

	it('null literal does not block compilation', () => {
		const code = `# Project: T

n = Template {
  template: "hi"
  x: null
}`;
		const p = parse(code);
		// No errors (the null is silently accepted via warning, not error).
		expect(p.errors.filter(e => e.message.toLowerCase().includes('null'))).toEqual([]);
	});

	it('connection-line null literal is also parsed as null', () => {
		const code = `# Project: T

n = Template { template: "hi" }
n.x = null`;
		const p = parse(code);
		const n = p.project.nodes.find(nn => nn.id === 'n')!;
		expect(n.config.x).toBeNull();
		// Warning is still emitted on the ignored synthesis.
		expect(p.warnings.some(w => w.message.includes("'x: null'") && w.message.includes('ignored'))).toBe(true);
	});
});

describe('error line numbers', () => {
	it('MustOverride error reports non-zero line', () => {
		const p = parse(`
# Project: Summarizer
article = Text {
  label: "Article"
  value: "Rust is a systems programming language..."
}

summarizer = LlmInference {
  label: "Summarizer"
}
summarizer.prompt = article.value

output = Debug { label: "Summary" }
output.data = summarizer.response
		`);
		const mustOverrideErr = p.errors.find(e => e.message.includes('requires a type declaration'));
		expect(mustOverrideErr).toBeDefined();
		expect(mustOverrideErr!.line).toBeGreaterThan(0);
	});

	it('unresolved type variable error reports non-zero line', () => {
		const p = parse(`
# Project: TypeVar
source = Text { value: "hello" }
gate = Gate
gate.value = source.value
output = Debug { label: "Out" }
output.data = gate.value
		`);
		// Gate's T resolves from value (wired to String), but gate.pass is
		// unwired so Gate has a validation error. If there is an unresolved
		// TypeVar error, its line must be non-zero.
		const typeVarErr = p.errors.find(e => e.message.includes('unresolved type variable'));
		if (typeVarErr) {
			expect(typeVarErr.line).toBeGreaterThan(0);
		}
		// At minimum, the MustOverride/validation errors should have real lines
		const anyNodeErr = p.errors.find(e => e.message.includes('Gate') || e.message.includes('gate'));
		if (anyNodeErr) {
			expect(anyNodeErr.line).toBeGreaterThan(0);
		}
	});
});

describe('post-config wrong order detection', () => {
	it('detects post-config outputs before config block', () => {
		const p = parse(`
# Project: WrongOrder
input = Text { value: "test" }
cfg = LlmConfig { model: "anthropic/claude-sonnet-4.6" }
qualify = LlmInference -> (response: JsonDict) -> (is_promising: Boolean, reason: String) {
  label: "Qualify"
  parseJson: true
}
qualify.prompt = input.value
qualify.config = cfg.config
		`);
		const wrongOrderErr = p.errors.find(e => e.message.includes('Two arrow clauses before the config block'));
		expect(wrongOrderErr).toBeDefined();
		expect(wrongOrderErr!.line).toBeGreaterThan(0);
	});

	it('wrong order error message warns about cascading', () => {
		const p = parse(`
# Project: WrongOrderCascade
input = Text { value: "test" }
cfg = LlmConfig { model: "anthropic/claude-sonnet-4.6" }
bad = LlmInference -> (response: JsonDict) -> (summary: String) {
  label: "Bad"
  parseJson: true
}
bad.prompt = input.value
bad.config = cfg.config
		`);
		const wrongOrderErr = p.errors.find(e => e.message.includes('Two arrow clauses before the config block'));
		expect(wrongOrderErr).toBeDefined();
		// The error message should warn that other errors may cascade from this
		expect(wrongOrderErr!.message).toContain('Other errors below are likely caused by this');
	});
});
