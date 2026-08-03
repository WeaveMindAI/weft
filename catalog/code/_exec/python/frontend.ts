import type { NodeTemplate } from '$lib/types';
import { CodeXml } from '@lucide/svelte';

export const CodeNode: NodeTemplate = {
	type: 'ExecPython',
	label: 'Code',
	description: 'Execute Python in a hosted sandbox. Input ports become variables, return a dict whose keys map to your output ports. Two extra ports `stdout` and `stderr` always carry whatever the code printed.',
	isBase: true,
	icon: CodeXml,
	color: '#5a8a6e',
	category: 'Utility',
	tags: ['python', 'script', 'transform', 'logic', 'compute'],
	fields: [
		{ key: 'code', label: 'Python Code', type: 'code', placeholder: '# Input ports are exposed as variables.\n# Return a dict whose keys are output port names.\n# Use None for ports that should NOT fire downstream.\n#\n# Example with input ports "data" and "threshold":\nif data["value"] > threshold:\n    return {"high": data, "low": None}\nelse:\n    return {"high": None, "low": data}' },
		{ key: 'dependencies', label: 'requirements.txt', type: 'code', placeholder: '# Pre-installed in the sandbox:\n# numpy, pandas, scipy, scikit-learn, matplotlib,\n# pillow, requests, httpx, beautifulsoup4, lxml, pyyaml\n#\n# Add extra packages below (one per line):\n# some-package==1.0.0\n# another-package>=2.0' },
	],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'stdout', portType: 'String', required: false, description: 'Everything the code printed to stdout' },
		{ name: 'stderr', portType: 'String', required: false, description: 'Everything the code printed to stderr' },
	],
	features: {
		canAddInputPorts: true,
		canAddOutputPorts: true,
	},
};
