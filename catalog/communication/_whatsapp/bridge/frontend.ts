import type { NodeTemplate } from '$lib/types';
import { Smartphone } from '@lucide/svelte';

export const WhatsAppBridgeNode: NodeTemplate = {
	type: 'WhatsAppBridge',
	label: 'WhatsApp Bridge',
	description: 'Infrastructure node that maintains a WhatsApp connection via Baileys. Scan the QR code to link your phone. Other WhatsApp nodes connect through this bridge.',
	icon: Smartphone,
	color: '#25D366',
	category: 'Infrastructure',
	tags: ['infrastructure', 'whatsapp', 'messaging', 'bridge', 'baileys'],
	fields: [],
	defaultInputs: [],
	defaultOutputs: [
		{ name: 'instanceId', portType: 'String', required: false, description: 'Instance ID for this WhatsApp bridge' },
		{ name: 'endpointUrl', portType: 'String', required: false, description: 'Sidecar action endpoint URL, connect to WhatsApp Send / Receive nodes' },
		{ name: 'status', portType: 'String', required: false, description: 'Connection status (qr_pending, connecting, connected, disconnected)' },
		{ name: 'phoneNumber', portType: 'String', required: false, description: 'Connected phone number' },
		{ name: 'jid', portType: 'String', required: false, description: 'WhatsApp JID of the connected phone (e.g. 1234567890@s.whatsapp.net)' },
	],
	features: {
		isInfrastructure: true,
		hasLiveData: true,
	},
};
