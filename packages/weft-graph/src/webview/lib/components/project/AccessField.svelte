<script lang="ts">
	// The `access` widget: the CONNECTION PICKER on an ACCESS NODE. The
	// picker itself is the connect library's (one set of components for
	// the editor and an instance's connect page); here it talks through the
	// host bridge, and what it picks is the small `{id, identity}` handle
	// the node's config holds. Pasted values go editor -> store directly
	// and are never written into node config.
	import { ConnectPicker } from '@weft/connect/svelte';
	import type { AccessSpecWire, AppRegistration } from '../../../../protocol';
	import { editorConnect } from './editor-connect';

	let {
		spec,
		projectApp,
		nodeType = null,
		value,
		onUpdate,
	}: {
		spec: AccessSpecWire;
		/// The access node's type, named in the permission picker's
		/// add-your-own hint.
		nodeType?: string | null;
		/// The project's declared PUBLIC app for this service.
		projectApp: AppRegistration | undefined;
		/// The config handle of the picked connection, if any.
		value: { id: string; identity?: string } | undefined;
		onUpdate: (v: { id: string; identity?: string } | null) => void;
	} = $props();
</script>

<div class="nodrag nopan">
	<ConnectPicker transport={editorConnect} {spec} {projectApp} {nodeType} {value} {onUpdate} />
</div>
