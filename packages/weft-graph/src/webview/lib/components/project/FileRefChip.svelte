<script lang="ts">
	import type { TextFileRef } from '../../utils/file-field';

	/// The badge on a field a text file backs. It doubles as the marker
	/// toggle: @file (editable, edits save to the file) <-> @asset
	/// (pull-only). A remote source (a URL, a stored file) is fetched at
	/// build and cannot flip: nothing to write back to.
	let { ref, remote, onFlip }: { ref: TextFileRef; remote: boolean; onFlip: () => void } = $props();
</script>

<button
	type="button"
	class="text-[9px] font-mono px-1 py-0.5 rounded nodrag transition-colors
		{ref.marker === 'asset' ? 'bg-amber-100 text-amber-800 hover:bg-amber-200' : 'bg-muted text-muted-foreground hover:bg-accent'}"
	title={remote
		? `@asset(${ref.path}): fetched at build, shown as its source.`
		: ref.marker === 'asset'
			? `@asset(${ref.path}): pull-only, not editable here. Click to switch to @file (editable).`
			: `@file(${ref.path}): edits save to this file. Click to switch to @asset (pull-only).`}
	aria-label={ref.marker === 'asset' ? `Switch ${ref.path} to editable @file` : `Switch ${ref.path} to pull-only @asset`}
	onclick={(e) => { e.stopPropagation(); onFlip(); }}
><span aria-hidden="true">{ref.marker === 'asset' ? '🔒' : '📄'}</span> {ref.path}</button>
