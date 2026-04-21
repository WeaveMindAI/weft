<script lang="ts">
	import { page } from '$app/stores';
	import { onMount, onDestroy } from 'svelte';
	import { browser } from '$app/environment';
	import * as te from '$lib/telemetry-events';

	interface FormFieldRender {
		component: string;
		source?: 'static' | 'input';
		multiple?: boolean;
		prefilled?: boolean;
	}

	interface FormField {
		fieldType: string;
		key: string;
		render?: FormFieldRender;
		value?: unknown;
		config?: Record<string, unknown>;
	}

	interface FormSchema {
		fields: FormField[];
	}

	interface PendingTask {
		executionId: string;
		nodeId: string;
		title: string;
		description?: string;
		createdAt: string;
		taskType?: string;
		formSchema?: FormSchema;
		metadata?: Record<string, unknown>;
	}

	let task = $state<PendingTask | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let submitting = $state(false);
	let completed = $state(false);

	let formValues = $state<Record<string, unknown>>({});
	let buttonDecisions = $state<Record<string, boolean | null>>({});

	const executionId = $derived($page.params.executionId);
	const nodeId = $derived($page.url.searchParams.get('nodeId') || '');
	const token = $derived($page.url.searchParams.get('token') || '');

	// Listen for field fill messages from the website sidebar
	function handleParentMessage(event: MessageEvent) {
		if (!event.data?.type) return;
		if (event.data.type === 'fillFormField' && event.data.key && task) {
			const { key, value } = event.data;
			// Check if it's a button decision field
			if (key in buttonDecisions) {
				buttonDecisions = { ...buttonDecisions, [key]: value };
			} else {
				formValues = { ...formValues, [key]: value };
			}
		}
		if (event.data.type === 'fillFormFields' && event.data.fields && task) {
			const fields = event.data.fields as Record<string, unknown>;
			for (const [key, value] of Object.entries(fields)) {
				if (key in buttonDecisions) {
					buttonDecisions = { ...buttonDecisions, [key]: value as boolean };
				} else {
					formValues = { ...formValues, [key]: value };
				}
			}
		}
	}

	/// Strip Svelte 5 reactive proxies so values can be cloned by postMessage.
	function toPlain<T>(value: T): T {
		return JSON.parse(JSON.stringify(value));
	}

	/// Notify the website that form context is available (for AI sidebar).
	function sendFormContext(t: PendingTask) {
		if (!browser || window.parent === window) return;
		console.log('[sendFormContext] formSchema:', JSON.stringify(t.formSchema)?.substring(0, 200));
		console.log('[sendFormContext] metadata:', JSON.stringify(t.metadata)?.substring(0, 200));
		console.log('[sendFormContext] title:', t.title, 'description:', t.description);
		window.parent.postMessage(toPlain({
			type: 'formPageContext',
			executionId,
			formSchema: t.formSchema,
			metadata: t.metadata,
			formTitle: t.title,
			formDescription: t.description,
			taskType: t.taskType,
			formValues,
			extensionToken: token,
		}), '*');
	}

	/// Notify the website that form values changed.
	function sendFormStateChanged() {
		if (!browser || window.parent === window) return;
		window.parent.postMessage(toPlain({
			type: 'formStateChanged',
			formValues: { ...formValues, ...Object.fromEntries(
				Object.entries(buttonDecisions).filter(([, v]) => v !== null)
			) },
		}), '*');
	}

	// Send form state changes to parent
	$effect(() => {
		if (task && !loading) {
			// Access reactive state to trigger effect
			const _vals = JSON.stringify(formValues);
			const _btns = JSON.stringify(buttonDecisions);
			sendFormStateChanged();
		}
	});

	onMount(async () => {
		if (browser) {
			window.addEventListener('message', handleParentMessage);
		}
		if (!token || !nodeId) {
			error = 'Missing required parameters (token or nodeId)';
			loading = false;
			return;
		}
		await fetchTask();
	});

	onDestroy(() => {
		if (browser) {
			window.removeEventListener('message', handleParentMessage);
			// Tell parent the form page is gone
			if (window.parent !== window) {
				window.parent.postMessage({ type: 'formPageClosed' }, '*');
			}
		}
	});

	async function fetchTask() {
		loading = true;
		error = null;
		try {
			const response = await fetch(`/api/ext/${token}/tasks`);
			if (!response.ok) {
				error = response.status === 401 ? 'Invalid or expired token.' : `Failed to fetch task: ${response.status}`;
				loading = false;
				return;
			}
			const data = await response.json();
			const tasks = (data.tasks || []) as PendingTask[];
			const found = tasks.find(t => t.executionId === executionId && t.nodeId === nodeId);
			if (found) {
				task = found;
				initFormState(found);
				sendFormContext(found);
			} else {
				error = 'Task not found. It may have already been completed.';
			}
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to connect';
		} finally {
			loading = false;
		}
	}

	function initFormState(t: PendingTask) {
		const fields = t.formSchema?.fields ?? [];
		const vals: Record<string, unknown> = {};
		const decisions: Record<string, boolean | null> = {};
		for (const f of fields) {
			if (!f.key) continue;
			const r = f.render;
			if (!r) continue;
			if (r.component === 'buttons') decisions[f.key] = null;
			else if (r.component === 'select' && r.multiple) vals[f.key] = [];
			else if ((r.component === 'textarea' || r.component === 'text') && r.prefilled) vals[f.key] = typeof f.value === 'string' ? f.value : '';
			else if (r.component !== 'readonly') vals[f.key] = '';
		}
		formValues = vals;
		buttonDecisions = decisions;
	}

	function isFormValid(): boolean {
		if (!task?.formSchema) return true;
		// Only approve_reject fields are required. All other field types (text, select, textarea)
		// are intentionally optional: missing values produce null on the output port.
		for (const f of task.formSchema.fields) {
			if (!f.key || !f.render) continue;
			if (f.render.component === 'buttons' && buttonDecisions[f.key] === null) return false;
		}
		return true;
	}

	const isTrigger = $derived(task?.taskType === 'Trigger');

	async function submitForm() {
		if (!task || !token) return;
		te.form.submitted(task.taskType || 'unknown', Object.keys(formValues).length);
		submitting = true;
		error = null;
		try {
			const inputPayload: Record<string, unknown> = { ...formValues };
			for (const [key, decision] of Object.entries(buttonDecisions)) {
				if (decision !== null) inputPayload[key] = decision;
			}

			let url: string;
			let body: string;

			if (isTrigger) {
				// Trigger forms use the trigger submit endpoint
				const triggerTaskId = encodeURIComponent(task.executionId);
				url = `/api/ext/${token}/triggers/${triggerTaskId}/submit`;
				body = JSON.stringify({ nodeId: task.nodeId, input: inputPayload });
			} else {
				// Regular task forms use the complete endpoint
				const parts = task.executionId.split('-');
				const projectExecutionId = parts.slice(0, 5).join('-');
				url = `/api/ext/${token}/tasks/${projectExecutionId}/complete`;
				body = JSON.stringify({ nodeId: task.nodeId, input: inputPayload, callbackId: task.executionId });
			}

			const response = await fetch(url, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body,
			});
			if (!response.ok) throw new Error(`Failed: ${await response.text()}`);
			completed = true;

			// For triggers, reset form after brief delay so user can submit again
			if (isTrigger) {
				setTimeout(() => {
					completed = false;
					if (task) initFormState(task);
				}, 1500);
			}
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to submit';
		} finally {
			submitting = false;
		}
	}

	function toggleMultiSelect(key: string, option: string) {
		const current = (formValues[key] as string[]) ?? [];
		formValues = {
			...formValues,
			[key]: current.includes(option) ? current.filter(o => o !== option) : [...current, option],
		};
	}

	function getOptions(field: FormField): string[] {
		if (field.render?.source === 'input') return Array.isArray(field.value) ? (field.value as string[]) : [];
		return (field.config?.options as string[]) ?? [];
	}

	function fmt(value: unknown): string {
		if (value === null || value === undefined) return '';
		if (typeof value === 'string') return value;
		if (typeof value === 'number' || typeof value === 'boolean') return String(value);
		return JSON.stringify(value, null, 2);
	}

	function isComplex(value: unknown): boolean {
		return typeof value === 'object' && value !== null;
	}
</script>

<svelte:head>
	<title>{task?.title || 'Task Review'} | WeaveMind</title>
</svelte:head>

<div class="min-h-screen relative overflow-hidden" style="background: #fafafa;">
	<div class="absolute inset-0 pointer-events-none" style="background-image: radial-gradient(circle, #d4d4d8 1px, transparent 1px); background-size: 24px 24px;"></div>
	<div class="relative z-10 min-h-screen flex justify-center px-4 pt-24 pb-8">
		<div class="w-full max-w-2xl">
			{#if loading}
				<div class="bg-white rounded-lg shadow-lg border border-zinc-200 overflow-hidden">
					<div class="px-4 py-3 border-b border-zinc-100 flex items-center gap-3">
						<div class="w-2.5 h-2.5 rounded-full bg-amber-500"></div>
						<span class="text-sm font-semibold text-zinc-700">Loading Task</span>
					</div>
					<div class="p-8 text-center">
						<div class="w-6 h-6 border-2 border-zinc-300 border-t-amber-500 rounded-full animate-spin mx-auto mb-3"></div>
						<p class="text-sm text-zinc-500">Loading task details...</p>
					</div>
				</div>
			{:else if error}
				<div class="bg-white rounded-lg shadow-lg border border-zinc-200 overflow-hidden">
					<div class="px-4 py-3 border-b border-zinc-100 flex items-center gap-3">
						<div class="w-2.5 h-2.5 rounded-full bg-red-500"></div>
						<span class="text-sm font-semibold text-zinc-700">Error</span>
					</div>
					<div class="p-6">
						<div class="p-3 bg-red-50 border border-red-200 rounded-lg text-red-600 text-sm">{error}</div>
					</div>
				</div>
			{:else if completed}
				<div class="bg-white rounded-lg shadow-lg border border-zinc-200 overflow-hidden">
					<div class="px-4 py-3 border-b border-zinc-100 flex items-center gap-3">
						<div class="w-2.5 h-2.5 rounded-full bg-green-500"></div>
						<span class="text-sm font-semibold text-zinc-700">Submitted</span>
					</div>
					<div class="p-6 text-center">
						<div class="w-12 h-12 bg-green-50 rounded-full flex items-center justify-center mx-auto mb-3">
							<svg class="w-6 h-6 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
								<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
							</svg>
						</div>
						<p class="text-sm text-zinc-600 mb-1">{isTrigger ? 'Trigger fired!' : 'Your response has been submitted.'}</p>
						<p class="text-xs text-zinc-400">{isTrigger ? 'The project execution has started.' : 'You can close this window.'}</p>
					</div>
				</div>
			{:else if task}
				<div class="bg-white rounded-lg shadow-lg border border-zinc-200 overflow-hidden">
					<div class="px-4 py-3 border-b border-zinc-100 flex items-center gap-3">
						<div class="w-2.5 h-2.5 rounded-full bg-amber-500"></div>
						<div class="flex-1 min-w-0">
							<span class="text-sm font-semibold text-zinc-700 truncate block">{task.title}</span>
							{#if task.description}
								<span class="text-xs text-zinc-400 truncate block">{task.description}</span>
							{/if}
						</div>
					</div>

					<div class="p-5 space-y-4">
						{#if task.formSchema}
							{#each task.formSchema.fields as field}
								{@const r = field.render}
								{#if r?.component === 'readonly'}
									<div>
										<p class="text-xs font-medium text-zinc-500 mb-1">{field.key}</p>
										{#if isComplex(field.value)}
											<pre class="text-xs text-zinc-700 bg-zinc-50 border border-zinc-200 rounded-md p-3 overflow-auto max-h-40 font-mono whitespace-pre-wrap">{fmt(field.value)}</pre>
										{:else}
											<p class="text-sm text-zinc-800 bg-zinc-50 border border-zinc-200 rounded-md px-3 py-2 min-h-[36px]">{fmt(field.value) || '(empty)'}</p>
										{/if}
									</div>
								{:else if r?.component === 'image'}
									{@const imgSrc = typeof field.value === 'string' ? field.value : ((field.value as Record<string, unknown>)?.url as string | undefined)}
									<div>
										<p class="text-xs font-medium text-zinc-500 mb-1">{(field.config?.label as string) ?? field.key}</p>
										{#if imgSrc}
											<img
												src={imgSrc}
												alt={(field.config?.label as string) ?? field.key}
												class="max-w-full max-h-80 rounded-md border border-zinc-200 object-contain bg-zinc-50"
											/>
										{:else}
											<p class="text-sm text-zinc-400 italic bg-zinc-50 border border-zinc-200 rounded-md px-3 py-2">(no image)</p>
										{/if}
									</div>
								{:else if r?.component === 'buttons'}
									{@const decision = buttonDecisions[field.key]}
									<div>
										<p class="text-xs font-medium text-zinc-500 mb-2">{field.key}</p>
										<div class="flex gap-2">
											<button
												onclick={() => { buttonDecisions = { ...buttonDecisions, [field.key]: false }; }}
												class="flex-1 py-2 px-4 rounded-md font-medium text-sm transition-colors {decision === false ? 'bg-red-500 text-white' : 'bg-zinc-100 hover:bg-zinc-200 text-zinc-700'}"
											>{(field.config?.rejectLabel as string) || 'Reject'}</button>
											<button
												onclick={() => { buttonDecisions = { ...buttonDecisions, [field.key]: true }; }}
												class="flex-1 py-2 px-4 rounded-md font-medium text-sm transition-colors {decision === true ? 'bg-green-500 text-white' : 'bg-zinc-100 hover:bg-zinc-200 text-zinc-700'}"
											>{(field.config?.approveLabel as string) || 'Approve'}</button>
										</div>
									</div>
								{:else if r?.component === 'select'}
									{@const options = getOptions(field)}
									{#if r.multiple}
										{@const selected = (formValues[field.key] as string[]) ?? []}
										<div>
											<p class="text-xs font-medium text-zinc-500 mb-1.5">{field.key}</p>
											<div class="flex flex-wrap gap-2">
												{#each options as option}
													<button
														onclick={() => toggleMultiSelect(field.key, option)}
														class="px-3 py-1.5 rounded-md text-sm font-medium transition-colors {selected.includes(option) ? 'bg-zinc-800 text-white' : 'bg-zinc-100 hover:bg-zinc-200 text-zinc-700'}"
													>{option}</button>
												{/each}
												{#if options.length === 0}
													<p class="text-xs text-zinc-400 italic">No options available</p>
												{/if}
											</div>
										</div>
									{:else}
										<div>
											<p class="text-xs font-medium text-zinc-500 mb-1.5">{field.key}</p>
											<div class="flex flex-wrap gap-2">
												{#each options as option}
													<button
														onclick={() => { formValues = { ...formValues, [field.key]: option }; }}
														class="px-3 py-1.5 rounded-md text-sm font-medium transition-colors {formValues[field.key] === option ? 'bg-zinc-800 text-white' : 'bg-zinc-100 hover:bg-zinc-200 text-zinc-700'}"
													>{option}</button>
												{/each}
												{#if options.length === 0}
													<p class="text-xs text-zinc-400 italic">No options available</p>
												{/if}
											</div>
										</div>
									{/if}
								{:else if r?.component === 'text'}
									<div>
										<p class="text-xs font-medium text-zinc-500 mb-1.5">{field.key}</p>
										<input
											type="text"
											class="w-full px-3 py-2 bg-zinc-50 border border-zinc-200 rounded-md text-zinc-800 placeholder-zinc-400 focus:outline-none focus:ring-2 focus:ring-amber-500/20 focus:border-amber-500 transition-all text-sm"
											placeholder={field.key}
											value={(formValues[field.key] as string) ?? ''}
											oninput={(e) => { formValues = { ...formValues, [field.key]: e.currentTarget.value }; }}
										/>
									</div>
								{:else if r?.component === 'textarea'}
									<div>
										<p class="text-xs font-medium text-zinc-500 mb-1.5">{field.key}</p>
										<textarea
											class="w-full px-3 py-2 bg-zinc-50 border border-zinc-200 rounded-md text-zinc-800 placeholder-zinc-400 focus:outline-none focus:ring-2 focus:ring-amber-500/20 focus:border-amber-500 transition-all text-sm resize-none"
											rows={r.prefilled ? 6 : 3}
											value={(formValues[field.key] as string) ?? ''}
											oninput={(e) => { formValues = { ...formValues, [field.key]: e.currentTarget.value }; }}
										></textarea>
									</div>
								{/if}
							{/each}
							<button
								onclick={submitForm}
								disabled={submitting || !isFormValid()}
								class="w-full py-2.5 px-4 bg-zinc-800 hover:bg-zinc-700 disabled:bg-zinc-300 disabled:cursor-not-allowed text-white font-medium rounded-md transition-colors text-sm"
							>
								{#if submitting}
									<span class="flex items-center justify-center gap-2">
										<span class="w-3 h-3 border-2 border-zinc-400 border-t-transparent rounded-full animate-spin"></span>
									</span>
								{:else}
									{isTrigger ? 'Run' : 'Submit'}
								{/if}
							</button>
						{:else}
							<p class="text-sm text-zinc-400 italic text-center py-4">No form fields configured for this task.</p>
						{/if}
					</div>
				</div>
				<p class="text-center text-zinc-400 text-xs mt-4">Task ID: {task.executionId.slice(0, 8)}...</p>
			{/if}
		</div>
	</div>
</div>
