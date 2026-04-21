<script lang="ts">
	import { goto } from "$app/navigation";
	import { onMount } from "svelte";
	import { page } from "$app/stores";

	onMount(() => {
		// Don't redirect if there's an initialPath in URL params (e.g., /tasks/xxx)
		// The layout will handle navigation to that path
		const initialPath = $page.url.searchParams.get('path');
		if (!initialPath || initialPath === '/' || initialPath === '/dashboard') {
			goto("/dashboard");
		}
	});
</script>

<div class="flex min-h-screen items-center justify-center">
	<p class="text-muted-foreground">Redirecting...</p>
</div>
