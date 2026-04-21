import { api, authFetch } from '$lib/config';

export async function fetchExecutionCost(executionId: string): Promise<number | null> {
	try {
		const res = await authFetch(api.getExecutionCost(executionId));
		if (res.ok) {
			const data = await res.json();
			return data.cost;
		}
	} catch {}
	return null;
}

export function formatCost(n: number): string {
	return n < 0.01 && n > 0 ? `$${n.toFixed(4)}` : `$${n.toFixed(2)}`;
}
