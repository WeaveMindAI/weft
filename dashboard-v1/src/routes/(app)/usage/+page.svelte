<script lang="ts">
	import { onMount } from "svelte";
	import { browser } from "$app/environment";
	import { getApiUrl, authFetch } from "$lib/config";
	import { getUserId } from "$lib/utils";
	import { Chart } from "svelte-echarts";
	import { init, use } from "echarts/core";
	import type { EChartsOption } from "echarts";
	import { LineChart } from "echarts/charts";
	import { GridComponent, TooltipComponent, LegendComponent } from "echarts/components";
	import { CanvasRenderer } from "echarts/renderers";

	use([LineChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer]);

	interface DailyUsage {
		userId: string;
		date: string;
		serviceCostUsd: number;
		serviceBilledUsd: number;
		serviceRequests: number;
		tangleCostUsd: number;
		tangleBilledUsd: number;
		tangleRequests: number;
		executionCount: number;
		infraCostUsd: number;
	}

	let daily = $state<DailyUsage[]>([]);
	let creditBalance = $state<number | null>(null);
	let executionCost = $state<number>(0.01);
	let isLoading = $state(true);
	let error = $state<string | null>(null);

	let totalServiceBilled = $derived(daily.reduce((sum, d) => sum + (d.serviceBilledUsd ?? 0), 0));
	let totalServiceCostRaw = $derived(daily.reduce((sum, d) => sum + (d.serviceCostUsd ?? 0), 0));
	let totalTangleBilled = $derived(daily.reduce((sum, d) => sum + (d.tangleBilledUsd ?? 0), 0));
	let totalTangleCostRaw = $derived(daily.reduce((sum, d) => sum + (d.tangleCostUsd ?? 0), 0));
	let totalExecutions = $derived(daily.reduce((sum, d) => sum + d.executionCount, 0));
	let totalInfraCost = $derived(daily.reduce((sum, d) => sum + d.infraCostUsd, 0));

	let histogramDays = $derived.by(() => {
		const map = new Map(daily.map((d) => [d.date, d]));
		const days: DailyUsage[] = [];
		const today = new Date();
		for (let i = 9; i >= 0; i--) {
			const d = new Date(today);
			d.setHours(0, 0, 0, 0);
			d.setDate(today.getDate() - i);
			const key = d.toISOString().split('T')[0];
			days.push(map.get(key) ?? {
				userId: '', date: key, serviceCostUsd: 0, serviceBilledUsd: 0, serviceRequests: 0,
				tangleCostUsd: 0, tangleBilledUsd: 0, tangleRequests: 0, executionCount: 0, infraCostUsd: 0,
			});
		}
		return days;
	});

	let serviceSeries = $derived(histogramDays.map((d) => d.serviceBilledUsd ?? 0));
	let tangleSeries = $derived(histogramDays.map((d) => d.tangleBilledUsd ?? 0));
	let infraSeries = $derived(histogramDays.map((d) => d.infraCostUsd));
	let executionSeries = $derived(histogramDays.map((d) => d.executionCount));
	let totalCostSeries = $derived(histogramDays.map((d) => (d.serviceBilledUsd ?? 0) + (d.tangleBilledUsd ?? 0) + d.infraCostUsd));
	let maxCostChart = $derived(Math.max(1, ...totalCostSeries));
	let maxExecutionChart = $derived(Math.max(1, ...executionSeries));

	function formatCost(cost: number): string {
		if (cost < 0.01 && cost > 0) return `$${cost.toFixed(4)}`;
		return `$${cost.toFixed(2)}`;
	}

	function shortDayLabel(isoDate: string): string {
		const d = new Date(`${isoDate}T00:00:00`);
		return d.toLocaleDateString(undefined, { month: 'numeric', day: 'numeric' });
	}

	let lineOptions = $derived.by<EChartsOption>(() => ({
		grid: { left: 56, right: 56, top: 20, bottom: 38 },
		legend: { top: 0, textStyle: { color: '#71717a', fontSize: 11 } },
		tooltip: {
			trigger: 'axis',
			axisPointer: { type: 'line', snap: true },
			backgroundColor: '#fff',
			borderColor: '#e4e4e7',
			textStyle: { color: '#3f3f46', fontSize: 11 },
			formatter: (params) => {
				const points = Array.isArray(params) ? params : [params];
				const idx = typeof points[0]?.dataIndex === 'number' ? points[0].dataIndex : -1;
				const dayLabel = idx >= 0 && histogramDays[idx] ? shortDayLabel(histogramDays[idx].date) : '';
				const lines = points.map((p) => {
					const value = Array.isArray(p.value) ? Number(p.value[1]) : Number(p.value ?? 0);
					const display = p.seriesName === 'Executions'
						? `${Math.round(Number.isFinite(value) ? value : 0)}`
						: formatCost(Number.isFinite(value) ? value : 0);
					return `${p.marker}${p.seriesName}: ${display}`;
				});
				return [dayLabel, ...lines].join('<br/>');
			},
		},
		xAxis: {
			type: 'category',
			boundaryGap: false,
			data: histogramDays.map((d) => shortDayLabel(d.date)),
			axisLabel: { color: '#a1a1aa', fontSize: 10 },
			axisLine: { lineStyle: { color: '#e4e4e7' } },
			axisTick: { show: false },
		},
		yAxis: [
			{
				type: 'value', min: 0, max: maxCostChart,
				axisLabel: { color: '#a1a1aa', fontSize: 10, formatter: (v: number) => formatCost(v) },
				splitLine: { lineStyle: { color: '#f4f4f5' } },
			},
			{
				type: 'value', min: 0, max: maxExecutionChart,
				axisLabel: { color: '#a1a1aa', fontSize: 10, formatter: (v: number) => `${Math.round(v)}` },
				splitLine: { show: false },
			},
		],
		series: [
			{ name: 'Service', type: 'line', smooth: true, symbol: 'circle', symbolSize: 6, showSymbol: false, emphasis: { disabled: true }, blur: { lineStyle: { opacity: 1 }, itemStyle: { opacity: 1 } }, lineStyle: { width: 2, color: '#3b82f6' }, itemStyle: { color: '#3b82f6' }, data: serviceSeries },
			{ name: 'Tangle', type: 'line', smooth: true, symbol: 'circle', symbolSize: 6, showSymbol: false, emphasis: { disabled: true }, blur: { lineStyle: { opacity: 1 }, itemStyle: { opacity: 1 } }, lineStyle: { width: 2, color: '#8b5cf6' }, itemStyle: { color: '#8b5cf6' }, data: tangleSeries },
			{ name: 'Infrastructure', type: 'line', smooth: true, symbol: 'circle', symbolSize: 6, showSymbol: false, emphasis: { disabled: true }, blur: { lineStyle: { opacity: 1 }, itemStyle: { opacity: 1 } }, lineStyle: { width: 2, color: '#f59e0b' }, itemStyle: { color: '#f59e0b' }, data: infraSeries },
			{ name: 'Executions', type: 'line', yAxisIndex: 1, smooth: true, symbol: 'circle', symbolSize: 6, showSymbol: false, emphasis: { disabled: true }, blur: { lineStyle: { opacity: 1 }, itemStyle: { opacity: 1 } }, lineStyle: { width: 2, color: '#10b981' }, itemStyle: { color: '#10b981' }, data: executionSeries },
		],
	}));

	async function fetchWithBackoff(url: string): Promise<Response> {
		const delays = [0, 2000, 5000, 20000, 20000];
		let lastError: unknown;
		for (const delay of delays) {
			if (delay > 0) await new Promise((r) => setTimeout(r, delay));
			try {
				const res = await authFetch(url);
				if (res.status !== 429 && res.status !== 503 && res.status !== 502) return res;
				lastError = new Error(`HTTP ${res.status}`);
			} catch (e) { lastError = e; }
		}
		throw lastError;
	}

	let serviceProviderSub = $derived(totalServiceCostRaw > 0 && totalServiceCostRaw !== totalServiceBilled ? `Provider: ${formatCost(totalServiceCostRaw)}` : null);
	let tangleProviderSub = $derived(totalTangleCostRaw > 0 && totalTangleCostRaw !== totalTangleBilled ? `Provider: ${formatCost(totalTangleCostRaw)}` : null);

	onMount(async () => {
		if (!browser) return;
		const userId = getUserId();
		const apiUrl = getApiUrl();
		const to = new Date().toISOString().split('T')[0];
		const from = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
		try {
			const response = await fetchWithBackoff(`${apiUrl}/api/v1/usage/${userId}?from=${from}&to=${to}`);
			if (!response.ok) throw new Error(`Failed to fetch usage: ${response.statusText}`);
			daily = (await response.json()).daily || [];
			const creditsRes = await fetchWithBackoff(`${apiUrl}/api/v1/credits?userId=${userId}`);
			if (creditsRes.ok) {
				const creditsData = await creditsRes.json();
				creditBalance = creditsData.balance ?? 0;
				executionCost = creditsData.executionCost ?? 0.01;
			}
		} catch (e) {
			console.error("Failed to fetch usage:", e);
			error = e instanceof Error ? e.message : "Unknown error";
		} finally {
			isLoading = false;
		}
	});
</script>

<div class="min-h-screen pt-20 px-6 pb-12" style="background: #f8f9fa; background-image: radial-gradient(circle, #d4d4d8 1px, transparent 1px); background-size: 24px 24px;">
	<div class="max-w-4xl mx-auto">

		<div class="mb-5">
			<h2 class="text-[15px] font-semibold text-zinc-800">Usage</h2>
			<p class="text-[12px] text-zinc-400 mt-0.5">Last 30 days</p>
		</div>

		{#if isLoading}
			<div class="flex items-center justify-center py-24">
				<div class="h-5 w-5 border-2 border-zinc-300 border-t-zinc-600 rounded-full animate-spin"></div>
			</div>
		{:else if error}
			<div class="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-[12px] text-red-600">{error}</div>
		{:else}
			<!-- Summary cards -->
			<div class="grid gap-3 grid-cols-2 lg:grid-cols-5 mb-5">
				<div class="bg-white rounded-xl border border-zinc-200 px-4 py-3" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
					<div class="flex items-center gap-1.5 mb-2">
						<span class="w-1.5 h-1.5 rounded-full bg-zinc-600"></span>
						<span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Credits Left</span>
					</div>
					<p class="text-[18px] font-semibold text-zinc-800 tabular-nums">{creditBalance === null ? 'N/A' : formatCost(creditBalance)}</p>
				</div>
				<div class="bg-white rounded-xl border border-zinc-200 px-4 py-3" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
					<div class="flex items-center gap-1.5 mb-2">
						<span class="w-1.5 h-1.5 rounded-full bg-blue-500"></span>
						<span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Service</span>
					</div>
					<p class="text-[18px] font-semibold text-zinc-800 tabular-nums">{formatCost(totalServiceBilled)}</p>
					{#if serviceProviderSub}<p class="text-[10px] text-zinc-400 mt-0.5">{serviceProviderSub}</p>{/if}
				</div>
				<div class="bg-white rounded-xl border border-zinc-200 px-4 py-3" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
					<div class="flex items-center gap-1.5 mb-2">
						<span class="w-1.5 h-1.5 rounded-full bg-violet-500"></span>
						<span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Tangle</span>
					</div>
					<p class="text-[18px] font-semibold text-zinc-800 tabular-nums">{formatCost(totalTangleBilled)}</p>
					{#if tangleProviderSub}<p class="text-[10px] text-zinc-400 mt-0.5">{tangleProviderSub}</p>{/if}
				</div>
				<div class="bg-white rounded-xl border border-zinc-200 px-4 py-3" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
					<div class="flex items-center gap-1.5 mb-2">
						<span class="w-1.5 h-1.5 rounded-full bg-emerald-500"></span>
						<span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Executions</span>
					</div>
					<p class="text-[18px] font-semibold text-zinc-800 tabular-nums">{totalExecutions}</p>
					<p class="text-[10px] text-zinc-400 mt-0.5">${executionCost}/run</p>
				</div>
				<div class="bg-white rounded-xl border border-zinc-200 px-4 py-3" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
					<div class="flex items-center gap-1.5 mb-2">
						<span class="w-1.5 h-1.5 rounded-full bg-amber-500"></span>
						<span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Infrastructure</span>
					</div>
					<p class="text-[18px] font-semibold text-zinc-800 tabular-nums">{formatCost(totalInfraCost)}</p>
					<p class="text-[10px] text-zinc-400 mt-0.5">At cost + margin</p>
				</div>
			</div>

			<!-- Chart -->
			{#if daily.length > 0}
				<div class="bg-white rounded-xl border border-zinc-200 overflow-hidden" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
					<div class="px-4 py-3 border-b border-zinc-100">
						<span class="text-[12px] font-semibold text-zinc-700">Daily Cost (last 10 days)</span>
					</div>
					<div class="px-4 py-3">
						<div class="flex flex-wrap items-center gap-4 text-[10px] text-zinc-400 mb-3">
							<span class="inline-flex items-center gap-1"><span class="inline-block h-1.5 w-1.5 rounded-full bg-blue-500"></span>Service ({formatCost(totalServiceBilled)})</span>
							<span class="inline-flex items-center gap-1"><span class="inline-block h-1.5 w-1.5 rounded-full bg-violet-500"></span>Tangle ({formatCost(totalTangleBilled)})</span>
							<span class="inline-flex items-center gap-1"><span class="inline-block h-1.5 w-1.5 rounded-full bg-amber-500"></span>Infra ({formatCost(totalInfraCost)})</span>
							<span class="inline-flex items-center gap-1"><span class="inline-block h-1.5 w-1.5 rounded-full bg-emerald-500"></span>Executions ({totalExecutions})</span>
						</div>
						<div class="h-64 w-full">
							<Chart {init} options={lineOptions} class="h-full w-full" />
						</div>
					</div>
				</div>
			{:else}
				<div class="bg-white rounded-xl border border-zinc-200 px-4 py-12 text-center" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
					<p class="text-[13px] text-zinc-400">No usage data yet</p>
					<p class="text-[11px] text-zinc-300 mt-1">Run some projects to see your usage here</p>
				</div>
			{/if}
		{/if}
	</div>
</div>
