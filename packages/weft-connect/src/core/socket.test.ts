import { describe, expect, it, vi } from 'vitest';
import { CONVERSATION_OVER, SETTLED_MS, openLiveSocket } from './socket';

// A stand-in socket that records what it was opened at and sent, and lets
// the test open and close it.
class FakeSocket {
	static OPEN = 1;
	static made: FakeSocket[] = [];
	readyState = 0;
	sent: string[] = [];
	onopen: (() => void) | null = null;
	onmessage: ((e: { data: string }) => void) | null = null;
	onclose: ((e: { code: number; reason: string }) => void) | null = null;
	constructor(public url: string) {
		FakeSocket.made.push(this);
	}
	send(data: string) {
		this.sent.push(data);
	}
	close(code: number) {
		this.drop(code);
	}
	open() {
		this.readyState = FakeSocket.OPEN;
		this.onopen?.();
	}
	drop(code: number, reason = '') {
		this.readyState = 3;
		this.onclose?.({ code, reason });
	}
}

const flush = () => new Promise((r) => setTimeout(r, 0));

describe('openLiveSocket', () => {
	it('reconnects with the same session after a drop, and sends what waited', async () => {
		vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
		try {
			FakeSocket.made = [];
			const drops: number[] = [];
			const live = openLiveSocket({
				url: 'https://w.example/connect/local/chat',
				onMessage: () => {},
				onDrop: (d) => drops.push(d.retryInMs),
				retry: { firstMs: 100, longestMs: 400 },
				WebSocketImpl: FakeSocket as unknown as typeof WebSocket,
			});
			await vi.waitFor(() => expect(FakeSocket.made.length).toBe(1));
			const first = FakeSocket.made[0];
			expect(first.url).toBe(`wss://w.example/connect/local/chat?session=${live.session}`);
			first.open();
			first.drop(1006);
			live.send('while away');
			await vi.advanceTimersByTimeAsync(100);
			await vi.waitFor(() => expect(FakeSocket.made.length).toBe(2));
			const second = FakeSocket.made[1];
			expect(second.url).toBe(first.url);
			second.open();
			expect(second.sent).toEqual(['while away']);
			expect(drops).toEqual([100]);
		} finally {
			vi.useRealTimers();
		}
	});

	it('stops when the program ends the conversation', async () => {
		FakeSocket.made = [];
		let ended = 0;
		openLiveSocket({
			url: 'http://127.0.0.1:14111/connect/local/chat',
			session: 's1',
			onMessage: () => {},
			onEnd: (e) => (ended = e.code),
			WebSocketImpl: FakeSocket as unknown as typeof WebSocket,
		});
		await vi.waitFor(() => expect(FakeSocket.made.length).toBe(1));
		expect(FakeSocket.made[0].url).toBe('ws://127.0.0.1:14111/connect/local/chat?session=s1');
		FakeSocket.made[0].open();
		FakeSocket.made[0].drop(CONVERSATION_OVER);
		await flush();
		expect(ended).toBe(CONVERSATION_OVER);
		expect(FakeSocket.made.length).toBe(1);
	});

	it('ends when the route refuses the caller, and refuses a send afterwards', async () => {
		FakeSocket.made = [];
		let ended: { status?: number } | null = null;
		const fetcher = (async () => new Response('refused', { status: 401 })) as unknown as typeof fetch;
		const live = openLiveSocket({
			url: 'https://w.example/connect/local/chat',
			headers: { 'Weft-Instance-Token': 'revoked' },
			onMessage: () => {},
			onEnd: (e) => (ended = e),
			fetcher,
			WebSocketImpl: FakeSocket as unknown as typeof WebSocket,
		});
		await vi.waitFor(() => expect(ended).not.toBeNull());
		expect(ended!.status).toBe(401);
		expect(FakeSocket.made.length).toBe(0);
		expect(() => live.send('late')).toThrow();
	});

	it('keeps backing off while each connection drops at once, and starts over after one that lasted', async () => {
		vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
		try {
			FakeSocket.made = [];
			const drops: number[] = [];
			openLiveSocket({
				url: 'https://w.example/connect/local/chat',
				onMessage: () => {},
				onDrop: (d) => drops.push(d.retryInMs),
				retry: { firstMs: 100, longestMs: 1_000 },
				WebSocketImpl: FakeSocket as unknown as typeof WebSocket,
			});
			for (let n = 1; n <= 3; n++) {
				await vi.waitFor(() => expect(FakeSocket.made.length).toBe(n));
				FakeSocket.made[n - 1].open();
				FakeSocket.made[n - 1].drop(1011);
				await vi.advanceTimersByTimeAsync(drops[drops.length - 1]);
			}
			expect(drops).toEqual([100, 200, 400]);
			// The fourth stays up past SETTLED_MS: its drop waits the first wait again.
			await vi.waitFor(() => expect(FakeSocket.made.length).toBe(4));
			FakeSocket.made[3].open();
			await vi.advanceTimersByTimeAsync(SETTLED_MS);
			FakeSocket.made[3].drop(1006);
			expect(drops).toEqual([100, 200, 400, 100]);
		} finally {
			vi.useRealTimers();
		}
	});

	it('with credentials, asks for the socket address first, carrying them', async () => {
		FakeSocket.made = [];
		const asked: { url: string; headers: HeadersInit | undefined }[] = [];
		const fetcher = (async (url: string, init?: RequestInit) => {
			asked.push({ url, headers: init?.headers });
			return new Response(JSON.stringify({ url: 'https://w.example/connect/local/chat?session=s2&wct=t', protocol: 'websocket' }));
		}) as unknown as typeof fetch;
		openLiveSocket({
			url: 'https://w.example/connect/local/chat',
			session: 's2',
			headers: { 'Weft-Instance-Token': 'wft-1' },
			onMessage: () => {},
			fetcher,
			WebSocketImpl: FakeSocket as unknown as typeof WebSocket,
		});
		await vi.waitFor(() => expect(FakeSocket.made.length).toBe(1));
		expect(asked).toEqual([{ url: 'https://w.example/connect/local/chat?session=s2', headers: { 'Weft-Instance-Token': 'wft-1' } }]);
		expect(FakeSocket.made[0].url).toBe('wss://w.example/connect/local/chat?session=s2&wct=t');
	});
});
