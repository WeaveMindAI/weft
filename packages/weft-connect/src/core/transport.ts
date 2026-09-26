// How a connect page talks to weft. The page only ever asks these few
// questions (which connections, which doors, connect, forget, poll a
// consent), so a host plugs in by answering them: the weft editor answers
// through its host bridge, a member's page answers with a member token
// against the dispatcher's member door.

import { trimTrailingSlashes } from './url';
import type {
	AccessSpecWire,
	AppRegistration,
	CompletedConnect,
	ConsentOutcome,
	Door,
	DoorsStatus,
	GrantSummary,
	LookupItem,
	LookupPage,
	MemberField,
	MemberFieldRef,
	MemberLookupRequest,
	MemberPickerRequest,
	MemberValueInput,
	PickerOutcome,
	ResourceSource,
	StartedConsent,
	ValuesChanged,
	ValuesRequest,
} from './wire';

// The header a trusted backend names a member with, on a route gated by a
// connection (a member's own browser never uses it: it holds a token).
// SYNC: MEMBER_HEADER <-> crates/weft-core/src/member.rs MEMBER_HEADER
export const MEMBER_HEADER = 'Weft-Member';

// The header a member's browser presents its member token in, on a live
// route call: the run it starts is that member's.
// SYNC: MEMBER_TOKEN_HEADER <-> crates/weft-core/src/member.rs MEMBER_TOKEN_HEADER
export const MEMBER_TOKEN_HEADER = 'Weft-Member-Token';

/** A paste / shared-key connect, completed in one request. */
// SYNC: DirectConnect <-> crates/weft-core/src/access/wire.rs ConnectDirect
export interface DirectConnect {
	spec: AccessSpecWire;
	door: Door;
	paste: boolean;
	values: Record<string, string>;
	label: string | null;
	permissions: string[];
	// SYNC: shared_app <-> crates/weft-core/src/access/wire.rs SharedDoorPick
	shared_app: string | null;
	registration: AppRegistration | null;
}

/** A browser consent to start. */
// SYNC: ConsentStart <-> crates/weft-core/src/access/wire.rs BeginOAuth
export interface ConsentStart {
	spec: AccessSpecWire;
	door: Door;
	permissions: string[];
	// SYNC: shared_app <-> crates/weft-core/src/access/wire.rs SharedDoorPick
	shared_app: string | null;
	upgrade_grant_id: string | null;
	registration: AppRegistration | null;
}

/** Everything a connect page asks of weft. */
export interface ConnectTransport {
	/** The connections of one service this page may pick from. */
	connections(service: string): Promise<GrantSummary[]>;
	/** Forget one connection for good. */
	forget(connection: GrantSummary): Promise<void>;
	/** Which doors the service offers right now. */
	doors(spec: AccessSpecWire): Promise<DoorsStatus>;
	connectDirect(request: DirectConnect): Promise<CompletedConnect>;
	beginConsent(request: ConsentStart): Promise<StartedConsent>;
	/** A started consent's outcome, `null` while it is still pending.
	 *  Rejects once the consent is gone (unknown or expired), which ends
	 *  the wait. */
	consentOutcome(state: string): Promise<ConsentOutcome>;
	/** "Create it for me": mint an app from the recipe. Absent where the
	 *  page may not mint (a member makes no apps for the program). */
	mintApp?(spec: AccessSpecWire, permissions: string[]): Promise<{ values: Record<string, string> }>;
	/** The shared door of a paste-less service (the runtime's own key).
	 *  Absent where it is not offered: a member connects their own
	 *  account, never the program author's key. */
	allowsSharedKey: boolean;
	/** Open a page in the person's real browser (the consent page). */
	openExternal(url: string): void;
}

/** Everything a `remote_select` field asks of weft to fill its list and
 *  open its chooser. Each source is named by its position in the field's
 *  `sources` too: the editor sends the source itself (it signs with the
 *  author's connection it traced), a member's page sends only the
 *  position (the member door reads the source off the program, so a
 *  member token asks for nothing the program does not declare). */
export interface ResourceTransport {
	granted(index: number, source: Extract<ResourceSource, { kind: 'granted' }>): Promise<LookupItem[]>;
	list(
		index: number,
		source: Extract<ResourceSource, { kind: 'list' }>,
		query: string,
		parents: Record<string, string>,
		cursor: string | null,
	): Promise<LookupPage>;
	beginPicker(index: number, source: Extract<ResourceSource, { kind: 'picker' }>): Promise<{ state: string; url: string }>;
	/** A started chooser's outcome, `null` while it is still open.
	 *  Rejects once the chooser is gone (unknown or expired), which ends
	 *  the wait. */
	pickerOutcome(state: string): Promise<PickerOutcome>;
	/** Open a page in the person's real browser (the chooser page). */
	openExternal(url: string): void;
}

/** Where a website's pages reach weft: their own server, which passes the
 *  call on to the dispatcher (`server/passthrough.ts`). The browser never
 *  needs the dispatcher's address, which is often one it cannot reach.
 *  SYNC: WEFT_PASS_THROUGH_PATH <-> src/server/passthrough.ts, the route the READMEs mount */
export const WEFT_PASS_THROUGH_PATH = '/weft';

/** What the member door refused, with the HTTP status it answered: 401
 *  for a token it does not know (or one that expired), 403 for a token
 *  that is not a member token. */
export class MemberDoorError extends Error {
	constructor(
		message: string,
		readonly status: number,
	) {
		super(message);
		this.name = 'MemberDoorError';
	}

	/** The token acts as nobody in particular: it is not a member token. */
	get notAMemberToken(): boolean {
		return this.status === 403;
	}
}

/** A member of a program, at the dispatcher's member door, with their
 *  member token: everything a connect page needs, plus the fields the
 *  program asks the member to fill, their values, and the lists and
 *  choosers those fields fill from. */
export class MemberDoor implements ConnectTransport {
	readonly allowsSharedKey = false;
	private readonly base: string;
	private readonly fetcher: typeof fetch;
	private readonly opener: (url: string) => void;

	/** A page of a website reaches the door through its own server, at
	 *  `WEFT_PASS_THROUGH_PATH` (the pass-through in `server/`), so that is
	 *  the default `base`. Something that reaches the dispatcher itself
	 *  (the browser extension) passes the dispatcher's address instead.
	 *  `fetcher` defaults to the global `fetch`. */
	constructor(
		private readonly token: string,
		options: { base?: string; fetcher?: typeof fetch; opener?: (url: string) => void } = {},
	) {
		this.base = trimTrailingSlashes(options.base ?? WEFT_PASS_THROUGH_PATH);
		this.fetcher = options.fetcher ?? ((...args) => fetch(...args));
		this.opener =
			options.opener ??
			((url) => {
				globalThis.open?.(url, '_blank', 'noopener');
			});
	}

	private async call<T>(method: 'GET' | 'POST' | 'PUT' | 'DELETE', path: string, body?: unknown): Promise<T> {
		const resp = await this.fetcher(`${this.base}/member/${path}`, {
			method,
			headers: {
				Authorization: `Bearer ${this.token}`,
				...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
			},
			body: body === undefined ? undefined : JSON.stringify(body),
		});
		if (!resp.ok) {
			const text = await resp.text().catch(() => '');
			throw new MemberDoorError(text || `${method} /member/${path}: ${resp.status}`, resp.status);
		}
		if (resp.status === 204) return undefined as T;
		const text = await resp.text();
		return (text ? JSON.parse(text) : undefined) as T;
	}

	connections(service: string): Promise<GrantSummary[]> {
		return this.call('GET', `connections?service=${encodeURIComponent(service)}`);
	}

	forget(connection: GrantSummary): Promise<void> {
		return this.call('DELETE', `connections/${encodeURIComponent(connection.id)}`);
	}

	doors(spec: AccessSpecWire): Promise<DoorsStatus> {
		return this.call('POST', 'doors', { spec });
	}

	connectDirect(request: DirectConnect): Promise<CompletedConnect> {
		return this.call('POST', 'connections/direct', request);
	}

	beginConsent(request: ConsentStart): Promise<StartedConsent> {
		return this.call('POST', 'connections/begin', request);
	}

	consentOutcome(state: string): Promise<ConsentOutcome> {
		return this.call('GET', `connections/status?state=${encodeURIComponent(state)}`);
	}

	openExternal(url: string): void {
		this.opener(url);
	}

	/** Every field the program asks the member to fill, with what they
	 *  gave. */
	fields(): Promise<MemberField[]> {
		return this.call('GET', 'fields');
	}

	/** Give values and clear others, all at once. A live trigger of the
	 *  member that reads a changed value is set up again before this
	 *  answers, and the answer names it. */
	setValues(set: MemberValueInput[], clear: MemberFieldRef[] = []): Promise<ValuesChanged> {
		const body: ValuesRequest = { set, clear };
		return this.call('PUT', 'values', body);
	}

	/** The lists and chooser of one field the member fills. */
	resources(step: string, field: string): ResourceTransport {
		const lookup = <T,>(body: MemberLookupRequest) => this.call<T>('POST', 'lookup', body);
		const picker = (body: MemberPickerRequest) => this.call<{ state: string; url: string }>('POST', 'picker', body);
		return {
			granted: (index) => lookup<LookupItem[]>({ step, field, source: index }),
			list: (index, _source, query, parents, cursor) => lookup<LookupPage>({ step, field, source: index, query, parents, cursor }),
			beginPicker: (index) => picker({ step, field, source: index }),
			pickerOutcome: (state) => this.call('GET', `connections/status?state=${encodeURIComponent(state)}`),
			openExternal: (url) => this.opener(url),
		};
	}
}
