// How a connect page talks to weft. The page only ever asks these few
// questions (which connections, which doors, connect, forget, poll a
// consent), so a host plugs in by answering them: the weft editor answers
// through its host bridge, an instance's page answers with an instance
// token against the dispatcher's instance door, and a website's admin page
// answers with a connections key against the program door (through the
// site's server, which holds the key).

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
	FilledField,
	InstanceFieldRef,
	InstanceLookupRequest,
	InstancePickerRequest,
	InstanceValueInput,
	PickerOutcome,
	ProgramConnected,
	ResourceSource,
	StartedConsent,
	ValuesChanged,
	ValuesRequest,
} from './wire';

// The header a trusted backend names an instance with, on a route gated by
// a connection (a browser never uses it: it holds an instance token).
// SYNC: INSTANCE_HEADER <-> crates/weft-core/src/instance.rs INSTANCE_HEADER
export const INSTANCE_HEADER = 'Weft-Instance';

// The header a browser presents its instance token in, on a live route
// call: the run it starts is that instance's.
// SYNC: INSTANCE_TOKEN_HEADER <-> crates/weft-core/src/instance.rs INSTANCE_TOKEN_HEADER
export const INSTANCE_TOKEN_HEADER = 'Weft-Instance-Token';

/** A paste / shared-key connect, completed in one request. */
// SYNC: DirectConnect <-> crates/weft-core/src/access/wire.rs ConnectDirect
export interface DirectConnect {
	spec: AccessSpecWire;
	door: Door;
	paste: boolean;
	values: Record<string, string>;
	/** The way to sign in `values` were filled for (`LoginWayWire.id`):
	 *  set on every own-door connect of a `static` service, `null` for
	 *  the paste section, the shared door and every other kind. */
	way: string | null;
	label: string | null;
	permissions: string[];
	// SYNC: sharedApp <-> crates/weft-core/src/access/wire.rs SharedDoorPick
	sharedApp: string | null;
	registration: AppRegistration | null;
}

/** `POST /program/connections/direct`: a paste connect for one connection
 *  field of the program. */
// SYNC: ProgramConnectDirect <-> crates/weft-core/src/instance_door.rs ProgramConnectDirect
export interface ProgramConnectDirect extends DirectConnect {
	node: string;
	field: string;
}

/** A browser consent to start. */
// SYNC: ConsentStart <-> crates/weft-core/src/access/wire.rs BeginOAuth
export interface ConsentStart {
	spec: AccessSpecWire;
	door: Door;
	permissions: string[];
	// SYNC: sharedApp <-> crates/weft-core/src/access/wire.rs SharedDoorPick
	sharedApp: string | null;
	upgradeGrantId: string | null;
	registration: AppRegistration | null;
}

/** What a connect answers: the connection made, and `picked` when the
 *  transport also picked it for its field in the same request (the program
 *  door's does), with what that pick changed. Without `picked`, the page
 *  stores the pick itself (`pickConnection`). */
export interface ConnectAnswer extends CompletedConnect {
	picked?: ValuesChanged;
}

/** Everything a connect page asks of weft. */
export interface ConnectTransport {
	/** The connections of one service this page may pick from. */
	connections(service: string): Promise<GrantSummary[]>;
	/** Forget one connection for good. */
	forget(connection: GrantSummary): Promise<void>;
	/** Which doors the service offers right now. */
	doors(spec: AccessSpecWire): Promise<DoorsStatus>;
	connectDirect(request: DirectConnect): Promise<ConnectAnswer>;
	beginConsent(request: ConsentStart): Promise<StartedConsent>;
	/** A started consent's outcome, `null` while it is still pending.
	 *  Rejects once the consent is gone (unknown or expired), which ends
	 *  the wait. */
	consentOutcome(state: string): Promise<ConsentOutcome>;
	/** "Create it for me": mint an app from the recipe. Absent where the
	 *  page may not mint (an instance makes no apps for the program). */
	mintApp?(spec: AccessSpecWire, permissions: string[]): Promise<{ values: Record<string, string> }>;
	/** The shared door of a paste-less service (the runtime's own key).
	 *  Absent where it is not offered: an instance connects its own
	 *  account, never the program author's key. */
	allowsSharedKey: boolean;
	/** Open a page in the person's real browser (the consent page). */
	openExternal(url: string): void;
}

/** Everything a `remote_select` field asks of weft to fill its list and
 *  open its chooser. Each source is named by its position in the field's
 *  `sources` too: the editor sends the source itself (it signs with the
 *  author's connection it traced), an instance's page sends only the
 *  position (the instance door reads the source off the program, so an
 *  instance token asks for nothing the program does not declare). */
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

/** Where a website's admin page reaches the program door: its own server,
 *  which adds the connections key (`weftProgramPassThrough` in `server/`)
 *  behind the site's own admin check. Apart from `WEFT_PASS_THROUGH_PATH`,
 *  so the key is never added to a call a visitor's page makes.
 *  SYNC: WEFT_PROGRAM_PASS_THROUGH_PATH <-> src/server/passthrough.ts weftProgramPassThrough, the route the README mounts */
export const WEFT_PROGRAM_PASS_THROUGH_PATH = '/weft-admin';

/** What a settings page (`DoorSettings`) asks of a door: the fields it
 *  fills, storing and clearing one, the connect page of a connection field,
 *  and the lists and chooser a field fills from. The instance door fills an
 *  instance's fields; the program door fills the program's own connections.
 *  Every change answers the triggers set up again because they read the
 *  field, and the connections forgotten because nothing used them any more
 *  (only the program door forgets any). */
export interface FieldsDoor {
	fields(): Promise<FilledField[]>;
	/** Store `value` for `field` (a connection field takes the picked
	 *  connection's `{ id }`). Absent on the program door: every field it
	 *  fills is a connection, picked by the connect that stores its key. */
	save?(field: FilledField, value: NonNullable<unknown>): Promise<ValuesChanged>;
	/** Clear what `field` holds. */
	clear(field: FilledField): Promise<ValuesChanged>;
	/** The connect page's transport for a connection field. Its connect
	 *  answers `picked` when it picked the connection for the field too. */
	connectTransport(field: FilledField): ConnectTransport;
	/** The lists and chooser of a `remote_select` field. Absent where the
	 *  door fills none (the program door fills connections alone). */
	resources?(node: string, field: string): ResourceTransport;
}

/** Store a field's new value through its door: `null` clears it. A door
 *  with no `save` fills no field but by connecting, so storing anything
 *  else there is refused. */
export function fillField(door: FieldsDoor, field: FilledField, value: unknown): Promise<ValuesChanged> {
	if (value === null || value === undefined) return door.clear(field);
	if (!door.save) {
		return Promise.reject(new Error(`this door stores no value for '${field.node}.${field.field}': it only picks a connection by connecting it`));
	}
	return door.save(field, value);
}

/** Store a connection field's new pick through its door, after the
 *  connect page chose `handle` (`null`: none). `picked` is what the
 *  connect answered when it already picked the connection itself (the
 *  program door), and then nothing more is sent. */
export function pickConnection(
	door: FieldsDoor,
	field: FilledField,
	handle: { id: string } | null,
	picked: ValuesChanged | null,
): Promise<ValuesChanged> {
	if (handle !== null && picked !== null) return Promise.resolve(picked);
	return fillField(door, field, handle === null ? null : { id: handle.id });
}

/** How a door is reached: `base` is where its calls go (a site's own
 *  server by default, see each door), `fetcher` defaults to the global
 *  `fetch`, `opener` opens a page in the person's browser. */
export interface DoorOptions {
	base?: string;
	fetcher?: typeof fetch;
	opener?: (url: string) => void;
}

/** One door of the dispatcher, its routes under `<base>/<prefix>/`, with
 *  the token it presents (`null`: the pass-through in front of it adds
 *  one): what the instance door and the program door share. */
abstract class BearerDoor {
	private readonly base: string;
	private readonly fetcher: typeof fetch;
	protected readonly opener: (url: string) => void;

	constructor(
		private readonly token: string | null,
		private readonly prefix: 'instance' | 'program',
		defaultBase: string,
		options: DoorOptions,
	) {
		this.base = trimTrailingSlashes(options.base ?? defaultBase);
		this.fetcher = options.fetcher ?? ((...args) => fetch(...args));
		this.opener =
			options.opener ??
			((url) => {
				globalThis.open?.(url, '_blank', 'noopener');
			});
	}

	/** The error a refusal of this door throws. */
	protected abstract refused(message: string, status: number): Error;

	protected async call<T>(method: 'GET' | 'POST' | 'PUT' | 'DELETE', path: string, body?: unknown): Promise<T> {
		const resp = await this.fetcher(`${this.base}/${this.prefix}/${path}`, {
			method,
			headers: {
				...(this.token === null ? {} : { Authorization: `Bearer ${this.token}` }),
				...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
			},
			body: body === undefined ? undefined : JSON.stringify(body),
		});
		if (!resp.ok) {
			const text = await resp.text().catch(() => '');
			throw this.refused(text || `${method} /${this.prefix}/${path}: ${resp.status}`, resp.status);
		}
		if (resp.status === 204) return undefined as T;
		const text = await resp.text();
		return (text ? JSON.parse(text) : undefined) as T;
	}

	doors(spec: AccessSpecWire): Promise<DoorsStatus> {
		return this.call('POST', 'doors', { spec });
	}

	openExternal(url: string): void {
		this.opener(url);
	}
}

/** What the instance door refused, with the HTTP status it answered: 401
 *  for a token it does not know (or one that expired), 403 for a token
 *  that is not an instance token. */
export class InstanceDoorError extends Error {
	constructor(
		message: string,
		readonly status: number,
	) {
		super(message);
		this.name = 'InstanceDoorError';
	}

	/** The token acts inside no instance: it is not an instance token. */
	get notAnInstanceToken(): boolean {
		return this.status === 403;
	}
}

/** One instance of a program, at the dispatcher's instance door, with its
 *  instance token: everything a connect page needs, plus the fields that
 *  take a value of the instance's own, its values, and the lists and
 *  choosers those fields fill from. A page of a website reaches it through
 *  its own server, at `WEFT_PASS_THROUGH_PATH` (`weftPassThrough` in
 *  `server/`), so that is the default `base`; something that reaches the
 *  dispatcher itself (the browser extension) passes its address. */
export class InstanceDoor extends BearerDoor implements ConnectTransport, FieldsDoor {
	readonly allowsSharedKey = false;

	constructor(token: string, options: DoorOptions = {}) {
		super(token, 'instance', WEFT_PASS_THROUGH_PATH, options);
	}

	protected refused(message: string, status: number): Error {
		return new InstanceDoorError(message, status);
	}

	connections(service: string): Promise<GrantSummary[]> {
		return this.call('GET', `connections?service=${encodeURIComponent(service)}`);
	}

	forget(connection: GrantSummary): Promise<void> {
		return this.call('DELETE', `connections/${encodeURIComponent(connection.id)}`);
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

	/** Every field that takes a value of the instance's own, with what
	 *  was given. */
	fields(): Promise<FilledField[]> {
		return this.call('GET', 'fields');
	}

	/** Give values and clear others, all at once. A live trigger of the
	 *  instance that reads a changed value is set up again before this
	 *  answers, and the answer names it. */
	setValues(set: InstanceValueInput[], clear: InstanceFieldRef[] = []): Promise<ValuesChanged> {
		const body: ValuesRequest = { set, clear };
		return this.call('PUT', 'values', body);
	}

	save(field: FilledField, value: NonNullable<unknown>): Promise<ValuesChanged> {
		return this.setValues([{ node: field.node, field: field.field, value }]);
	}

	clear(field: FilledField): Promise<ValuesChanged> {
		return this.setValues([], [{ node: field.node, field: field.field }]);
	}

	/** An instance picks among its own connections: the door itself. Its
	 *  connect makes the connection and picks nothing, so the page saves
	 *  the pick. */
	connectTransport(): ConnectTransport {
		return this;
	}

	/** The lists and chooser of one field the instance fills. */
	resources(node: string, field: string): ResourceTransport {
		const lookup = <T,>(body: InstanceLookupRequest) => this.call<T>('POST', 'lookup', body);
		const picker = (body: InstancePickerRequest) => this.call<{ state: string; url: string }>('POST', 'picker', body);
		return {
			granted: (index) => lookup<LookupItem[]>({ node, field, source: index }),
			list: (index, _source, query, parents, cursor) => lookup<LookupPage>({ node, field, source: index, query, parents, cursor }),
			beginPicker: (index) => picker({ node, field, source: index }),
			pickerOutcome: (state) => this.call('GET', `connections/status?state=${encodeURIComponent(state)}`),
			openExternal: (url) => this.opener(url),
		};
	}
}

/** What the program door refused, with the HTTP status it answered: 401
 *  for a key it does not know (or one that expired), 403 for a token that
 *  is not a connections key. */
export class ProgramDoorError extends Error {
	constructor(
		message: string,
		readonly status: number,
	) {
		super(message);
		this.name = 'ProgramDoorError';
	}

	/** The token is not a connections key. */
	get notAConnectionsKey(): boolean {
		return this.status === 403;
	}
}

/** The program of one project, at the dispatcher's program door, with a
 *  connections key (`weft token mint --connections`): the connections the
 *  program itself uses on the install, each picked by pasting its key.
 *  The key replaces the program's keys, so it lives on the site's server and
 *  never in a browser: a page reaches this door through its own server, at
 *  `WEFT_PROGRAM_PASS_THROUGH_PATH` (`weftProgramPassThrough` in `server/`,
 *  which adds the key behind the site's own admin check), and passes no
 *  `key`. Code on a server that calls the dispatcher itself passes the
 *  dispatcher's address as `base` and the key as `key`. */
export class ProgramDoor extends BearerDoor implements FieldsDoor {
	constructor(options: DoorOptions & { key?: string } = {}) {
		super(options.key ?? null, 'program', WEFT_PROGRAM_PASS_THROUGH_PATH, options);
	}

	protected refused(message: string, status: number): Error {
		return new ProgramDoorError(message, status);
	}

	/** Every connection the program picks on the install, with what is
	 *  picked there. */
	fields(): Promise<FilledField[]> {
		return this.call('GET', 'connections');
	}

	/** Clear a field's pick; the connection it held is forgotten once
	 *  nothing on the install uses it, and the answer names it. */
	clear(field: FilledField): Promise<ValuesChanged> {
		return this.call('DELETE', `connections/${encodeURIComponent(field.node)}/${encodeURIComponent(field.field)}`);
	}

	/** The connect page of one connection field: paste a key, which is
	 *  stored as the program's and picked for the field in the same request,
	 *  so the connect answers `picked`. It lists no connections (a key sees
	 *  none but the ones it picked) and offers no browser sign-in. */
	connectTransport(field: FilledField): ConnectTransport {
		const noConsent = () =>
			Promise.reject(new ProgramDoorError('a browser sign-in is connected from the weft editor or with `weft connect`', 400));
		return {
			allowsSharedKey: false,
			connections: async () => [],
			// It lists no connection, so nothing is ever forgotten from it;
			// clearing the field is `clear(field)`.
			forget: () => Promise.reject(new ProgramDoorError('the program door lists no connection to forget; clear the field instead', 400)),
			doors: (spec) => this.doors(spec),
			connectDirect: async (request) => {
				const body: ProgramConnectDirect = { ...request, node: field.node, field: field.field };
				const { grant, ...picked } = await this.call<ProgramConnected>('POST', 'connections/direct', body);
				return { grant, picked };
			},
			beginConsent: noConsent,
			consentOutcome: noConsent,
			openExternal: (url) => this.opener(url),
		};
	}
}
