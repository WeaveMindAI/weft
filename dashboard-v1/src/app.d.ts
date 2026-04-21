// See https://svelte.dev/docs/kit/types#app.d.ts
// for information about these interfaces
declare global {
	namespace App {
		// interface Error {}
		interface Locals {
			/** Authenticated user, populated by the auth middleware in
			 *  hooks.server.ts. Always present on `/api/*` routes
			 *  (except `/api/validate-token`, which IS the auth
			 *  handshake) because the middleware short-circuits with
			 *  401 if the JWT is missing or invalid. In local OSS
			 *  mode the middleware injects `{ id: 'local',
			 *  username: 'local' }` so single-user dev keeps working
			 *  without a JWT infrastructure. */
			user?: {
				id: string;
				username: string;
			};
		}
		// interface PageData {}
		// interface PageState {}
		// interface Platform {}
	}
}

export {};
