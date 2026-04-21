import { jwtVerify } from 'jose';
import { env } from '$env/dynamic/private';

export interface DashboardTokenPayload {
	user_id: string;
	username: string;
	email?: string;
	name?: string;
	iss: string;
	aud: string;
	iat: number;
	exp: number;
}

export interface VerifyResult {
	valid: boolean;
	payload?: DashboardTokenPayload;
	error?: string;
}

export async function verifyDashboardToken(token: string): Promise<VerifyResult> {
	try {
		const dashboardSecret = env.DASHBOARD_EMBED_SECRET;
		if (!dashboardSecret) {
			return { valid: false, error: 'DASHBOARD_EMBED_SECRET is not set' };
		}
		const secret = new TextEncoder().encode(dashboardSecret);
		
		const { payload } = await jwtVerify(token, secret, {
			issuer: 'weavemind-website',
			audience: 'weavemind-dashboard',
		});
		
		return {
			valid: true,
			payload: payload as unknown as DashboardTokenPayload,
		};
	} catch (error) {
		const message = error instanceof Error ? error.message : 'Invalid token';
		return {
			valid: false,
			error: message,
		};
	}
}
