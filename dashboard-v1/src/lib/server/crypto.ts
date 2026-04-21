/**
 * Server-side encryption for sensitive node config fields (passwords, tokens, etc.)
 *
 * Uses AES-256-GCM with random nonce. The encryption key is loaded from
 * CREDENTIAL_ENCRYPTION_KEY env var (base64-encoded 32-byte key).
 *
 * For local dev, falls back to a deterministic dev key with a warning.
 */

import crypto from 'node:crypto';
import { env } from '$env/dynamic/private';
import { NODE_TYPE_CONFIG } from '$lib/nodes';

const ALGORITHM = 'aes-256-gcm';
const NONCE_SIZE = 12;
const AUTH_TAG_SIZE = 16;
const ENCRYPTED_PREFIX = 'enc:';

function getEncryptionKey(): Buffer {
	const keyB64 = env.CREDENTIAL_ENCRYPTION_KEY;
	if (keyB64) {
		const key = Buffer.from(keyB64, 'base64');
		if (key.length !== 32) {
			throw new Error('CREDENTIAL_ENCRYPTION_KEY must be exactly 32 bytes (256 bits)');
		}
		return key;
	}

	const isCloud = env.DEPLOYMENT_MODE?.toLowerCase() === 'cloud';
	if (isCloud) {
		throw new Error(
			'CREDENTIAL_ENCRYPTION_KEY is NOT SET but DEPLOYMENT_MODE=cloud. ' +
			'This is a CRITICAL SECURITY ERROR. ' +
			'Generate a key with: openssl rand -base64 32'
		);
	}

	console.warn('[crypto] CREDENTIAL_ENCRYPTION_KEY not set - using development key (local mode only)');
	return Buffer.from('weavemind-dev-encryption-key-32!', 'utf8');
}

function encrypt(plaintext: string): string {
	const key = getEncryptionKey();
	const nonce = crypto.randomBytes(NONCE_SIZE);
	const cipher = crypto.createCipheriv(ALGORITHM, key, nonce);

	const encrypted = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
	const authTag = cipher.getAuthTag();

	// Format: enc:<base64(nonce + ciphertext + authTag)>
	const combined = Buffer.concat([nonce, encrypted, authTag]);
	return ENCRYPTED_PREFIX + combined.toString('base64');
}

function decrypt(encryptedStr: string): string {
	if (!encryptedStr.startsWith(ENCRYPTED_PREFIX)) {
		return encryptedStr;
	}

	const key = getEncryptionKey();
	const combined = Buffer.from(encryptedStr.slice(ENCRYPTED_PREFIX.length), 'base64');

	if (combined.length < NONCE_SIZE + AUTH_TAG_SIZE) {
		throw new Error('Encrypted data too short');
	}

	const nonce = combined.subarray(0, NONCE_SIZE);
	const authTag = combined.subarray(combined.length - AUTH_TAG_SIZE);
	const ciphertext = combined.subarray(NONCE_SIZE, combined.length - AUTH_TAG_SIZE);

	const decipher = crypto.createDecipheriv(ALGORITHM, key, nonce);
	decipher.setAuthTag(authTag);

	return decipher.update(ciphertext) + decipher.final('utf8');
}

function isEncrypted(value: string): boolean {
	return typeof value === 'string' && value.startsWith(ENCRYPTED_PREFIX);
}

/**
 * Get the set of config keys that are sensitive for a given node type.
 * A field is sensitive if its type is 'password' in the node template.
 */
function getSensitiveKeys(nodeType: string): Set<string> {
	const template = NODE_TYPE_CONFIG[nodeType];
	if (!template?.fields) return new Set();
	return new Set(
		template.fields
			.filter((f) => f.type === 'password')
			.map((f) => f.key)
	);
}

/**
 * Encrypt sensitive config values in-place within a Weft code string.
 * Scans for node declarations (`id = NodeType {`), determines which
 * fields are sensitive for that node type, and replaces their plaintext
 * values with `enc:...` ciphertext directly in the string.
 *
 * Handles both quoted values (`key: "value"`) and triple-backtick
 * multiline values (`key: \`\`\`\n...\n\`\`\``).
 */
export function encryptWeftCode(weftCode: string): string {
	if (!weftCode) return weftCode;
	const lines = weftCode.split('\n');
	const result: string[] = [];

	let currentNodeType: string | null = null;
	let sensitiveKeys: Set<string> = new Set();
	let inHeredoc = false;
	let heredocKey = '';
	let heredocLines: string[] = [];
	let heredocIndent = '';
	let depth = 0;

	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];
		const trimmed = line.trim();

		// Triple-backtick multiline collection
		if (inHeredoc) {
			if (trimmed === '```') {
				const plaintext = heredocLines.join('\n');
				if (sensitiveKeys.has(heredocKey) && plaintext.length > 0 && !isEncrypted(plaintext)) {
					result.push(`${heredocIndent}${heredocKey}: "${encrypt(plaintext).replace(/"/g, '\\"')}"`);
				} else {
					result.push(`${heredocIndent}${heredocKey}: \`\`\``);
					for (const hl of heredocLines) result.push(hl);
					result.push(line);
				}
				inHeredoc = false;
				heredocLines = [];
				continue;
			}
			heredocLines.push(line);
			continue;
		}

		// Track brace depth for node blocks
		// Node declaration: `id = Type(...)` or `id = Type { config }`
		const nodeMatch = trimmed.match(/^[a-zA-Z_][a-zA-Z0-9_]*\s*=\s*([A-Z][a-zA-Z0-9]*).*\{(.*)$/);
		if (nodeMatch && !trimmed.endsWith('}')) {
			currentNodeType = nodeMatch[1];
			sensitiveKeys = getSensitiveKeys(currentNodeType);
			depth++;
			result.push(line);
			continue;
		}

		// Group block: `name = Group(...) {`
		const groupMatch = trimmed.match(/^[a-zA-Z_][a-zA-Z0-9_]*\s*=\s*Group.*\{$/);
		if (groupMatch) {
			depth++;
			result.push(line);
			continue;
		}

		// Closing brace
		if (trimmed === '}') {
			depth--;
			if (depth <= 0) {
				currentNodeType = null;
				sensitiveKeys = new Set();
				depth = 0;
			}
			result.push(line);
			continue;
		}

		// One-liner node: `id = NodeType { key: "val", key2: "val2" }`
		const oneLinerMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([A-Z][a-zA-Z0-9]*)\s*(?:"[^"]*")?\s*\{([^}]*)\}$/);
		if (oneLinerMatch) {
			const olNodeType = oneLinerMatch[2];
			const olSensitive = getSensitiveKeys(olNodeType);
			if (olSensitive.size > 0) {
				const body = oneLinerMatch[3];
				const encryptedBody = encryptInlineBody(body, olSensitive);
				if (encryptedBody !== body) {
					const prefix = line.substring(0, line.indexOf('{') + 1);
					const suffix = line.substring(line.lastIndexOf('}'));
					result.push(`${prefix}${encryptedBody}${suffix}`);
					continue;
				}
			}
			result.push(line);
			continue;
		}

		// Inside a node block: check for sensitive key-value pairs
		if (currentNodeType && sensitiveKeys.size > 0) {
			// Triple-backtick multiline start: `key: ``` `
			const heredocMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*```$/);
			if (heredocMatch) {
				heredocKey = heredocMatch[1];
				heredocIndent = line.substring(0, line.length - trimmed.length);
				inHeredoc = true;
				continue;
			}

			// Quoted value: `key: "value"`
			const kvMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*"((?:[^"\\]|\\.)*)"$/);
			if (kvMatch && sensitiveKeys.has(kvMatch[1])) {
				const key = kvMatch[1];
				const value = kvMatch[2].replace(/\\"/g, '"');
				if (value.length > 0 && !isEncrypted(value)) {
					const indent = line.substring(0, line.length - trimmed.length);
					result.push(`${indent}${key}: "${encrypt(value).replace(/"/g, '\\"')}"`);
					continue;
				}
			}
		}

		result.push(line);
	}

	return result.join('\n');
}

/**
 * Decrypt sensitive config values in-place within a Weft code string.
 * Finds `enc:...` values and replaces them with their decrypted plaintext.
 */
export function decryptWeftCode(weftCode: string): string {
	if (!weftCode) return weftCode;
	const lines = weftCode.split('\n');
	const result: string[] = [];

	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];
		// Fast path: skip lines without enc: prefix
		if (!line.includes(ENCRYPTED_PREFIX)) {
			result.push(line);
			continue;
		}

		// Quoted value containing enc:
		const kvMatch = line.match(/^(\s*[a-zA-Z_][a-zA-Z0-9_]*\s*:\s*)"(enc:[A-Za-z0-9+/=]+)"$/);
		if (kvMatch) {
			const encValue = kvMatch[2];
			try {
				const decrypted = decrypt(encValue);
				if (decrypted.includes('\n')) {
					// Multi-line value: re-emit as triple-backtick block
					const indent = kvMatch[1].match(/^(\s*)/)?.[1] ?? '';
					const key = kvMatch[1].trim().replace(/:\s*$/, '');
					result.push(`${indent}${key}: \`\`\``);
					result.push(decrypted);
					result.push(`${indent}\`\`\``);
				} else {
					result.push(`${kvMatch[1]}"${decrypted.replace(/"/g, '\\"')}"`);
				}
			} catch (e) {
				console.error(`[crypto] Failed to decrypt Weft value:`, e);
				result.push(line);
			}
			continue;
		}

		// One-liner body containing enc:
		const oneLinerMatch = line.match(/^(.+\{)(.+)(\}.*)$/);
		if (oneLinerMatch && oneLinerMatch[2].includes(ENCRYPTED_PREFIX)) {
			const body = oneLinerMatch[2];
			const decryptedBody = decryptInlineBody(body);
			result.push(`${oneLinerMatch[1]}${decryptedBody}${oneLinerMatch[3]}`);
			continue;
		}

		result.push(line);
	}

	return result.join('\n');
}

function encryptInlineBody(body: string, sensitiveKeys: Set<string>): string {
	const pairs = body.split(',');
	const result: string[] = [];
	for (const pair of pairs) {
		const trimPair = pair.trim();
		const kvMatch = trimPair.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*"((?:[^"\\]|\\.)*)"$/);
		if (kvMatch && sensitiveKeys.has(kvMatch[1])) {
			const value = kvMatch[2].replace(/\\"/g, '"');
			if (value.length > 0 && !isEncrypted(value)) {
				result.push(` ${kvMatch[1]}: "${encrypt(value).replace(/"/g, '\\"')}"`);
				continue;
			}
		}
		result.push(pair);
	}
	return result.join(',');
}

function decryptInlineBody(body: string): string {
	const pairs = body.split(',');
	const result: string[] = [];
	for (const pair of pairs) {
		const trimPair = pair.trim();
		const kvMatch = trimPair.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*"(enc:[A-Za-z0-9+/=]+)"$/);
		if (kvMatch) {
			try {
				const decrypted = decrypt(kvMatch[2]);
				result.push(` ${kvMatch[1]}: "${decrypted.replace(/"/g, '\\"')}"`);
				continue;
			} catch (e) {
				console.error(`[crypto] Failed to decrypt inline Weft value:`, e);
			}
		}
		result.push(pair);
	}
	return result.join(',');
}
