import { browser } from '$app/environment';
import { authFetch, api } from '$lib/config';
import { STORAGE_KEYS } from '$lib/utils';
import type { FileRef } from '$lib/types';

/**
 * Check if we're running in cloud mode (managed uploads available).
 * Cloud mode = auth token exists in sessionStorage (set by website iframe auth).
 */
export function isCloudMode(): boolean {
	if (!browser) return false;
	return !!sessionStorage.getItem(STORAGE_KEYS.authToken);
}

/** Progress callback: loaded bytes, total bytes */
export type UploadProgressCallback = (loaded: number, total: number) => void;

/** Track active uploads to warn on page unload */
let activeUploads = 0;

function onBeforeUnload(e: BeforeUnloadEvent) {
	if (activeUploads > 0) {
		e.preventDefault();
	}
}

if (browser) {
	window.addEventListener('beforeunload', onBeforeUnload);
}

/**
 * Upload file bytes to a presigned URL using XHR (supports progress).
 */
function putWithProgress(
	url: string,
	body: File | Blob,
	mimeType: string,
	onProgress?: UploadProgressCallback,
): Promise<void> {
	return new Promise((resolve, reject) => {
		const xhr = new XMLHttpRequest();
		xhr.open('PUT', url);
		xhr.setRequestHeader('Content-Type', mimeType);

		if (onProgress) {
			xhr.upload.onprogress = (e) => {
				if (e.lengthComputable) {
					onProgress(e.loaded, e.total);
				}
			};
		}

		xhr.onload = () => {
			if (xhr.status >= 200 && xhr.status < 300) {
				resolve();
			} else {
				reject(new Error(`Upload to storage failed: ${xhr.status}`));
			}
		};

		xhr.onerror = () => reject(new Error('Upload network error'));
		xhr.onabort = () => reject(new Error('Upload aborted'));

		xhr.send(body);
	});
}

/**
 * Upload a file to storage (works in both local and cloud mode).
 * Flow: POST /api/v1/files → get upload_url → PUT bytes to upload_url
 */
export async function uploadBlob(
	file: File | Blob,
	filename: string,
	mimeType: string,
	onProgress?: UploadProgressCallback,
): Promise<FileRef> {
	activeUploads++;
	try {
		// 1. Create file record
		const createRes = await authFetch(api.createFile(), {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				filename,
				mimeType,
				sizeBytes: file.size,
			}),
		});

		if (!createRes.ok) {
			const err = await createRes.text();
			throw new Error(`File create failed: ${err}`);
		}

		const { file_id, upload_url, url } = await createRes.json();

		// 2. PUT bytes to upload_url (presigned R2 URL in cloud, local endpoint in open source)
		await putWithProgress(upload_url, file, mimeType, onProgress);

		return {
			file_id,
			url,
			filename,
			mime_type: mimeType,
			size_bytes: file.size,
		};
	} finally {
		activeUploads--;
	}
}

export interface CloudFile {
	id: string;
	filename: string;
	mime_type: string;
	size_bytes: number;
	created_at: string;
}

/** Fetch the user's uploaded files from cloud storage. */
export async function listCloudFiles(): Promise<CloudFile[]> {
	const res = await authFetch(api.listFiles());
	if (!res.ok) return [];
	return res.json();
}

/** Resolve a file ID to a FileRef with a fresh download URL. */
export async function resolveCloudFile(file: CloudFile): Promise<FileRef> {
	const res = await authFetch(api.getFile(file.id));
	if (!res.ok) throw new Error('Failed to get download URL');
	const { url } = await res.json();
	return {
		file_id: file.id,
		url,
		filename: file.filename,
		mime_type: file.mime_type,
		size_bytes: file.size_bytes,
	};
}

/**
 * Validate a pasted URL. Only http/https URLs are accepted.
 * Returns a FileRef with file_id undefined (external URL, not cloud-managed).
 */
export function validateExternalUrl(url: string): FileRef | null {
	const trimmed = url.trim();
	if (!trimmed) return null;

	// Reject data: URIs
	if (trimmed.startsWith('data:')) {
		return null;
	}

	// Only allow https (http is rejected to prevent SSRF to internal services)
	if (!trimmed.startsWith('https://')) {
		return null;
	}

	// Extract filename from URL path
	let filename = 'unknown';
	try {
		const parsed = new URL(trimmed);
		const pathParts = parsed.pathname.split('/');
		const lastPart = pathParts[pathParts.length - 1];
		if (lastPart) filename = decodeURIComponent(lastPart);
	} catch {
		// Invalid URL
		return null;
	}

	// Guess mime type from extension
	const mimeType = guessMimeType(filename);

	return {
		url: trimmed,
		filename,
		mime_type: mimeType,
		size_bytes: 0, // Unknown for external URLs
	};
}

/**
 * Compress an audio file to 16kHz mono WAV for speech processing.
 */
export async function compressAudio(file: File): Promise<{ blob: Blob; displaySize: string }> {
	const arrayBuffer = await file.arrayBuffer();
	const audioContext = new AudioContext();
	try {
		const audioBuffer = await audioContext.decodeAudioData(arrayBuffer);

		const targetSampleRate = 16000;
		const offlineContext = new OfflineAudioContext(
			1, // mono
			audioBuffer.duration * targetSampleRate,
			targetSampleRate,
		);

		const source = offlineContext.createBufferSource();
		source.buffer = audioBuffer;
		source.connect(offlineContext.destination);
		source.start();

		const renderedBuffer = await offlineContext.startRendering();
		const wavBlob = audioBufferToWav(renderedBuffer);

		const displaySize = `${(wavBlob.size / 1024).toFixed(0)}KB`;
		return { blob: wavBlob, displaySize };
	} finally {
		await audioContext.close();
	}
}

/**
 * Convert an AudioBuffer to a WAV Blob.
 */
function audioBufferToWav(buffer: AudioBuffer): Blob {
	const numChannels = buffer.numberOfChannels;
	const sampleRate = buffer.sampleRate;
	const format = 1; // PCM
	const bitDepth = 16;

	const bytesPerSample = bitDepth / 8;
	const blockAlign = numChannels * bytesPerSample;

	const samples = buffer.getChannelData(0);
	const dataLength = samples.length * bytesPerSample;
	const bufferLength = 44 + dataLength;

	const arrayBuffer = new ArrayBuffer(bufferLength);
	const view = new DataView(arrayBuffer);

	const writeString = (offset: number, str: string) => {
		for (let i = 0; i < str.length; i++) {
			view.setUint8(offset + i, str.charCodeAt(i));
		}
	};

	writeString(0, 'RIFF');
	view.setUint32(4, bufferLength - 8, true);
	writeString(8, 'WAVE');
	writeString(12, 'fmt ');
	view.setUint32(16, 16, true);
	view.setUint16(20, format, true);
	view.setUint16(22, numChannels, true);
	view.setUint32(24, sampleRate, true);
	view.setUint32(28, sampleRate * blockAlign, true);
	view.setUint16(32, blockAlign, true);
	view.setUint16(34, bitDepth, true);
	writeString(36, 'data');
	view.setUint32(40, dataLength, true);

	let offset = 44;
	for (let i = 0; i < samples.length; i++) {
		const sample = Math.max(-1, Math.min(1, samples[i]));
		view.setInt16(offset, sample < 0 ? sample * 0x8000 : sample * 0x7FFF, true);
		offset += 2;
	}

	return new Blob([arrayBuffer], { type: 'audio/wav' });
}

const MIME_MAP: Record<string, string> = {
	// Audio
	mp3: 'audio/mpeg', ogg: 'audio/ogg', wav: 'audio/wav', flac: 'audio/flac',
	m4a: 'audio/mp4', aac: 'audio/aac', opus: 'audio/opus', webm: 'audio/webm',
	// Video
	mp4: 'video/mp4', mov: 'video/quicktime', avi: 'video/x-msvideo',
	mkv: 'video/x-matroska', wmv: 'video/x-ms-wmv',
	// Image
	png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', webp: 'image/webp',
	gif: 'image/gif', bmp: 'image/bmp', svg: 'image/svg+xml', avif: 'image/avif',
	// Document
	pdf: 'application/pdf', csv: 'text/csv', txt: 'text/plain', json: 'application/json',
	zip: 'application/zip',
};

function guessMimeType(filename: string): string {
	const ext = filename.split('.').pop()?.toLowerCase() || '';
	return MIME_MAP[ext] || 'application/octet-stream';
}

/**
 * Shared handler for blob field file uploads (drop or file input).
 * Handles: audio compression, progress tracking, error toasting.
 * Used by ConfigPanel, ProjectNode, and RunnerView.
 */
export async function handleBlobFieldUpload(
	file: File,
	acceptHint: string | undefined,
	onUpdate: (ref: FileRef | null) => void,
	onError: (msg: string) => void,
): Promise<void> {
	try {
		const isAudio = file.type.startsWith('audio/') || acceptHint?.includes('audio');
		let uploadFile: File | Blob = file;
		let filename = file.name;
		let mimeType = file.type || 'application/octet-stream';

		if (isAudio) {
			onUpdate({ filename: `Compressing ${file.name}...`, url: '', mime_type: '', size_bytes: 0 });
			const { blob } = await compressAudio(file);
			uploadFile = blob;
			filename = file.name.replace(/\.[^.]+$/, '.wav');
			mimeType = 'audio/wav';
		}

		onUpdate({ filename: `Uploading ${filename}...`, url: '', mime_type: mimeType, size_bytes: 0 });

		const ref = await uploadBlob(uploadFile, filename, mimeType, (loaded, total) => {
			onUpdate({ filename: `Uploading ${filename} (${formatBytes(loaded)} / ${formatBytes(total)})`, url: '', mime_type: mimeType, size_bytes: 0 });
		});

		onUpdate(ref);
	} catch (err) {
		onError(err instanceof Error ? err.message : 'Unknown error');
		onUpdate(null);
	}
}

/** Format bytes to human-readable string */
export function formatBytes(bytes: number): string {
	if (bytes >= 1048576) return `${(bytes / 1048576).toFixed(1)}MB`;
	if (bytes >= 1024) return `${(bytes / 1024).toFixed(0)}KB`;
	return `${bytes}B`;
}
