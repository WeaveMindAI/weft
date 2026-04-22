# Field Editor + Form Builder + Blob Upload Parity

**v1 sources**:
- `dashboard-v1/src/lib/utils/field-editor.svelte.ts` (99 lines)
- `dashboard-v1/src/lib/utils/form-field-specs.ts` (126 lines)
- `dashboard-v1/src/lib/utils/blob-upload.ts` (326 lines)
- `dashboard-v1/src/lib/components/project/BlobField.svelte` (117 lines)

## `createFieldEditor(debounceMs = 2000)` (already ported)

Debounced value controller. Prevents reactive store updates from
clobbering the user's in-progress typing.

Pattern per field instance:
```ts
const editor = createFieldEditor(2000);

// In template:
value={editor.display(key, storeValue)}
onfocus={() => editor.focus(key, storeValue)}
oninput={(e) => editor.input(e.currentTarget.value, key, (v) => updateConfig(key, v))}
onblur={() => editor.blur(key, (v) => updateConfig(key, v))}
```

Invariants:
- On focus: snapshot store value to local state, stop any timer.
- On input: update local state only, schedule debounced save.
- On blur: flush immediately, reset local state.
- `flush()`: call before destructive actions (Run Project) to push
  any pending debounce.
- `display(key, storeValue)`: if this key is the active one,
  return local value; else return store value. Means the input
  shows the user's typed text, not a stale reactive cycle.

Ported at `extension-vscode/src/webview/utils/field-editor.svelte.ts`.

## Field kinds (FieldDefinition.type)

v1's `NODE_TYPE_CONFIG` catalog declares fields per node. Each
field has:

```ts
{
  key: string;          // config key + port name for configurable
  label: string;
  type: 'text' | 'textarea' | 'code' | 'select' | 'multiselect'
      | 'checkbox' | 'password' | 'api_key' | 'form_builder' | 'blob';
  placeholder?: string;
  description?: string;
  options?: string[];   // for select / multiselect
  accept?: string;      // for blob
}
```

Rendering per kind is covered in `project-node.md` line-by-line.

## Form builder spec (form-field-specs.ts)

A form builder is a field of type `form_builder`. It lets the
user define a dynamic list of sub-fields for the node's runtime
form (HumanQuery, etc). Each sub-field is a `FormFieldDef`:

```ts
{
  fieldType: string;   // spec id, looked up in NODE_TYPE_CONFIG[type].formFieldSpecs
  key: string;         // user-chosen; becomes the port name
  render?: FormFieldRender;
  config?: Record<string, unknown>;  // spec-specific extras (e.g. options[])
  required?: boolean;  // default true
}
```

### `FormFieldSpec`

Per fieldType:
- `requiredConfig: string[]`: config keys the user must fill
  (e.g. `['options']` for select).
- `optionalConfig: string[]`.
- `addsInputs: PortDefinition[]`: ports added when this field is
  present. Names use `{key}` template, resolved to
  `port.name.replace('{key}', f.key)`.
- `addsOutputs: PortDefinition[]`: same for outputs.

### Auto type-vars

Port types can contain the sentinel `T_Auto`. When resolving,
every `T_Auto` becomes `T__{key}` (scoped to that field's key).
This means each form-field port gets an independent TypeVar that
unifies with the edge-connected value's type, but different
fields don't share the TypeVar.

Example: HumanQuery's "select" field spec has
`addsOutputs: [port('{key}', 'T_Auto')]`. If the user adds two
select fields with keys "color" and "size", they get independent
output ports `color: T__color` and `size: T__size`.

### `deriveInputsFromFields` / `deriveOutputsFromFields`

Walk the field list, look up spec, resolve port names and types
with the key substitutions, return flat port arrays. These are
merged into the node's inputs/outputs at build time
(`validateAndBuild` line 4184-4208 calls them when
`hasFormSchema` is true).

### When the form builder field is edited

ProjectNode's `updateConfig` has a special branch (line 342-351):
```ts
if (typeConfig.features?.hasFormSchema && key === 'fields') {
  const fields = value as FormFieldDef[];
  data.onUpdate({
    config: newConfig,
    inputs: deriveInputsFromFields(fields, nodeFormSpecMap),
    outputs: deriveOutputsFromFields(fields, nodeFormSpecMap),
  });
}
```

Editing the field list triggers a port rebuild. The editor
surgically rewrites the weft source via `weftUpdatePorts` to
persist the new ports.

### Form builder UI (ProjectNode.svelte line 969-1039)

- Existing fields render as rows with `fieldType + key + × remove`.
- "+ Add field" button opens an inline form:
  - fieldType dropdown (from `nodeFormFieldSpecs`).
  - key input (sanitized via `.replace(/\s+/g, '_')`).
  - options sub-builder IF the spec's `requiredConfig.includes('options')`.
  - Cancel / Add buttons.
- Key collision detection via `deriveInputsFromFields` /
  `deriveOutputsFromFields` comparison → toast error `"Port name
  conflict: \"X\" already exists..."`.

## BlobField pipeline (v1 frontend)

BlobField.svelte owns the UI; blob-upload.ts owns the network +
transform.

### UI states (BlobField.svelte)

1. **Uploading** (`fileRef.filename && !fileRef.url && !fileRef.file_id`):
   Shows the progressive message set by `handleBlobFieldUpload`
   (e.g. `"Uploading foo.mp3 (1.2MB / 3.5MB)"`).
   Class: `animate-pulse`, `bg-muted px-2 py-1.5`.
2. **Uploaded** (`fileRef.url || fileRef.file_id`):
   Shows filename + size (KB if <1MB, MB otherwise) + × delete.
3. **Empty**: hidden file input, drag-drop label, browse button,
   URL paste input.

### `handleBlobFieldUpload(file, acceptHint, onUpdate, onError)`
(blob-upload.ts line 288-319)

```ts
const isAudio = file.type.startsWith('audio/') || acceptHint?.includes('audio');

if (isAudio) {
  onUpdate({ filename: `Compressing ${file.name}...`, url: '', ... });
  const { blob } = await compressAudio(file);  // decode → 16kHz mono WAV
  uploadFile = blob;
  filename = file.name.replace(/\.[^.]+$/, '.wav');
  mimeType = 'audio/wav';
}

onUpdate({ filename: `Uploading ${filename}...`, url: '', ... });

const ref = await uploadBlob(uploadFile, filename, mimeType, (loaded, total) => {
  onUpdate({ filename: `Uploading ${filename} (${formatBytes(loaded)} / ${formatBytes(total)})`, url: '', ... });
});

onUpdate(ref);  // final FileRef with url + file_id set
```

Intermediate `onUpdate` calls with partial FileRef shape make
the UI show the progress message (in the Uploading state). Only
the final call has a real `url`/`file_id`.

### Audio compression (line 186-261)

`compressAudio(file)` uses Web Audio API:
1. `decodeAudioData(file.arrayBuffer())`.
2. Render via `OfflineAudioContext(1 channel, 16000 sample rate)`.
3. `audioBufferToWav(renderedBuffer)` → Blob via manual RIFF/WAVE
   encoding (44-byte header + int16 PCM samples).

This is a v1 optimization for speech-processing nodes
(Transcribe, etc). A 3MB mp3 becomes a ~100KB 16kHz mono WAV
before upload.

### `uploadBlob(file, filename, mimeType, onProgress)` (line 72-111)

1. POST `/api/v1/files` with `{ filename, mimeType, sizeBytes }`
   → server returns `{ file_id, upload_url, url }`.
2. `putWithProgress(upload_url, file, mimeType, onProgress)` —
   XHR with `upload.onprogress` → loaded/total callback.
3. Returns `FileRef { file_id, url, filename, mime_type, size_bytes }`.
4. Tracks `activeUploads++` so `beforeunload` can warn the user
   if they try to close the tab mid-upload.

### `validateExternalUrl(url)` (line 146-181)

Pasted URL:
- Reject `data:` URIs.
- Require `https://` prefix (http rejected for SSRF to internal services).
- Extract filename from URL path (last segment, decodeURIComponent).
- `guessMimeType(filename)` uses `MIME_MAP` extension table
  (audio: mp3/ogg/wav/…, video: mp4/mov/…, image: png/jpg/…,
  document: pdf/csv/txt/json/zip).
- Return `FileRef { url, filename, mime_type, size_bytes: 0 }`.
  `file_id` undefined = external URL, not cloud-managed.

### `listCloudFiles()` + `resolveCloudFile(file)` (line 122-140)

Feed the FilePicker modal (BlobField's "Browse uploaded files"
button). `listCloudFiles` calls `/api/v1/files`, returns
CloudFile[]. `resolveCloudFile` asks for a fresh download URL
for a specific file.

### `FileRef` shape

```ts
{
  file_id?: string;   // present for cloud-managed files
  url: string;
  filename: string;
  mime_type: string;
  size_bytes: number;
}
```

### Global `beforeunload` warning (line 21-29)

```ts
let activeUploads = 0;
function onBeforeUnload(e: BeforeUnloadEvent) {
  if (activeUploads > 0) e.preventDefault();
}
window.addEventListener('beforeunload', onBeforeUnload);
```

Upload increments the counter; upload end (success or fail)
decrements. Page close with pending uploads prompts the user.

### `blob-drag-over` class

When dragging a file over the container, `ondragover` adds the
class:
```css
:global(.blob-drag-over) {
  outline: 2px solid rgb(96, 165, 250);
  outline-offset: -2px;
  border-radius: 0.375rem;
  background-color: rgba(96, 165, 250, 0.08);
}
```
Removed on `ondragleave` / `ondrop`.

## v2 port plan: BlobField

Deferred to Phase B. See `weavemind/docs/v2-cloud-design.md`
section 10.13.

**What to ship in phase A**: URL-paste only, `validateExternalUrl`
logic. Skip the upload pipeline entirely.

**What Phase B needs**:
- A dispatcher `/upload` endpoint that POST multipart → PUT to R2
  with SSE-C per-file key → returns FileRef.
- The client-side `uploadBlob` + `handleBlobFieldUpload` pipeline.
- The FilePicker browse-uploaded-files modal.
- The audio compression helper (only relevant if we have
  speech-processing nodes; could port at the same time).
- `beforeunload` warning for in-flight uploads.

## v2 port status

- `field-editor.svelte.ts` ported verbatim: DONE.
- `FieldEditor.svelte` covers all kinds except `code` (CodeMirror
  deferred for bundle size reasons) and `form_builder` (stub;
  needs full form builder + port auto-derivation).
- `form-field-specs.ts` not yet ported. Needed to make
  `form_builder` work: derive inputs/outputs when the user edits
  the field list.
- `blob-upload.ts` + `BlobField.svelte`: **deferred to Phase B**.
  v2 webview currently accepts pasted URLs only.

### What's still missing in my port
- `code` kind still renders as a regular textarea; a proper CodeEditor
  is deferred (CodeMirror bundle cost).
- `form_builder` stub says "edit .weft source directly"; needs the
  full builder UI with fieldType dropdown, key validation, options
  sub-builder.
- BlobField URL-paste works; upload does not.

### Done
- textareaHeights persistence via ResizeObserver.
- `nowheel` class added on focus, removed on blur.
