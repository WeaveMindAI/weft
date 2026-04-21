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

## BlobField (v1) uses cloud API

BlobField.svelte:
- Drag-drop file → `handleBlobFieldUpload(file, accept, onUpdate, onError)`.
- Browse button → `filePickerOpen = true`.
- URL paste → `validateExternalUrl` (rejects `data:`).

`uploadBlob` (blob-upload.ts):
1. POST `/api/v1/files` with filename, mimeType, sizeBytes → returns
   `{ file_id, upload_url, url }`.
2. PUT bytes to `upload_url` (presigned R2 URL in cloud, local
   endpoint in dev).
3. Call `data.onUpdate({ config: { [key]: { file_id, filename,
   url, size_bytes, mime_type } } })`.

The resulting `FileRef`:
```ts
{
  file_id: string;
  filename: string;
  url: string;         // public / presigned
  size_bytes: number;
  mime_type: string;
}
```

**Deferred to Phase B in v2 extension.** See
`weavemind/docs/v2-cloud-design.md` section 10.13.

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
- Textarea height persistence via ResizeObserver + `textareaHeights`
  config map. The config value exists but my FieldEditor doesn't
  read or write it.
- `nowheel` class management on code/textarea focus (for wheel
  containment).
- `code` kind renders as a regular textarea; needs CodeEditor
  component (CodeMirror-based).
- `form_builder` stub says "edit .weft source directly"; needs
  the full builder UI with fieldType dropdown, key validation,
  options sub-builder.
- BlobField URL-paste works; upload does not.
