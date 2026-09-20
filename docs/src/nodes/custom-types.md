# Custom types

If several nodes pass the same kind of record around, give it a name.
A `SupportTicket` input tells you more than `JsonDict`: it says which
record this node expects, and the declaration says which fields belong in it.

Add a `types` block to a node's metadata or its package's shared metadata:

```json
"types": {
  "SupportTicket": "{ id: String, question: String, screenshot?: Image }",
  "TicketBatch": "List[SupportTicket]"
}
```

Then use the name on a port:

```json
{
  "name": "ticket",
  "type": "SupportTicket",
  "required": true
}
```

Put the `types` block at the top level of the metadata. Add the port object
to the `inputs` array.

## A name is part of the contract

A `SupportTicket` output can feed a `SupportTicket` input.
An unnamed object does not become a ticket merely because its fields look
right. To accept an object from an untyped source, use `Cast` and declare
the target type. The conversion checks its fields at run time.

A named value can flow out to a compatible structural type.
`SupportTicket` can feed its record shape or `JsonDict`;
`TicketBatch` can feed a compatible list. A list is not a `JsonDict`.

The check establishes the declared structure. It does not prove that
ticket `"T-42"` exists or that its question was answered correctly.

For casts and source-level type declarations, read
[Types](../language/types.md).

## Where the declaration is visible

Metadata types are available throughout the project's catalog and in its
`.weft` source. Put the declaration beside the node or package that owns
the concept.

Two packages can repeat the same name with an identical body. Different
bodies under one name fail catalog loading, so two implementations cannot
quietly disagree about what `SupportTicket` contains.

Records reject undeclared fields. Use `?` for a field that may be absent,
and include every field you intend to carry. For example, adding an
`assignee` property to a value requires adding it to the declaration too.

If you use [package defaults](metadata.md#package-defaults), remember that
a member's own `types` object replaces the inherited object as a whole.

## Files inside a record

The optional `screenshot` above is an `Image` field. When it holds a
stored-file reference, the graph carries that reference instead of copying
the image bytes into the record.

A provider may need a URL or inline data instead. The storage helpers can
convert all file positions in a typed value, including files nested inside
lists and records. You still need to call the helper at the provider
boundary; declaring a type does not make the request for you.

For example, inside a node that declares a `ticket` output:

```rust
use weft::storage::{StorageScope, media::ExternalizePolicy};

let ty = ctx.output_type("ticket")
    .ok_or_else(|| weft::node_error("The ticket output type is missing"))?;
let storage = ctx.storage(StorageScope::Execution);

let provider_value = storage
    .externalize(&ticket, &ty, ExternalizePolicy::urls())
    .await?;
```

Here `ticket` is the JSON value you are preparing to send.
`urls()` requests public links for stored files and uses inline data when
no public link can be served. Use `ExternalizePolicy::inline()` when the
consumer requires inline data.

The converted object is for that provider call. Its file fields now hold
external strings, so keep the original typed value for the graph.

If a response contains media URLs or data URLs in the same declared shape,
bring those files into storage:

```rust
let stored_ticket = storage.internalize(&response, &ty, None).await?;
ctx.pulse_downstream(
    NodeOutput::new().set("ticket", stored_ticket),
).await?;
```

`internalize` stores raw media and replaces those positions with file
references; already-stored references pass through. It does not validate
unrelated response fields. Your node still needs to check that the provider
returned the result it promised.

Here `None` leaves newly stored files eligible for cleanup after the
execution. Pass `Some(KeepTtl::Default)` to keep them for the default
retention period; import `KeepTtl` from `weft::storage`.

For retention and conversion policies, read [Storage](storage.md).
For an existing example with nested media, read the
[chat type declarations](https://github.com/WeavemindAI/weft/blob/mvp/catalog/ai/llm/metadata.json).
