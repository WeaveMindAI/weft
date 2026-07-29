//! S3Access: emits the connected bucket account's access value. Pure
//! pass-through of the sealed connect handle; every request through
//! the access is SigV4-signed by the authenticated client, aimed at
//! the stored endpoint (S3, R2, MinIO, any S3-compatible store).

weft::access_node!(S3AccessNode);
