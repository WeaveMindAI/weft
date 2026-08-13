//! FirecrawlAccess: emits the connected Firecrawl access value. Pure
//! pass-through of the sealed connect handle; the bearer header is
//! applied by the authenticated client at call time.

weft::access_node!(FirecrawlAccessNode);
