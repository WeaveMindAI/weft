import urllib.request
assert "__weft_image__" not in photo, "the marker arrives unwrapped"
with urllib.request.urlopen(photo["url"], timeout=30) as response:
    fetched = response.read()
return {
    "body": {
        "filename": photo["filename"],
        "mimeType": photo["mimeType"],
        "fetched": len(fetched),
        "declared": photo["sizeBytes"],
    },
    "same": photo,
}
