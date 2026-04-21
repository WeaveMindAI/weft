# Extension Downloads

This folder contains the built extension packages for different browsers.

## Files

- `weavemind-chrome.zip` - For Chrome (unpacked extension)
- `weavemind-edge.zip` - For Edge (unpacked extension)
- `weavemind-firefox.xpi` - For Firefox (signed add-on)

## Building & Deploying

Use the build script in the weavemind directory:

```bash
cd /path/to/weavemind
./build-extension.sh          # Build and deploy all browsers
./build-extension.sh --bump   # Bump version first, then build
```

This script:
1. Builds Chrome, Edge, and Firefox extensions
2. Signs the Firefox extension via Mozilla API
3. Copies all files to this folder

## First-Time Setup

1. Install web-ext globally: `pnpm install -g web-ext`
2. Create Mozilla developer account: https://addons.mozilla.org/developers/
3. Generate API keys: https://addons.mozilla.org/en-US/developers/addon/api/key/
4. Create `weavemind/.env.extension` with your keys:
   ```
   WEB_EXT_API_KEY=user:XXXXX:XXX
   WEB_EXT_API_SECRET=your-secret-here
   ```
