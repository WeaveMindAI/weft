FROM node:24-alpine

RUN apk add --no-cache git
# Node 24 = active LTS in 2026 (Node 22 enters EOL April 2027).
# Baileys requires Node >= 20 so 24 is well within range. pnpm 11
# (April 2026) requires Node >= 22 and is the current stable.
# Both versions pinned: `pnpm@latest` in a Dockerfile is a build-
# reproducibility bug regardless of which version it points at
# right now.
RUN corepack enable && corepack prepare pnpm@11.0.0 --activate

WORKDIR /app

COPY . .

ENV CI=true
RUN rm -rf node_modules && pnpm install --prod

# PORT is set by the bridge node's Rust spec (BRIDGE_PORT = 8090)
# whenever weft runs the image; the JS default is 8090 too, for a
# plain `docker run` by hand. No Dockerfile ENV, so the spec stays
# the one source of truth.
ENV AUTH_DIR=/data/auth

EXPOSE 8090

CMD ["node", "src/index.js"]
