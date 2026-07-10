# node >= 22 required: pnpm 11 uses node:sqlite internally
FROM node:22-alpine

RUN apk add --no-cache git
# Pinned: pnpm@latest broke builds when v11 made unapproved
# dependency build scripts a hard install error
RUN corepack enable && corepack prepare pnpm@11.11.0 --activate

WORKDIR /app

COPY sidecars/whatsapp-bridge .

ENV CI=true
RUN rm -rf node_modules && pnpm install --prod --frozen-lockfile

ENV PORT=8090
ENV AUTH_DIR=/data/auth

EXPOSE 8090

CMD ["node", "src/index.js"]
