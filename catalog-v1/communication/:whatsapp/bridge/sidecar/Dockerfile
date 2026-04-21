FROM node:20-alpine

RUN apk add --no-cache git
RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /app

COPY sidecars/whatsapp-bridge .

ENV CI=true
RUN rm -rf node_modules && pnpm install --prod

ENV PORT=8090
ENV AUTH_DIR=/data/auth

EXPOSE 8090

CMD ["node", "src/index.js"]
