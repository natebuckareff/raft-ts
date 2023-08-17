FROM node:18-alpine
ARG PNPM_VERSION="8.6.3"
RUN npm install --location=global pnpm@${PNPM_VERSION}
WORKDIR /app
RUN pnpm config set store-dir /root/.local/share/pnpm/store/ --global
ENTRYPOINT ["pnpm", "run", "dev"]