import { fileURLToPath } from 'node:url';

import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: [
      {
        find: /^graphql$/,
        replacement: fileURLToPath(new URL('./node_modules/graphql/index.js', import.meta.url)),
      },
    ],
    dedupe: ['graphql'],
  },
  test: {
    server: {
      deps: {
        external: ['graphql', /^@graphql-tools\//, 'graphql-scalars', 'graphql-ws'],
      },
    },
  },
});
