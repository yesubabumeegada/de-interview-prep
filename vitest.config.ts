import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: false,
    environment: 'node',
    include: ['tests/**/*.test.ts'],
  },
  resolve: {
    alias: {
      '@services': './src/services',
      '@components': './src/components',
      '@layouts': './src/layouts',
      '@styles': './src/styles',
      '@content': './content',
      '@utils': './src/utils',
    },
  },
});
