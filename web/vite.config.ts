/// <reference types="vitest/config" />
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const target = process.env.API_TARGET ?? 'http://localhost:8080';
const proxy = { '/api': { target }, '/sim': { target } };

export default defineConfig({
  plugins: [react()],
  server: { proxy },
  preview: { proxy },
  test: { environment: 'node' },
});
