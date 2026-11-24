import { defineConfig } from '@playwright/test';
import os from 'os';
import path from 'path';

export default defineConfig({
  testDir: './tests',
  // Keep the delivery directory clean: test artifacts go to the system temp folder.
  outputDir: path.join(os.tmpdir(), 'pw-results-domain-events'),
  timeout: 60000,
  expect: { timeout: 10000 },
  fullyParallel: false,
  workers: 1,
  reporter: [['list']],
  use: {
    baseURL: 'http://localhost:5173',
    // Reuse the installed Edge; do not download Chromium.
    channel: 'msedge',
    trace: 'off',
    screenshot: 'off',
    viewport: { width: 1440, height: 900 },
  },
  webServer: [
    { command: 'node mock-server/index.js', url: 'http://localhost:3001/api/v1/health', reuseExistingServer: true, timeout: 30000 },
    { command: 'npm run dev', url: 'http://localhost:5173', reuseExistingServer: true, timeout: 30000 },
  ],
});
