// Render bench/video/explainer.html to frames and an MP4 (needs Playwright's Chromium and ffmpeg).
//   node scripts/render-video.mjs sheet   # one contact sheet of sample frames (bench/video/sheet.png)
//   node scripts/render-video.mjs video   # bench/video/ringrt-explainer.mp4 at 24 fps, 1280x720
import { chromium } from '/opt/node-tools/node_modules/playwright/index.mjs';
import { execSync } from 'node:child_process';
import { mkdirSync, rmSync, existsSync } from 'node:fs';
import { resolve } from 'node:path';

const mode = process.argv[2] || 'sheet';
const FPS = 24;
const page_url = 'file://' + resolve('bench/video/explainer.html');
const outDir = resolve('bench/video/frames');
const browser = await chromium.launch({ executablePath: process.env.CHROME || undefined });
const page = await browser.newPage({ viewport: { width: 1280, height: 720 }, deviceScaleFactor: 1 });
await page.goto(page_url);
await page.waitForFunction(() => window.__explainer);
await page.evaluate(() => document.fonts.ready);
await page.evaluate(() => { document.getElementById('big').remove(); });
const total = await page.evaluate(() => window.__explainer.total);
const canvas = page.locator('#c');

async function shot(t, path) {
  await page.evaluate((s) => window.__explainer.seek(s), t);
  await canvas.screenshot({ path });
}

rmSync(outDir, { recursive: true, force: true }); mkdirSync(outDir, { recursive: true });
if (mode === 'sheet') {
  const times = [2, 7, 14, 23, 30, 40, 50, 60, 66, 75, 82, 92, 105, 115, 120, 127].filter((t) => t < total);
  for (let i = 0; i < times.length; i++) await shot(times[i], `${outDir}/s${String(i).padStart(2, '0')}.png`);
  execSync(`ffmpeg -y -loglevel error -pattern_type glob -i '${outDir}/s*.png' -filter_complex "scale=640:360,tile=4x4" bench/video/sheet.png`);
  console.log(`sheet: bench/video/sheet.png (${times.length} frames, total ${total}s)`);
} else {
  const n = Math.ceil(total * FPS);
  for (let i = 0; i < n; i++) {
    await shot(i / FPS, `${outDir}/f${String(i).padStart(5, '0')}.png`);
    if (i % 240 === 0) console.log(`${i}/${n}`);
  }
  execSync(`ffmpeg -y -loglevel error -framerate ${FPS} -i '${outDir}/f%05d.png' -c:v libx264 -pix_fmt yuv420p -crf 22 -preset medium -movflags +faststart bench/video/ringrt-explainer.mp4`);
  rmSync(outDir, { recursive: true, force: true });
  console.log('video: bench/video/ringrt-explainer.mp4');
}
await browser.close();
