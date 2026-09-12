import vue from '@vitejs/plugin-vue'
import { fileURLToPath, URL } from 'node:url'
import { defineConfig } from 'vite'

// 独立开发入口：不加载正式 App、路由、环境文件或 API 代理，不用于发布。
export default defineConfig({
  root: fileURLToPath(new URL('.', import.meta.url)),
  envDir: false,
  plugins: [vue()],
  server: { host: '127.0.0.1', port: 5179, strictPort: true },
  build: { outDir: '../../../../dist/home-prototype' },
})
