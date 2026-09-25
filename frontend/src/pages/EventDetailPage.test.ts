import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'

const source = readFileSync(new URL('./EventDetailPage.vue', import.meta.url), 'utf8')
const routerSource = readFileSync(new URL('../router/index.ts', import.meta.url), 'utf8')
const viteSource = readFileSync(new URL('../../vite.config.ts', import.meta.url), 'utf8')

describe('传统事件观测独立运行', () => {
  it('不渲染报告或组合调查', () => {
    expect(source).not.toContain('CountryOutageReportWorkbench')
    expect(source).not.toContain('country-outage-investigation')
    expect(source).not.toContain('创建组合调查')
    expect(source).not.toContain('报告与追问')
    expect(source).not.toContain('P2-S1 W5')
  })

  it('保留事件数据路由，不注册问答或组合调查', () => {
    expect(routerSource).toContain("path: '/events'")
    expect(routerSource).toContain("path: '/events/detail'")
    expect(routerSource).not.toContain("path: '/events/chat'")
    expect(routerSource).not.toContain("name: 'country-outage-chat'")
    expect(routerSource).not.toContain("path: '/events/investigation'")
    expect(routerSource).not.toContain("name: 'country-outage-investigation'")
  })

  it('仅代理数据 API，不配置独立 Agent 控制面', () => {
    expect(viteSource).toContain("'/api/v1'")
    expect(viteSource).toContain("'/api/v2'")
    expect(viteSource).not.toContain('VITE_AGENT_CONTROL_PROXY_TARGET')
    expect(viteSource).not.toContain('^/api/v2/country-outage(?:/|$)')
  })
})
