import { AxiosError } from 'axios'
import { h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import { describe, expect, it } from 'vitest'
import AsnRequestState from './AsnRequestState.vue'

describe('ASN 请求反馈', () => {
  it('事件模式说明当前读取对象和等待上限，不保证命中缓存', async () => {
    const html = await renderToString(h(AsnRequestState, { loading: true, error: null, eventWindow: true }))
    expect(html).toContain('正在读取事件窗口 ASN 数据')
    expect(html).toContain('本次请求最多等待 60 秒')
    expect(html).not.toContain('ASN 运维候选集')
    expect(html).not.toContain('后续查询使用只读缓存')
  })
  it('真实超时错误给出中文反馈与重试入口，不将失败显示为空数据', async () => {
    const error = new AxiosError('timeout of 60000ms exceeded', 'ETIMEDOUT')
    const html = await renderToString(h(AsnRequestState, { loading: false, error, eventWindow: true }))
    expect(html).toContain('ASN 数据请求超时')
    expect(html).toContain('已等待 60 秒')
    expect(html).toContain('未取得结果不表示没有数据')
    expect(html).toContain('重试')
    expect(html).not.toContain('timeout of')
  })

  it('普通故障和输入错误保留原信息，成功后没有错误或重试按钮', async () => {
    for (const error of [new Error('来源暂不可用'), '请输入纯数字 ASN']) {
      const html = await renderToString(h(AsnRequestState, { loading: false, error, eventWindow: false }))
      expect(html).toContain(typeof error === 'string' ? error : error.message)
      expect(html).not.toContain('ASN 数据请求超时')
    }
    const html = await renderToString(h(AsnRequestState, { loading: false, error: null, eventWindow: false }))
    expect(html).not.toContain('重试')
    expect(html).not.toContain('ASN 态势不可用')
  })

  it('浏览器提前中止不被误报为已等待至超时上限', async () => {
    const error = new AxiosError('Request aborted', 'ECONNABORTED')
    const html = await renderToString(h(AsnRequestState, { loading: false, error, eventWindow: true }))
    expect(html).not.toContain('ASN 数据请求超时')
    expect(html).not.toContain('已等待 60 秒')
    expect(html).toContain('请求已中止')
    expect(html).toContain('重试')
  })

})
