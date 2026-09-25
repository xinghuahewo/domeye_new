import dataProfile from '../../../config/data-profile.json'

export const businessTimezone = dataProfile.timezone
const formatter = new Intl.DateTimeFormat('sv-SE', {
  timeZone: businessTimezone, year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
})

export function toBusinessTime(date: Date): string {
  return formatter.format(date)
}

export function formatBusinessEndTime(end: string, start: string, full = false): string {
  const endTime = toBusinessTime(new Date(end))
  return full || endTime.slice(0, 10) !== toBusinessTime(new Date(start)).slice(0, 10)
    ? endTime : endTime.slice(11)
}

// 当前固定数据档由 Vite 校验为 +08:00；数据库特征时间没有时区后缀。
export function businessTimeToIso(value: string): string {
  const timestamp = value.replace(' ', 'T')
  const offset = /(?:Z|[+-]\d{2}:\d{2})$/i.test(timestamp) ? '' : dataProfile.window_start.slice(-6)
  return new Date(`${timestamp}${offset}`).toISOString()
}
