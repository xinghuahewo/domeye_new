import dataProfile from '../../../config/data-profile.json'

export const businessTimezone = dataProfile.timezone
const formatter = new Intl.DateTimeFormat('sv-SE', {
  timeZone: businessTimezone, year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
})

export function toBusinessTime(date: Date): string {
  return formatter.format(date)
}

// 当前固定数据档由 Vite 校验为 +08:00；数据库特征时间没有时区后缀。
export function businessTimeToIso(value: string): string {
  return new Date(`${value.replace(' ', 'T')}${dataProfile.window_start.slice(-6)}`).toISOString()
}
