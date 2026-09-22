import { shallowRef } from 'vue'
import { apiGet } from './client'
import type { HealthPayload } from '@/types/api'

export const resultDelivery = shallowRef<HealthPayload['result_delivery']>()
export async function getHealth() {
  const payload = await apiGet<HealthPayload>('healthz')
  resultDelivery.value = payload.result_delivery
  return payload
}
