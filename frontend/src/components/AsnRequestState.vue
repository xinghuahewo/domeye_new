<script setup lang="ts">
import { isAxiosError } from 'axios'
import { computed } from 'vue'
import { errorMessage } from '@/utils/normalize'
import { API_TIMEOUT_MS } from '@/api/client'
import PageState from '@/components/PageState.vue'

const props = defineProps<{ loading: boolean, error: unknown, eventWindow: boolean }>()
const emit = defineEmits<{ retry: [] }>()
const waitSeconds = API_TIMEOUT_MS / 1000
const timedOut = computed(() => isAxiosError(props.error) && props.error.code === 'ETIMEDOUT')
const detail = computed(() => timedOut.value
  ? `已等待 ${waitSeconds} 秒，尚未取得 ASN 数据。未取得结果不表示没有数据；可以重试，或返回事件继续查看已有观测。`
  : isAxiosError(props.error) && props.error.code === 'ECONNABORTED'
    ? '请求已中止，尚未取得 ASN 数据，可以重试。'
    : typeof props.error === 'string' ? props.error : errorMessage(props.error))
</script>

<template>
  <PageState v-if="loading" kind="loading"
    :title="eventWindow ? '正在读取事件窗口 ASN 数据' : '正在读取 ASN 数据'"
    :detail="`首次读取可能较慢。本次请求最多等待 ${waitSeconds} 秒；超时后可重试。`" />
  <PageState v-else-if="error" kind="error" :title="timedOut ? 'ASN 数据请求超时' : 'ASN 态势不可用'"
    :detail="detail" @retry="emit('retry')" />
</template>
