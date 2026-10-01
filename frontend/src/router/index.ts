import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router'

const routes: RouteRecordRaw[] = [
  {
    path: '/',
    name: 'home',
    component: () => import('@/pages/CoreOverviewPage.vue'),
    meta: { title: '核心态势', section: '监测' },
  },
  {
    path: '/events',
    name: 'events',
    component: () => import('@/pages/EventsPage.vue'),
    meta: { title: '异常事件', section: '异常监测' },
  },
  {
    path: '/events/detail',
    name: 'event-detail',
    component: () => import('@/pages/EventDetailPage.vue'),
    meta: { title: '事件证据', section: '异常监测' },
  },
  {
    path: '/features',
    name: 'features',
    redirect: to => ({ path: '/', query: to.query, hash: '#routing' }),
  },
  {
    path: '/countries',
    name: 'countries',
    redirect: to => ({ name: 'home', query: to.query }),
  },
  {
    path: '/countries/:country',
    name: 'country-detail',
    redirect: to => ({ name: 'home', query: { ...to.query, country: String(to.params.country) } }),
  },
  {
    path: '/ases',
    name: 'ases',
    component: () => import('@/pages/AsnPage.vue'),
    meta: { title: 'AS 查询', section: '网络观测' },
  },
  {
    path: '/ases/:asn',
    name: 'asn-detail',
    component: () => import('@/pages/AsnPage.vue'),
    meta: { title: 'AS 档案', section: '网络观测' },
  },
]

if (import.meta.env.DEV || import.meta.env.VITE_COMPONENT_PREVIEW === 'true') {
  routes.push({
    path: '/__components',
    name: 'component-preview',
    component: () => import('@/pages/ComponentPreviewPage.vue'),
    meta: { title: '组件标本', section: '开发工具' },
  })
}

routes.push({
  path: '/:pathMatch(.*)*',
  name: 'not-found',
  component: () => import('@/pages/NotFoundPage.vue'),
  meta: { title: '页面不存在', section: 'Domeye Core' },
})

const router = createRouter({
  history: createWebHistory(),
  routes,
  scrollBehavior: to => to.hash ? { el: to.hash } : { top: 0 },
})

router.afterEach((to) => {
  document.title = `${String(to.meta.title || '工作台')} · Domeye Core`
})

export default router
