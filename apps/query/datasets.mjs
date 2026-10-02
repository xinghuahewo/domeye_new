import { API_SOURCE_COMMIT } from './source.mjs';

// 宿主登记本项目的两个读取入口。批次固定于会话，覆盖与版本仍由实际查询发现。
export const PROJECT_DATASETS = Object.freeze([
  Object.freeze({ id: 'completed-files', label: '2026 年 2 月 24 日完成结果',
    description: '本项目已交付的完成文件；实际覆盖在查询时确认。',
    apiBaseUrl: 'http://127.0.0.1:28683', specFile: 'openapi.json',
    sourceRun: '/home/bgpdata/domeye-new-runtime/dev/runs/iran-business-20260921d',
    apiSourceCommit: API_SOURCE_COMMIT, apiSourceSnapshot: null }),
  Object.freeze({ id: 'three-day', label: '三日任务已交付结果',
    description: '仅查询本项目已完成并可读的部分，不代表三日完整。',
    apiBaseUrl: 'http://127.0.0.1:28473', specFile: 'openapi-three-day.json',
    sourceRun: '/home/bgpdata/domeye-new-runtime/dev/runs/iran-three-days-20260922a',
    apiSourceCommit: null, apiSourceSnapshot: 'three-day-source.json' }),
]);

export function selectDataset(id = 'completed-files', apiBaseUrl) {
  const dataset = PROJECT_DATASETS.find(item => item.id === id);
  if (!dataset) throw new Error('请选择已登记的本项目结果批次。');
  return { ...dataset, apiBaseUrl: apiBaseUrl ?? dataset.apiBaseUrl };
}

export const publicDataset = dataset => dataset ? ({
  id: dataset.id, label: dataset.label, description: dataset.description,
}) : null;
