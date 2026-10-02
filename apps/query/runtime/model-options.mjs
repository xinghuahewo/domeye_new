export function configuredThinkingLevel(config) {
  const level = config.thinkingLevel ?? 'high';
  if (!['low','high'].includes(level)) throw new Error('thinkingLevel 只支持 low 或 high；省略时使用 high。');
  return level;
}

export function configureModel(known, config) {
  const model = {...known,baseUrl:config.baseUrl ?? known.baseUrl};
  // Pro 已于 2026-08-13 支持 low；Pi 0.87.0 的目录仍将其标为 null 并提升为 high。
  // 只修正会话副本；升级依赖时对照 https://api-docs.deepseek.com/updates/ 复核。
  if (model.provider==='deepseek' && model.id==='deepseek-v4-pro' && model.thinkingLevelMap?.low===null) {
    model.thinkingLevelMap = {...model.thinkingLevelMap,low:'low'};
  }
  return {model,thinkingLevel:configuredThinkingLevel(config)};
}
