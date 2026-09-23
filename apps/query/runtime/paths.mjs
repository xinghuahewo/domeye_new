// 只匹配合同中的完整路径；参数按单个 URL 段编码，不能改变宿主或路由层级。
export function createPathMatcher(paths) {
  const routes = paths.map(path => ({ path, parts: path.split('/').slice(1) }));
  return path => {
    if (typeof path !== 'string' || !path.startsWith('/') || /[\s?#{}\\]/u.test(path)) return null;
    const parts = path.split('/').slice(1);
    if (parts.some(part => {
      try {
        const decoded = decodeURIComponent(part);
        return !decoded || decoded === '.' || decoded === '..' || /[/\\%\u0000-\u001f\u007f]/u.test(decoded);
      } catch { return true; }
    })) return null;
    // 静态路径优先，避免被宽泛事件模板冒认。
    const exact = routes.find(route => route.path === path);
    if (exact) return exact.path;
    return routes.find(route => route.parts.length === parts.length && route.parts.every((part, index) =>
      /^\{[A-Za-z_][A-Za-z0-9_]*\}$/.test(part) || part === parts[index]))?.path ?? null;
  };
}
