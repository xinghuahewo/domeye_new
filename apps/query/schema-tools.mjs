// 这两个纯函数也注入 search 沙箱；不依赖宿主变量，不改写原始合同。
export function outlineSchema(schema) {
  function visit(value, schemaPath) {
    if (value === undefined) return { schemaPath, state: 'not_declared' };
    if (typeof value === 'boolean') return { schemaPath, booleanSchema: value };
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('所选位置不是 Schema 对象。');
    const result = { schemaPath, keywords: Object.keys(value) };
    for (const key of ['type', 'title', 'description', 'nullable', 'enum', 'const', 'required', 'discriminator']) {
      if (Object.hasOwn(value, key)) result[key] = value[key];
    }
    if (value.properties) result.fields = Object.keys(value.properties);
    for (const key of ['oneOf', 'anyOf', 'allOf', 'prefixItems']) {
      if (Array.isArray(value[key])) result[key] = value[key].map((child, index) => visit(child, [...schemaPath, key, index]));
    }
    for (const key of ['items', 'additionalProperties']) {
      if (Object.hasOwn(value, key)) result[key] = Array.isArray(value[key])
        ? value[key].map((child, index) => visit(child, [...schemaPath, key, index]))
        : visit(value[key], [...schemaPath, key]);
    }
    return result;
  }
  return visit(schema, []);
}

export function selectSchema(schema, schemaPath) {
  if (!Array.isArray(schemaPath) || schemaPath.some(key =>
    typeof key !== 'string' && !(Number.isSafeInteger(key) && key >= 0))) {
    throw new Error('schemaPath 须为字段名或非负整数组成的数组。');
  }
  let value = schema;
  for (const key of schemaPath) {
    if (!value || typeof value !== 'object' || !Object.hasOwn(value, key)) {
      throw new Error(`结构路径不存在：${JSON.stringify(schemaPath)}`);
    }
    value = value[key];
  }
  if (value === undefined) throw new Error(`结构路径不存在：${JSON.stringify(schemaPath)}`);
  return value;
}
