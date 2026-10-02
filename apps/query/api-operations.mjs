// 从本次选定的 OpenAPI 派生调用入口；不维护第二份路径、参数或业务语义表。
export function collectOperations(spec) {
  const operations = {};
  for (const [path, item] of Object.entries(spec.paths)) {
    const operation = item.get;
    if (!operation) continue;
    const id = operation.operationId ?? `GET ${path}`;
    if (typeof id !== 'string' || !id || Object.hasOwn(operations, id)) throw new Error('GET 操作标识须非空且唯一。');
    const parameters = new Map();
    for (const parameter of [...(item.parameters ?? []), ...(operation.parameters ?? [])]) {
      parameters.set(`${parameter.in}:${parameter.name}`, parameter);
    }
    const values = [...parameters.values()];
    // 当前调用器只接受独立的标量路径／查询参数；不猜测其他 OpenAPI 序列化方式。
    const callable = values.every(p => ['path', 'query'].includes(p.in)) &&
      new Set(values.map(p => p.name)).size === values.length;
    Object.defineProperty(operations, id, {enumerable:true, value:{
      operationId:id, method:'GET', path, summary:operation.summary, description:operation.description,
      deprecated:operation.deprecated, parameters:values, callable
    }});
  }
  return operations;
}

// 在宿主映射后继续走原 request 策略；参数检查仅覆盖标量结构，解释资格由业务合同决定。
export function requestForOperation(operation, params = {}) {
  const invalid = message => { const error = new Error(message); error.kind = 'input'; throw error; };
  if (!operation?.callable) invalid('该操作的参数无法由标量调用器表达，请使用原始 request 并按合同组织参数。');
  if (!params || typeof params !== 'object' || Array.isArray(params)) invalid('操作参数须为对象。');
  const scalar = value => ['string', 'number', 'boolean'].includes(typeof value) &&
    (typeof value !== 'number' || Number.isFinite(value));
  const conforms = (value, schema) => {
    if (schema === false) return false;
    if (!schema || schema === true) return true;
    if (schema.allOf && !schema.allOf.every(s => conforms(value, s))) return false;
    if (schema.anyOf && !schema.anyOf.some(s => conforms(value, s))) return false;
    if (schema.oneOf && schema.oneOf.filter(s => conforms(value, s)).length !== 1) return false;
    if (Object.hasOwn(schema, 'const') && value !== schema.const) return false;
    if (schema.enum && !schema.enum.includes(value)) return false;
    if (schema.type) {
      const types = Array.isArray(schema.type) ? schema.type : [schema.type];
      if (!types.some(type => type === 'integer' ? Number.isInteger(value) : typeof value === type)) return false;
    }
    if (typeof value === 'number') {
      if (schema.minimum !== undefined && value < schema.minimum) return false;
      if (schema.maximum !== undefined && value > schema.maximum) return false;
    }
    if (typeof value === 'string') {
      const length = [...value].length;
      if (schema.minLength !== undefined && length < schema.minLength) return false;
      if (schema.maxLength !== undefined && length > schema.maxLength) return false;
      if (schema.pattern !== undefined && !new RegExp(schema.pattern).test(value)) return false;
    }
    return true;
  };
  for (const key of Object.keys(params)) {
    if (!operation.parameters.some(p => p.name === key)) invalid(`操作 ${operation.operationId} 没有参数 ${key}。`);
  }
  let path = operation.path;
  const query = {};
  for (const parameter of operation.parameters) {
    const {name, schema} = parameter;
    if (!Object.hasOwn(params, name) || params[name] === undefined) {
      if (parameter.required || parameter.in === 'path') invalid(`操作 ${operation.operationId} 缺少参数 ${name}。`);
      continue;
    }
    const value = params[name];
    if (!scalar(value) || !conforms(value, schema)) invalid(`操作 ${operation.operationId} 的参数 ${name} 不符合声明的标量类型或取值约束。`);
    if (parameter.in === 'path') path = path.split(`{${name}}`).join(encodeURIComponent(String(value)));
    else Object.defineProperty(query, name, {enumerable:true, value});
  }
  return {method:'GET', path, query};
}

// 只在 search 沙箱创建；describe 给出调用合同，schema 按需读取原始响应子树。
export function createApiDiscovery(spec, operations, outline, select, sourceSpec = spec) {
  const get = id => {
    if (typeof id !== 'string' || !Object.hasOwn(operations, id)) throw new Error(`操作不存在：${String(id)}；请从 api.list() 选择。`);
    return operations[id];
  };
  const response = (operation, status = '200') => spec.paths[operation.path].get.responses?.[status];
  // 保留原始引用及其传递依赖，避免把同一类型在多个字段中反复展开。
  const contract = schema => {
    const references = {};
    const visit = value => {
      if (!value || typeof value !== 'object') return;
      if (Object.hasOwn(value, '$ref') && !Object.hasOwn(references, value.$ref)) {
        const ref = value.$ref;
        if (typeof ref !== 'string' || !ref.startsWith('#/')) throw new Error('调用合同只支持文件内引用。');
        let target = sourceSpec;
        for (const part of decodeURIComponent(ref.slice(2)).split('/')) {
          const key = part.replace(/~1/g, '/').replace(/~0/g, '~');
          if (!target || typeof target !== 'object' || !Object.hasOwn(target, key)) throw new Error(`调用合同引用不存在：${ref}`);
          target = target[key];
        }
        references[ref] = target;
        visit(target);
      }
      for (const child of Object.values(value)) visit(child);
    };
    visit(schema);
    return {schema, references};
  };
  return Object.freeze({
    list: () => Object.values(operations).map(({operationId, path, summary, deprecated, callable}) =>
      ({operationId, path, summary, deprecated, callable})),
    describe: ids => {
      const selected = (Array.isArray(ids) ? ids : [ids]).map(get);
      const results = selected.map(operation => {
        const returned = response(operation), schema = returned?.content?.['application/json']?.schema;
        return {...operation, call:operation.callable ? `domeye.api[${JSON.stringify(operation.operationId)}](params)` : null,
          responseDescription:returned?.description, responseState:schema === undefined ? 'not_declared' : 'outline',
          responseStructure:outline(schema)};
      });
      for (let index = 0; index < selected.length; index++) {
        if (results[index].responseState === 'not_declared') continue;
        const operation = selected[index];
        const schema = sourceSpec.paths?.[operation.path]?.get?.responses?.['200']?.content?.['application/json']?.schema ??
          response(operation)?.content?.['application/json']?.schema;
        const {responseStructure, ...base} = results[index];
        const complete = {...base, responseState:'complete', responseContract:contract(schema)};
        const candidate = results.map((value, i) => i === index ? complete : value);
        // 与现有工具输出使用同一字符门槛；超出预算时明确保留目录供收窄。
        if (JSON.stringify(candidate).length <= 24000) results[index] = complete;
      }
      return results;
    },
    schema: (id, schemaPath = [], status = '200') =>
      select(response(get(id), status)?.content?.['application/json']?.schema, schemaPath)
  });
}
