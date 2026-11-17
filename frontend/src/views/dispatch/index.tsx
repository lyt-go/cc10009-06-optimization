import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'

import { request } from '@/api/client'

const ENDPOINT = '/api/dispatch'
const columns = [
  "策略编号",
  "目标域名",
  "分配算法",
  "节点亲和性",
  "负载阈值",
  "稳定窗口",
  "优先级",
  "当前结果编号",
  "当前结果版本",
  "待复核结果"
]
const actions = [
  "提交策略验证",
  "启用策略",
  "停用策略"
]

type Row = Record<string, string | number | null>

type Stats = { total: number; enabled: number; verifying: number }

export default function DispatchView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => ['策略编号', '目标域名'], [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('分配策略列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '请求分配列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  const stats: Stats = useMemo(() => ({
    total,
    enabled: rows.filter((row) => row.status === '已启用').length,
    verifying: rows.filter((row) => row.status === '策略验证中').length,
  }), [rows, total])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('请求分配动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '请求分配操作失败')
    }
  }

  return (
    <section className="page" data-module="dispatch">
      <header className="page-head">
        <div>
          <h2>请求分配管理</h2>
          <p className="page-desc">
            管理采集请求的分配策略，按域名粒度和节点负载动态调度。策略编号决定分配结果数据集名字，
            可下钻到「请求分配结果」查看每一版分流。
          </p>
        </div>
        <div className="page-actions">
          <Link className="btn primary" to="/dispatch-result">查看请求分配结果</Link>
          <button className="btn" type="button" onClick={() => setErrorMessage('分配策略登记入口尚未接入审批流')}>
            登记分配策略
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出请求分配清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">分配策略总数</span>
          <strong className="stat-value">{stats.total}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">已启用策略</span>
          <strong className="stat-value">{stats.enabled}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">验证中策略</span>
          <strong className="stat-value">{stats.verifying}</strong>
        </article>
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>结果</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => (
                <td key={column}>
                  {column === '当前结果编号' && row[column]
                    ? <Link to={`/dispatch-result?domain=${encodeURIComponent(String(row['目标域名']))}`}>{row[column]}</Link>
                    : (row[column] ?? '—')}
                </td>
              ))}
              <td>
                <Link className="link" to={`/dispatch-result?domain=${encodeURIComponent(String(row['目标域名']))}`}>
                  分配结果
                </Link>
              </td>
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 2}>暂无请求分配数据，可先登记分配策略</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条请求分配记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
