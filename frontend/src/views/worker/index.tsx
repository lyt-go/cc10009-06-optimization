import { useCallback, useEffect, useMemo, useState } from 'react'

import { fetchJson, request } from '@/api/client'

const ENDPOINT = '/api/worker'
const columns = [
  "节点编号",
  "主机地址",
  "并发槽位数",
  "地域标签",
  "CPU使用率",
  "负载状态",
  "已占用槽位",
  "剩余槽位",
  "当前结果编号",
  "当前结果版本",
  "最近迁移原因"
]
const actions = [
  "提交注册",
  "标记在线",
  "标记离线"
]

type Row = Record<string, string | number | null>

type CapacityRow = {
  node_id: number
  node_code: string
  slots: number
  occupied_slots: number[]
  free_slots: number
  eligible: boolean
  accepting: boolean
  reason: string
  current_result_no: string
  current_result_version: number
  last_move_reason: string
}

type CapacityView = {
  taken_at: string
  fingerprint: string
  items: CapacityRow[]
}

export default function WorkerView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [capacity, setCapacity] = useState<CapacityView | null>(null)
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => [
    { field: '节点编号', param: 'keyword' },
    { field: '负载状态', param: 'load_state' },
  ], [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams()
    filterFields.forEach(({ field, param }) => {
      const value = filters[field]
      if (value) query.set(param, value)
    })
    try {
      const response = await request(`${ENDPOINT}?${query.toString()}`)
      if (!response.ok) {
        throw new Error('工作节点列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
      setCapacity(await fetchJson<CapacityView>(`${ENDPOINT}/capacity`))
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '采集节点列表读取失败')
    }
  }, [filters, filterFields])

  useEffect(() => {
    void reload()
  }, [reload])

  const stats = useMemo(() => ({
    total,
    online: rows.filter((row) => row.status === '在线').length,
    abnormal: rows.filter((row) => row['负载状态'] !== '正常').length,
  }), [rows, total])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('采集节点动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '采集节点操作失败')
    }
  }

  return (
    <section className="page" data-module="worker">
      <header className="page-head">
        <div>
          <h2>采集节点管理</h2>
          <p className="page-desc">
            跟踪节点资源与心跳健康。已占用/剩余槽位由调度落点侧清单反推，连续超阈值超出稳定窗口的节点会退出分流目标。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('工作节点登记入口尚未接入审批流')}>
            登记工作节点
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出采集节点清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">工作节点总数</span>
          <strong className="stat-value">{stats.total}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">在线节点数</span>
          <strong className="stat-value">{stats.online}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">负载异常节点</span>
          <strong className="stat-value">{stats.abnormal}</strong>
        </article>
      </div>

      {capacity && (
        <p className="hint-text">
          容量快照 {capacity.fingerprint} · 采集时间 {capacity.taken_at.replace('T', ' ').slice(0, 19)}
          {' · '}共 {capacity.items.length} 个节点，剩余槽位合计 {capacity.items.reduce((sum, item) => sum + item.free_slots, 0)}
        </p>
      )}

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map(({ field }) => (
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
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => (
                <td key={column}>{row[column] ?? '—'}</td>
              ))}
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
              <td className="empty-state" colSpan={columns.length + 1}>暂无采集节点数据，可先登记工作节点</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条采集节点记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
