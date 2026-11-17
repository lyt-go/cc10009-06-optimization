import { useCallback, useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'

import { fetchJson, request } from '@/api/client'

const ENDPOINT = '/api/dispatch-result'

type Entry = {
  id: number
  result_no: string
  version: number
  revision?: number
  status: string
  domain: string
  domains: string[]
  affected_nodes: string[]
  untouched_domains: string[]
  assignments: { request_key: string; node_code: string; slot: number }[]
  notes: string[]
  moves: { request_key: string; from_node: string; to_node: string; reason: string }[]
  previous_result_no: string
  previous_version: number
  reviewer?: string
  reviewed_at?: string
  review_note?: string
  created_at: string
  failure_reason?: string
  rollback_to?: string
  version_conflicts?: Record<string, unknown>
  sink_manifest?: SinkRow[]
}

type SinkRow = {
  node_id: number
  node_code: string
  result_no: string
  version: number
  request_count: number
  occupied_slots: number[]
  node_occupied_slots?: number[]
  free_slots: number
  state: string
}

type VersionRow = {
  result_id: number
  result_no: string
  version: number
  status: string
  is_current: boolean
  assignments: { request_key: string; node_code: string; slot: number }[]
}

type RuleRow = {
  id: number
  策略编号?: string
  目标域名?: string
  分配算法?: string
  节点亲和性?: string
  负载阈值?: number
  稳定窗口?: number
  优先级?: number
}

type Review = {
  result: Entry
  rules: RuleRow[]
  target_domains: string[]
  affinities: Record<string, string>
  versions: VersionRow[]
  moves: Entry['moves']
  capacity_snapshot: { fingerprint?: string; taken_at?: string; nodes?: Record<string, unknown>[] }
  sink_manifest: SinkRow[]
  traceability: Record<string, unknown>
}

type RebalanceResponse = {
  ok: boolean
  skipped?: boolean
  idempotent?: boolean
  message: string
  extra?: {
    entries?: Entry[]
    failures?: { domain: string; message: string }[]
    skipped_domains?: { domain: string; result_no: string; reason: string }[]
    affected_nodes?: { node_code: string; state: string; notes: string[] }[]
    stable_window_hold?: { node_code: string; notes: string[] }[]
  }
  entry?: Entry | null
}

const statuses = ['待复核', '已生效', '迁移失败', '已回滚', '已被替代']

export default function DispatchResultView() {
  const [searchParams] = useSearchParams()
  const [domainFilter, setDomainFilter] = useState(searchParams.get('domain') ?? '')
  const [statusFilter, setStatusFilter] = useState('')
  const [rows, setRows] = useState<Entry[]>([])
  const [total, setTotal] = useState(0)
  const [message, setMessage] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [review, setReview] = useState<Review | null>(null)
  const [reviewer, setReviewer] = useState('张复核')
  const [busy, setBusy] = useState(false)

  const load = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams({ size: '100' })
    if (domainFilter) query.set('domain', domainFilter)
    if (statusFilter) query.set('status', statusFilter)
    try {
      const payload = await fetchJson<{ items: Entry[]; total: number }>(`${ENDPOINT}?${query.toString()}`)
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '请求分配结果读取失败')
    }
  }, [domainFilter, statusFilter])

  useEffect(() => {
    void load()
  }, [load])

  const stats = useMemo(() => {
    const pending = rows.filter((row) => row.status === '待复核').length
    const failed = rows.filter((row) => row.status === '迁移失败').length
    const current = rows.filter((row) => row.status === '已生效').length
    return { total, pending, failed, current }
  }, [rows, total])

  async function runRebalance() {
    if (busy) return
    setBusy(true)
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/rebalance`, {
        method: 'POST',
        body: JSON.stringify({ values: { force: false } }),
      })
      const payload = (await response.json()) as RebalanceResponse
      setMessage(payload.message)
      if (!payload.ok) {
        const detail = payload.extra?.failures?.map((item) => `${item.domain}：${item.message}`).join('；')
        setErrorMessage(detail || '增量再平衡未完全成功')
      }
      await load()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '增量再平衡请求失败')
    } finally {
      setBusy(false)
    }
  }

  async function openReview(row: Entry) {
    setErrorMessage('')
    try {
      const payload = await fetchJson<Review>(`${ENDPOINT}/${row.id}/review`)
      setReview(payload)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '复核视图读取失败')
    }
  }

  async function runAction(row: Entry, action: string) {
    setErrorMessage('')
    setMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ values: { action, reviewer, note: '' } }),
      })
      const payload = await response.json()
      setMessage(payload.message ?? '')
      if (!payload.ok) {
        setErrorMessage(payload.message ?? '动作未生效')
      }
      if (review && review.result.id === row.id) {
        setReview(null)
      }
      await load()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '分配结果操作失败')
    }
  }

  return (
    <section className="page" data-module="dispatch-result">
      <header className="page-head">
        <div>
          <h2>请求分配结果</h2>
          <p className="page-desc">
            只重算受影响域名的分流，未受影响的域名保持原落点。每份结果记录迁移前后的版本、
            责任节点、容量快照与迁移原因；稳定窗口内短时超阈值不会触发迁移。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" disabled={busy} onClick={() => void runRebalance()}>
            {busy ? '正在再平衡…' : '执行增量再平衡'}
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出分配结果
          </button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">结果版本总数</span>
          <strong className="stat-value">{stats.total}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">当前生效</span>
          <strong className="stat-value">{stats.current}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">待复核</span>
          <strong className="stat-value">{stats.pending}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">迁移失败</span>
          <strong className="stat-value">{stats.failed}</strong>
        </article>
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void load() }}>
        <label className="filter-item">
          <span>目标域名</span>
          <input
            value={domainFilter}
            placeholder="按目标域名过滤"
            onChange={(event) => setDomainFilter(event.target.value)}
          />
        </label>
        <label className="filter-item">
          <span>结果状态</span>
          <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
            <option value="">全部</option>
            {statuses.map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>复核人</span>
          <input value={reviewer} onChange={(event) => setReviewer(event.target.value)} />
        </label>
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => { setDomainFilter(''); setStatusFilter('') }}>
          重置条件
        </button>
      </form>

      {(message || errorMessage) && (
        <p className={errorMessage ? 'error-text' : 'hint-text'}>{errorMessage || message}</p>
      )}

      <table className="data-table">
        <thead>
          <tr>
            <th>结果编号</th>
            <th>版本</th>
            <th>状态</th>
            <th>目标域名</th>
            <th>受影响节点</th>
            <th>请求数</th>
            <th>迁移条数</th>
            <th>上一版本</th>
            <th>生成时间</th>
            <th>复核/回滚</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              <td>{row.result_no}</td>
              <td>v{row.version}{row.revision && row.revision > 1 ? `.${row.revision}` : ''}</td>
              <td>{row.status}</td>
              <td>{row.domains?.join('、') || row.domain}</td>
              <td>{row.affected_nodes?.join('、') || '—'}</td>
              <td>{row.assignments?.length ?? 0}</td>
              <td>{row.moves?.length ?? 0}</td>
              <td>{row.previous_result_no ? `v${row.previous_version}` : '—'}</td>
              <td>{row.created_at?.replace('T', ' ').slice(0, 19)}</td>
              <td className="row-actions">
                <button className="link" type="button" onClick={() => void openReview(row)}>复核视图</button>
                {row.status === '待复核' && (
                  <>
                    <button className="link" type="button" onClick={() => void runAction(row, '复核通过')}>复核通过</button>
                    <button className="link" type="button" onClick={() => void runAction(row, '驳回')}>驳回</button>
                  </>
                )}
                {(row.status === '已生效' || row.status === '待复核') && row.previous_result_no && (
                  <button className="link" type="button" onClick={() => void runAction(row, '回滚')}>回滚</button>
                )}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={10}>暂无请求分配结果，可先执行一次增量再平衡</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条分配结果版本</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>

      {review && <ReviewPanel review={review} onClose={() => setReview(null)} />}
    </section>
  )
}

function ReviewPanel({ review, onClose }: { review: Review; onClose: () => void }) {
  const { result } = review
  return (
    <section className="review-panel" aria-label="分配结果复核">
      <header className="review-head">
        <div>
          <h3>复核：{result.result_no} 第 {result.version} 版</h3>
          <p className="page-desc">
            目标域名 {review.target_domains.join('、')}
            {' · '}亲和性 {Object.entries(review.affinities).map(([domain, affinity]) => `${domain}=${affinity || '未限定'}`).join('、')}
          </p>
        </div>
        <button className="btn ghost" type="button" onClick={onClose}>关闭</button>
      </header>

      <div className="review-grid">
        <article className="review-card">
          <h4>分配策略</h4>
          <table className="data-table compact">
            <thead>
              <tr><th>策略编号</th><th>目标域名</th><th>算法</th><th>亲和性</th><th>阈值</th><th>稳定窗口</th><th>优先级</th></tr>
            </thead>
            <tbody>
              {review.rules.map((rule) => (
                <tr key={String(rule.id)}>
                  <td>{rule.策略编号 ?? '—'}</td>
                  <td>{rule.目标域名 ?? '—'}</td>
                  <td>{rule.分配算法 ?? '—'}</td>
                  <td>{rule.节点亲和性 || '—'}</td>
                  <td>{rule.负载阈值 ?? '—'}</td>
                  <td>{rule.稳定窗口 ?? '—'}</td>
                  <td>{rule.优先级 ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>

        <article className="review-card">
          <h4>版本对比</h4>
          <table className="data-table compact">
            <thead>
              <tr><th>版本</th><th>状态</th><th>分流落点</th><th>当前</th></tr>
            </thead>
            <tbody>
              {review.versions.map((version) => (
                <tr key={`${version.result_no}-${version.version}`}>
                  <td>v{version.version}</td>
                  <td>{version.status}</td>
                  <td>
                    {version.assignments
                      .map((item) => `${item.request_key}→${item.node_code}#${item.slot}`)
                      .join('，') || '（空）'}
                  </td>
                  <td>{version.is_current ? '是' : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>

        <article className="review-card">
          <h4>迁移原因</h4>
          <table className="data-table compact">
            <thead>
              <tr><th>请求</th><th>迁移前</th><th>迁移后</th><th>原因</th></tr>
            </thead>
            <tbody>
              {review.moves.map((move) => (
                <tr key={move.request_key}>
                  <td>{move.request_key}</td>
                  <td>{move.from_node || '—'}</td>
                  <td>{move.to_node || '—'}</td>
                  <td>{move.reason}</td>
                </tr>
              ))}
              {review.moves.length === 0 && (
                <tr><td className="empty-state" colSpan={4}>本次没有请求换节点</td></tr>
              )}
            </tbody>
          </table>
        </article>

        <article className="review-card">
          <h4>调度落点侧清单</h4>
          <table className="data-table compact">
            <thead>
              <tr><th>节点</th><th>结果编号</th><th>版本</th><th>占用槽位</th><th>节点剩余</th><th>状态</th></tr>
            </thead>
            <tbody>
              {review.sink_manifest.map((row) => (
                <tr key={`${row.node_id}-${row.result_no}-${row.version}`}>
                  <td>{row.node_code}</td>
                  <td>{row.result_no}</td>
                  <td>v{row.version}</td>
                  <td>{row.occupied_slots.join(',') || '—'}</td>
                  <td>{row.free_slots}</td>
                  <td>{row.state}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>

        <article className="review-card">
          <h4>容量快照与追溯</h4>
          <p className="page-desc">
            快照指纹 {review.capacity_snapshot.fingerprint ?? '—'}
            {' · '}采集时间 {String(review.capacity_snapshot.taken_at ?? '—').replace('T', ' ')}
          </p>
          <ul className="trace-list">
            {Object.entries(review.traceability).map(([key, value]) => (
              <li key={key}>
                <span>{traceLabel(key)}</span>
                <strong>{typeof value === 'object' ? JSON.stringify(value) : String(value ?? '—')}</strong>
              </li>
            ))}
          </ul>
        </article>
      </div>
    </section>
  )
}

function traceLabel(key: string): string {
  const labels: Record<string, string> = {
    generated_by: '生成责任人',
    generated_at: '生成时间',
    reviewer: '复核人',
    reviewed_at: '复核时间',
    previous_versions: '迁移前版本',
    previous_result_no: '迁移前结果',
    affected_nodes: '受影响节点',
    operator: '操作人',
    batch_no: '批次号',
  }
  return labels[key] ?? key
}
