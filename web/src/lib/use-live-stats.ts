import * as React from "react"

import { getStats, type FileStats } from "@/lib/api"

/**
 * 统计卡片实时数据：挂载拉一次 + 定时轮询 `/api/stats`。
 *
 * - 仅更新 `stats` 状态，读取它的统计卡数字节点随之重渲染，不触发整页重建。
 * - 页面隐藏（`document.hidden`）时暂停轮询省资源；重新可见时立即补一次再恢复。
 * - 返回值里的 `reload` 供"刷新"按钮主动重取。
 *
 * @param intervalMs 轮询间隔（毫秒），默认 10000（10 秒）。
 */
export function useLiveStats(intervalMs: number = 10_000) {
  const [stats, setStats] = React.useState<FileStats | null>(null)

  const load = React.useCallback(() => {
    getStats()
      .then(setStats)
      .catch(() => {
        /* 轮询失败保持上一次数值 */
      })
  }, [])

  React.useEffect(() => {
    let timer: number | undefined
    const start = () => {
      if (timer === undefined) timer = window.setInterval(load, intervalMs)
    }
    const stop = () => {
      if (timer !== undefined) {
        window.clearInterval(timer)
        timer = undefined
      }
    }
    const onVisibility = () => {
      if (document.hidden) {
        stop()
      } else {
        load()
        start()
      }
    }
    load()
    start()
    document.addEventListener("visibilitychange", onVisibility)
    return () => {
      stop()
      document.removeEventListener("visibilitychange", onVisibility)
    }
  }, [load, intervalMs])

  return { stats, reload: load }
}
