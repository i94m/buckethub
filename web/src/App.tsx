import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom"
import { ThemeProvider } from "next-themes"

import { JobsProvider } from "@/lib/use-jobs"
import { BucketsProvider } from "@/lib/use-buckets"
import { withApiKey } from "@/lib/link"
import { AdminGuard, AuthGuard } from "@/components/auth-guard"
import { Toaster } from "@/components/ui/sonner"
import { TooltipProvider } from "@/components/ui/tooltip"
import AdminPage from "@/pages/admin-page"
import PublicPage from "@/pages/public-page"

/**
 * 路由：/ = 对外展示页（只读同步矩阵 + 链接录入），/admin = 管理后台。
 * Provider 全部放在 Routes 之上两页共享：JobsProvider 的 socket 只建连一次
 * （放 Route 内切页会断连重连、丢速率样本窗口），AuthGuard/BucketsProvider 同理。
 */
export default function App() {
  return (
    <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
      <BrowserRouter>
        <AuthGuard>
          <BucketsProvider>
            <JobsProvider>
              <TooltipProvider>
                <Routes>
                  {/* 旧首页 / 重定向到 /dashboard（保留 apikey 查询参数） */}
                  <Route path="/" element={<Navigate to={withApiKey("/dashboard")} replace />} />
                  <Route path="/dashboard" element={<PublicPage />} />
                  <Route path="/admin" element={<AdminGuard><AdminPage /></AdminGuard>} />
                  <Route path="*" element={<Navigate to={withApiKey("/dashboard")} replace />} />
                </Routes>
                <Toaster />
              </TooltipProvider>
            </JobsProvider>
          </BucketsProvider>
        </AuthGuard>
      </BrowserRouter>
    </ThemeProvider>
  )
}
