import { Layout } from 'antd'
import AppSidebar from '@/components/layout/AppSidebar'
import AppHeader from '@/components/layout/AppHeader'
import AppContent from '@/components/layout/AppContent'
import AppFooter from '@/components/layout/AppFooter'

export default function MainLayout() {
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <AppSidebar />
      <Layout>
        <AppHeader />
        <AppContent />
        <AppFooter />
      </Layout>
    </Layout>
  )
}
