import { Breadcrumb, Layout, theme } from 'antd'
import { Outlet, useLocation, Link } from 'react-router-dom'

const { Content } = Layout

export default function AppContent() {
  const {
    token: { colorBgContainer, borderRadiusLG },
  } = theme.useToken()

  const location = useLocation()

  // Tách đường dẫn thành mảng
  const pathSnippets = location.pathname.split('/').filter(i => i)

  // Tạo breadcrumb item
  const breadcrumbItems = [
    <Breadcrumb.Item key="home">
      <Link to="/">Home</Link>
    </Breadcrumb.Item>,
    ...pathSnippets.map((snippet, index) => {
      const url = `/${pathSnippets.slice(0, index + 1).join('/')}`
      return (
        <Breadcrumb.Item key={url}>
          <Link to={url}>{snippet.charAt(0).toUpperCase() + snippet.slice(1)}</Link>
        </Breadcrumb.Item>
      )
    })
  ]

  return (
    <Content style={{ margin: '0 16px' }}>
      <Breadcrumb style={{ margin: '16px 0' }}>{breadcrumbItems}</Breadcrumb>
      <div
        style={{
          padding: 24,
          minHeight: 360,
          background: colorBgContainer,
          borderRadius: borderRadiusLG,
        }}
      >
        <Outlet />
      </div>
    </Content>
  )
}
