import React, { useState } from 'react'
import {
    HomeOutlined,
    SearchOutlined,
    BarChartOutlined,
} from '@ant-design/icons'
import { Layout, Menu } from 'antd'
import type { MenuProps } from 'antd'
import { useNavigate, useLocation } from 'react-router-dom'

const { Sider } = Layout

type MenuItem = Required<MenuProps>['items'][number]

function getItem(
    label: React.ReactNode,
    key: React.Key,
    icon?: React.ReactNode,
    children?: MenuItem[],
): MenuItem {
    return {
        key,
        icon,
        children,
        label,
    } as MenuItem
}

const items: MenuItem[] = [
    getItem('Trang chủ', '/', <HomeOutlined />),
    getItem('Tìm kiếm', '/search', <SearchOutlined />),
    getItem('Phân tích', '/analytics', <BarChartOutlined />),
]

export default function AppSidebar() {
    const [collapsed, setCollapsed] = useState(false)
    const navigate = useNavigate()
    const location = useLocation()

    const handleMenuClick = ({ key }: { key: string }) => {
        navigate(key)
    }

    return (
        <Sider collapsible collapsed={collapsed} onCollapse={(value) => setCollapsed(value)}>
            <div className="demo-logo-vertical h-8 m-4 bg-gray-300 rounded" />
            <Menu
                theme="dark"
                selectedKeys={[location.pathname]}
                mode="inline"
                items={items}
                onClick={handleMenuClick}
            />
        </Sider>
    )
}
