import { createBrowserRouter } from 'react-router-dom'
import MainLayout from '@/layouts/MainLayout'
import Home from '@/pages/Home'
import About from '@/pages/About'
import NotFound from '@/pages/NotFound'
import ErrorBoundary from '@/components/ErrorBoundary'

const router = createBrowserRouter([
    {
        path: '/',
        element: <MainLayout />,
        errorElement: <ErrorBoundary><NotFound /></ErrorBoundary>,
        children: [
            {
                index: true,
                element: <Home />,
                errorElement: <div className="p-8 text-center">
                    <h2 className="text-xl font-bold text-red-600">Có lỗi xảy ra ở trang chủ</h2>
                    <button onClick={() => window.location.reload()} className="mt-4 px-4 py-2 bg-blue-500 text-white rounded">Tải lại</button>
                </div>
            },
            {
                path: 'about',
                element: <ErrorBoundary><About /></ErrorBoundary>
            }
        ]
    }
])

export default router
