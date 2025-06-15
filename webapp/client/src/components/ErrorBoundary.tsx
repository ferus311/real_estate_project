import React from 'react';
import { Result, Button } from 'antd';

interface ErrorBoundaryState {
    hasError: boolean;
    error: Error | null;
}

class ErrorBoundary extends React.Component<
    { children: React.ReactNode },
    ErrorBoundaryState
> {
    constructor(props: { children: React.ReactNode }) {
        super(props);
        this.state = { hasError: false, error: null };
    }

    static getDerivedStateFromError(error: Error): ErrorBoundaryState {
        console.error('🚨 ErrorBoundary caught error:', error);
        return { hasError: true, error };
    }

    componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
        console.error('🚨 ErrorBoundary componentDidCatch:', error, errorInfo);
    }

    render() {
        if (this.state.hasError) {
            return (
                <div className="min-h-screen flex items-center justify-center bg-gray-50">
                    <Result
                        status="error"
                        title="Có lỗi xảy ra"
                        subTitle="Đã có lỗi trong ứng dụng. Vui lòng làm mới trang."
                        extra={[
                            <Button
                                type="primary"
                                key="refresh"
                                onClick={() => window.location.reload()}
                            >
                                Làm mới trang
                            </Button>,
                            <Button
                                key="home"
                                onClick={() => {
                                    this.setState({ hasError: false, error: null });
                                    window.location.href = '/';
                                }}
                            >
                                Về trang chủ
                            </Button>
                        ]}
                    />
                </div>
            );
        }

        return this.props.children;
    }
}

export default ErrorBoundary;
