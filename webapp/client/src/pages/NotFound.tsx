import { Result, Button } from 'antd'
import { Link } from 'react-router-dom'

export default function NotFound() {
    console.error('🚨 NotFound component rendered! This might be the cause of the 404 issue.');
    console.error('Current URL:', window.location.href);
    console.error('Current pathname:', window.location.pathname);

    return (
        <Result
            status="404"
            title="404"
            subTitle="Sorry, the page you visited does not exist."
            extra={
                <Link to="/">
                    <Button type="primary">Back Home</Button>
                </Link>
            }
        />
    )
}
