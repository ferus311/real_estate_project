import { Layout as AntLayout, Menu, Button, Dropdown } from "antd";
import {
  HomeOutlined,
  CalculatorOutlined,
  InfoCircleOutlined,
  UserOutlined,
  MenuOutlined,
} from "@ant-design/icons";
import "../styles/Layout.css";

const { Header, Content, Footer } = AntLayout;

const Layout = ({ children }) => {
  const menuItems = [
    {
      key: "home",
      icon: <HomeOutlined />,
      label: "Trang chủ",
    },
    {
      key: "predict",
      icon: <CalculatorOutlined />,
      label: "Dự đoán giá",
    },
    {
      key: "about",
      icon: <InfoCircleOutlined />,
      label: "Giới thiệu",
    },
  ];

  const mobileMenu = (
    <Menu items={menuItems} className="mobile-dropdown-menu" />
  );

  return (
    <AntLayout className="app-layout">
      <Header className="app-header">
        <div className="header-container">
          <div className="logo">
            <HomeOutlined className="logo-icon" />
            <span>BĐS AI</span>
          </div>

          <div className="desktop-menu">
            <Menu
              mode="horizontal"
              defaultSelectedKeys={["predict"]}
              items={menuItems}
              className="main-menu"
            />
          </div>

          <div className="header-actions">
            <Button type="text" icon={<UserOutlined />} className="user-button">
              Đăng nhập
            </Button>

            <Dropdown
              overlay={mobileMenu}
              trigger={["click"]}
              className="mobile-menu-dropdown"
            >
              <Button
                type="text"
                icon={<MenuOutlined />}
                className="mobile-menu-button"
              />
            </Dropdown>
          </div>
        </div>
      </Header>

      <Content className="app-content">{children}</Content>

      <Footer className="app-footer">
        <div className="footer-container">
          <div className="footer-logo">
            <HomeOutlined className="logo-icon" />
            <span>BĐS AI</span>
          </div>

          <div className="footer-links">
            <a href="#">Điều khoản sử dụng</a>
            <a href="#">Chính sách bảo mật</a>
            <a href="#">Liên hệ</a>
          </div>

          <div className="footer-copyright">
            © {new Date().getFullYear()} Hệ thống dự đoán giá nhà | Được phát
            triển bởi DS Team
          </div>
        </div>
      </Footer>
    </AntLayout>
  );
};

export default Layout;
