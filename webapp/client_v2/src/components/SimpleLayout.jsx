import { Layout as AntLayout } from "antd";
import { hustLogo, headerBackground } from "../assets";
import "../styles/SimpleLayout.css";

const { Header, Content } = AntLayout;

const SimpleLayout = ({ children, appName }) => {
  return (
    <AntLayout className="simple-layout">
      <Header
        className="simple-header"
        style={{ backgroundImage: `url(${headerBackground})` }}
      >
        <div className="header-overlay"></div>
        <div className="header-logo">
          <div className="logo-wrapper">
            <img src={hustLogo} alt="HUST Logo" className="hust-logo" />
          </div>
          <span className="app-name">{appName}</span>
        </div>
      </Header>

      <Content className="simple-content">{children}</Content>
    </AntLayout>
  );
};

export default SimpleLayout;
