import "../styles/Footer.css";

export default function Footer() {
  return (
    <footer className="footer">
      <div className="footer-container">
        <div className="footer-copyright">
          © 2023 DS Project. All rights reserved
        </div>
        <div className="footer-links">
          <a href="#" className="footer-link">
            Privacy
          </a>
          <a href="#" className="footer-link">
            Terms
          </a>
          <a href="#" className="footer-link">
            Contact
          </a>
        </div>
      </div>
    </footer>
  );
}
