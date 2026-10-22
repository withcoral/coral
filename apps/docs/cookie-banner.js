// Cookie consent banner for the docs.
//
// Cloudflare Zaraz loads analytics on withcoral.com, including these docs, and
// holds back each tool that has a consent purpose until a visitor consents. The
// marketing site renders the same banner; both write Zaraz's `cf_consent`
// cookie, so a choice made on one applies to the other. Without Zaraz (local
// `mint dev`, the mintlify.dev preview), the consent API never becomes ready
// and no banner appears.
(function () {
  const CONSENT_COOKIE_NAME = "cf_consent";
  const BANNER_ID = "coral-cookie-banner";

  const STYLES = `
    #${BANNER_ID} {
      position: fixed;
      left: 0;
      right: 0;
      bottom: 0;
      z-index: 100;
      display: flex;
      justify-content: center;
      padding: 0 12px 12px;
      pointer-events: none;
      transform: translateY(100%);
      visibility: hidden;
      /* Hide the buttons from focus and screen readers after the slide-out. */
      transition:
        transform 500ms ease-out,
        visibility 0s linear 500ms;
    }
    #${BANNER_ID}[data-visible="true"] {
      transform: translateY(0);
      visibility: visible;
      transition: transform 500ms ease-out;
    }
    #${BANNER_ID} .coral-cookie-card {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 12px 24px;
      width: 100%;
      max-width: 768px;
      box-sizing: border-box;
      padding: 12px 20px;
      border: 1px solid rgba(0, 0, 0, 0.06);
      border-radius: 16px;
      background: rgba(255, 255, 255, 0.9);
      backdrop-filter: blur(24px);
      -webkit-backdrop-filter: blur(24px);
      box-shadow: 0 16px 40px -20px rgba(0, 0, 0, 0.2);
      pointer-events: auto;
      letter-spacing: -0.01em;
    }
    #${BANNER_ID} p {
      margin: 0;
      font-size: 14px;
      line-height: 1.5;
      color: rgba(0, 0, 0, 0.6);
    }
    #${BANNER_ID} .coral-cookie-actions {
      display: flex;
      gap: 8px;
      flex-shrink: 0;
    }
    #${BANNER_ID} button {
      height: 32px;
      padding: 0 16px;
      border: 0;
      border-radius: 9999px;
      font: inherit;
      font-size: 14px;
      font-weight: 500;
      cursor: pointer;
      transition: opacity 150ms;
    }
    #${BANNER_ID} button:hover {
      opacity: 0.8;
    }
    #${BANNER_ID} .coral-cookie-decline {
      background: rgba(0, 0, 0, 0.05);
      color: rgba(0, 0, 0, 0.87);
    }
    #${BANNER_ID} .coral-cookie-accept {
      background: #111;
      color: #fff;
    }
    html.dark #${BANNER_ID} .coral-cookie-card {
      border-color: rgba(255, 255, 255, 0.08);
      background: rgba(25, 25, 25, 0.9);
      box-shadow: 0 16px 40px -20px rgba(0, 0, 0, 0.6);
    }
    html.dark #${BANNER_ID} p {
      color: rgba(255, 255, 255, 0.65);
    }
    html.dark #${BANNER_ID} .coral-cookie-decline {
      background: rgba(255, 255, 255, 0.08);
      color: rgba(255, 255, 255, 0.93);
    }
    html.dark #${BANNER_ID} .coral-cookie-accept {
      background: #fff;
      color: #111;
    }
  `;

  function getCookie(name) {
    const value = `; ${document.cookie}`;
    return value.split(`; ${name}=`)[1]?.split(";")[0];
  }

  function withConsentAPI(callback) {
    if (window.zaraz?.consent?.APIReady) {
      callback();
      return;
    }

    document.addEventListener("zarazConsentAPIReady", callback, { once: true });
  }

  function createBanner() {
    const style = document.createElement("style");
    style.textContent = STYLES;
    document.head.appendChild(style);

    const banner = document.createElement("div");
    banner.id = BANNER_ID;
    banner.setAttribute("role", "region");
    banner.setAttribute("aria-label", "Cookie consent");
    banner.innerHTML = `
      <div class="coral-cookie-card">
        <p>We use cookies to improve your experience and analyze site traffic.</p>
        <div class="coral-cookie-actions">
          <button type="button" class="coral-cookie-decline">Decline</button>
          <button type="button" class="coral-cookie-accept">Accept</button>
        </div>
      </div>
    `;
    document.body.appendChild(banner);

    banner
      .querySelector(".coral-cookie-accept")
      .addEventListener("click", () => applyConsent(true));
    banner
      .querySelector(".coral-cookie-decline")
      .addEventListener("click", () => applyConsent(false));

    return banner;
  }

  function getBanner() {
    return document.getElementById(BANNER_ID) ?? createBanner();
  }

  function syncBanner() {
    if (!window.zaraz?.consent?.APIReady) {
      return;
    }

    const visible = !getCookie(CONSENT_COOKIE_NAME);
    if (!visible && !document.getElementById(BANNER_ID)) {
      return;
    }

    const banner = getBanner();
    // Wait a frame so a newly added banner slides in rather than appearing.
    requestAnimationFrame(() => {
      banner.dataset.visible = String(visible);
    });
  }

  function applyConsent(consentStatus) {
    withConsentAPI(() => {
      window.zaraz.consent.setAll(consentStatus);

      if (consentStatus) {
        window.zaraz.consent.sendQueuedEvents();
      }

      getBanner().dataset.visible = "false";
    });
  }

  withConsentAPI(syncBanner);
  document.addEventListener("zarazConsentChoicesUpdated", syncBanner);
})();
