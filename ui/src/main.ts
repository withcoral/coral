import "./styles.css";

const app = document.querySelector<HTMLDivElement>("#app");

if (!app) {
  throw new Error("Missing #app root");
}

app.innerHTML = `
  <main class="placeholder-page">
    <section class="placeholder-panel" aria-labelledby="placeholder-title">
      <p class="eyebrow">Coral</p>
      <h1 id="placeholder-title">UI Placeholder</h1>
      <p>Coral UI Coming soon</p>
    </section>
  </main>
`;
