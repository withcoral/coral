import { createHash } from 'node:crypto'

// Trusted, evidence-independent enhancement. Hash only this exact source, never
// scripts read from artifacts. The rest of the viewer needs no browser runtime.
export const playbackScript = `(() => {
  const video = document.querySelector('#recording video');
  const controls = document.querySelector('[data-playback-controls]');
  if (!video || !controls) return;
  const buttons = controls.querySelectorAll('button[data-playback-rate]');
  const sync = () => {
    for (const button of buttons) {
      button.setAttribute('aria-pressed', String(Number(button.dataset.playbackRate) === video.playbackRate));
    }
  };
  for (const button of buttons) {
    button.addEventListener('click', () => {
      const rate = Number(button.dataset.playbackRate);
      if (![0.1, 0.25, 1].includes(rate)) return;
      video.defaultPlaybackRate = rate;
      video.playbackRate = rate;
      sync();
    });
  }
  video.addEventListener('ratechange', sync);
  sync();
  controls.hidden = false;
})();`

export const playbackScriptHash = `sha256-${createHash('sha256').update(playbackScript).digest('base64')}`
