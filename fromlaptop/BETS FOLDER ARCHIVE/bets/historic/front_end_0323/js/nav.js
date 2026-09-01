(() => {
  const el = document.getElementById("nav-placeholder");
  if (!el) return;

  fetch("nav.html")
    .then(r => {
      if (!r.ok) throw new Error("nav.html not found");
      return r.text();
    })
    .then(html => {
      el.innerHTML = html;
    })
    .catch(() => {
      el.innerHTML = "";
    });
})();
