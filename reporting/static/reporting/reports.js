document.addEventListener("DOMContentLoaded", () => {
  const form = document.querySelector(".report-form");
  const users = new Set(["users", "countries", "ages", "genders"]);
  function show(name, visible) {
    const wrapper = form.querySelector(`[data-field="${name}"]`);
    wrapper.hidden = !visible;
    wrapper.querySelectorAll("input,select").forEach(el => { el.disabled = !visible; });
  }
  function update() {
    const report = form.elements.report.value;
    const user = users.has(report);
    const total = form.elements.total.checked;
    const custom = form.elements.custom.checked;
    show("content_type", !user && report !== "follows" && report !== "social");
    show("total", user);
    show("custom", user && !total);
    show("month", user && !total && !custom);
    show("year", user && !total && !custom);
    show("since", user && !total && custom);
    show("until", user && !total && custom);
    show("months", !user);
  }
  ["report", "total", "custom"].forEach(name => form.elements[name].addEventListener("change", update));
  update();
});
