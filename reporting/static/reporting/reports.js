document.addEventListener("DOMContentLoaded", () => {
  const form = document.querySelector(".report-form");
  if (!form) return;
  const users = new Set(["users", "countries", "ages", "genders"]);
  function control(name, visible, enabled = visible) {
    const wrapper = form.querySelector(`[data-field="${name}"]`);
    if (!wrapper) return;
    wrapper.hidden = !visible;
    wrapper.querySelectorAll("input,select").forEach(el => { el.disabled = !enabled; });
  }
  function update(event) {
    const report = form.elements.report.value;
    const user = users.has(report);
    const creator = report === "creator_eligibility";
    if (!user) form.elements.total.checked = false;
    if (event?.target.name === "total" && form.elements.total.checked) form.elements.custom.checked = false;
    if (event?.target.name === "custom" && form.elements.custom.checked) form.elements.total.checked = false;
    const total = user && form.elements.total.checked;
    const custom = form.elements.custom.checked;
    control("content_type", !user && !creator && report !== "follows" && report !== "social");
    ["countries", "ages", "genders"].forEach(name => control(name, !creator));
    control("min_followers", creator);
    control("total", user, user && !custom);
    control("custom", true, !total);
    control("months", true, !total && !custom);
    control("years", true, !total && !custom);
    control("since", true, custom);
    control("until", true, custom);
  }
  ["report", "total", "custom"].forEach(name => form.elements[name].addEventListener("change", update));
  update();
});
