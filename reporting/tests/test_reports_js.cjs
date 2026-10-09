// Run with: node reporting/tests/test_reports_js.cjs
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const path = require("node:path");

const names = ["report", "total", "custom", "months", "years", "since", "until", "content_type", "countries", "ages", "genders", "min_followers"];
const elements = Object.fromEntries(names.map(name => [name, {
  name, checked: false, disabled: false, value: "", addEventListener() {},
}]));
const wrappers = Object.fromEntries(names.map(name => [name, {
  hidden: false, querySelectorAll: () => [elements[name]],
}]));
let update;
elements.report.addEventListener = (_, callback) => { update = callback; };
const form = { elements, querySelector: selector => wrappers[selector.match(/"(.*?)"/)[1]] };
const document = { querySelector: () => form, addEventListener: (_, callback) => callback() };
vm.runInNewContext(fs.readFileSync(path.join(__dirname, "../static/reporting/reports.js"), "utf8"), { document });

for (const report of ["users", "countries", "ages", "genders", "productions", "genres", "combinations", "recommended", "direct", "ratings", "comments", "videos", "added", "removed", "social", "comment_likes", "comment_dislikes", "video_likes", "video_dislikes", "follows"]) {
  elements.report.value = report;
  elements.custom.checked = false;
  elements.total.checked = false;
  update();
  assert.equal(wrappers.custom.hidden, false, report);
  assert.equal(elements.months.disabled, false, report);
  assert.equal(elements.years.disabled, false, report);
  assert.equal(elements.since.disabled, true, report);
  assert.equal(elements.until.disabled, true, report);
  elements.custom.checked = true;
  update({ target: elements.custom });
  assert.equal(elements.since.disabled, false, report);
  assert.equal(elements.until.disabled, false, report);
  assert.equal(elements.months.disabled, true, report);
  assert.equal(elements.years.disabled, true, report);
  assert.equal(elements.total.checked, false, report);
  elements.custom.checked = false;
  update({ target: elements.custom });
  assert.equal(elements.months.disabled, false, report);
  assert.equal(elements.years.disabled, false, report);
}
elements.report.value = "users";
elements.total.checked = true;
update({ target: elements.total });
assert.equal(elements.custom.disabled, true);
assert.equal(elements.months.disabled, true);
assert.equal(elements.since.disabled, true);
elements.report.value = "productions";
update({ target: elements.report });
assert.equal(elements.total.checked, false);
assert.equal(wrappers.total.hidden, true);
assert.equal(elements.custom.disabled, false);
assert.equal(elements.months.disabled, false);
assert.equal(elements.content_type.disabled, false);
elements.custom.checked = true;
update({ target: elements.custom });
elements.report.value = "social";
update({ target: elements.report });
assert.equal(elements.custom.checked, true);
assert.equal(elements.since.disabled, false);
assert.equal(elements.content_type.disabled, true);
console.log("Report form modes passed for all 20 reports.");

elements.report.value = "creator_eligibility";
elements.total.checked = true;
update({ target: elements.report });
assert.equal(elements.total.checked, false);
assert.equal(wrappers.total.hidden, true);
assert.equal(wrappers.min_followers.hidden, false);
assert.equal(elements.min_followers.disabled, false);
for (const name of ["countries", "ages", "genders", "content_type"]) {
  assert.equal(wrappers[name].hidden, true);
  assert.equal(elements[name].disabled, true);
}
assert.equal(elements.since.disabled, false);
elements.custom.checked = false;
update({ target: elements.custom });
assert.equal(elements.months.disabled, false);
assert.equal(elements.years.disabled, false);
assert.equal(elements.since.disabled, true);
elements.report.value = "users";
update({ target: elements.report });
assert.equal(wrappers.min_followers.hidden, true);
assert.equal(elements.min_followers.disabled, true);
assert.equal(elements.countries.disabled, false);
delete wrappers.min_followers;
update(); // Non-superuser forms have no threshold field at all.
console.log("Creator eligibility controls and forms without internal fields passed.");
