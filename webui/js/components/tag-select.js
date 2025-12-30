// webui/js/components/tag-select.js
// PDC · OL · OT order (as requested)

const OL_OPTIONS = ["OAR","OOH","OIM","OOL","OBR"];
const OT_OPTIONS = ["BULL","BEAR","TR"];
const PDC_OPTIONS = ["TR","BULL","BEAR"]; // keep simple and robust

export function renderTagSelect(mount){
  mount.innerHTML = `
    <div class="tag-select">
      ${dd("PDC","Previous Day Context", PDC_OPTIONS)}
      ${dd("OL","Open Location", OL_OPTIONS)}
      ${dd("OT","Opening Trend", OT_OPTIONS)}
    </div>
  `;

  const selects = mount.querySelectorAll("select.tag-dd");
  selects.forEach(sel => sel.addEventListener("change", () => {
    const detail = {
      PDC: getVal(mount, "PDC"),
      OL:  getVal(mount, "OL"),
      OT:  getVal(mount, "OT"),
    };
    mount.dispatchEvent(new CustomEvent("tagsChanged", { detail }));
  }));
}

function getVal(root, name){
  const el = root.querySelector(`select.tag-dd[data-name="${name}"]`);
  return el ? (el.value || "") : "";
}

function dd(name, label, options){
  const opts = [`<option value="">${label}</option>`]
    .concat(options.map(o => `<option value="${o}">${o}</option>`))
    .join("");
  return `
    <select class="tag-dd" data-name="${name}" title="${label}">
      ${opts}
    </select>
  `;
}
