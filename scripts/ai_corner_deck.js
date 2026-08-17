/**
 * SSD Bot — AI Corner Demo Deck
 * 11 slides, 16x9, engineer audience, live Slack demo slot
 */

const pptxgen = require("./node_modules/pptxgenjs");
const React   = require("./node_modules/react");
const RDS     = require("./node_modules/react-dom/server");
const sharp   = require("./node_modules/sharp");
const fs      = require("fs");

// ─── palette ────────────────────────────────────────────────────────────────
const C = {
  navy:    "1E3A5F",
  teal:    "0D9488",
  lteal:   "CCFBF1",
  mint:    "14B8A6",
  ice:     "EFF6FF",
  white:   "FFFFFF",
  dark:    "1E293B",
  gray:    "64748B",
  lgray:   "F1F5F9",
  gold:    "F59E0B",
  red:     "EF4444",
  card:    "F8FAFC",
};

// ─── icon helper ────────────────────────────────────────────────────────────
const fa  = require("./node_modules/react-icons/fa");
const md  = require("./node_modules/react-icons/md");
const bi  = require("./node_modules/react-icons/bi");

async function icon(Component, color, size = 256) {
  const svg = RDS.renderToStaticMarkup(
    React.createElement(Component, { color: `#${color}`, size: String(size) })
  );
  const buf = await sharp(Buffer.from(svg)).png().toBuffer();
  return "image/png;base64," + buf.toString("base64");
}

// ─── layout helpers ─────────────────────────────────────────────────────────
const W = 10, H = 5.625;
const MARGIN = 0.45;
const makeShadow = () => ({ type: "outer", color: "000000", blur: 8, offset: 2, angle: 45, opacity: 0.10 });

function card(slide, x, y, w, h, fill = C.card) {
  slide.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x, y, w, h,
    fill: { color: fill },
    line: { color: "E2E8F0", width: 0.5 },
    shadow: makeShadow(),
    rectRadius: 0.08,
  });
}

function darkCard(slide, x, y, w, h, fill = C.navy) {
  slide.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x, y, w, h,
    fill: { color: fill },
    line: { color: "FFFFFF", width: 0.3, transparency: 70 },
    rectRadius: 0.08,
  });
}

function slideTitle(slide, text, color = C.dark, y = 0.28) {
  slide.addText(text, {
    x: MARGIN, y, w: W - MARGIN * 2, h: 0.55,
    fontSize: 28, bold: true, color, fontFace: "Calibri", margin: 0,
  });
}

function divider(slide, y, color = C.teal) {
  slide.addShape(pres.shapes.RECTANGLE, {
    x: MARGIN, y, w: 0.4, h: 0.04,
    fill: { color },
    line: { color, width: 0 },
  });
}

function tag(slide, text, x, y, bg = C.teal, textColor = C.white) {
  slide.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x, y, w: text.length * 0.085 + 0.3, h: 0.28,
    fill: { color: bg },
    line: { color: bg, width: 0 },
    rectRadius: 0.04,
  });
  slide.addText(text, {
    x, y, w: text.length * 0.085 + 0.3, h: 0.28,
    fontSize: 10, bold: true, color: textColor, fontFace: "Calibri",
    align: "center", valign: "middle", margin: 0,
  });
}

// ─── presentation ────────────────────────────────────────────────────────────
const pres = new pptxgen();
pres.layout = "LAYOUT_16x9";
pres.title  = "SSD Bot — AI Agent for Pipeline Support";
pres.author = "Wendy";

// ════════════════════════════════════════════════════════════════════════════
// SLIDE 1 — Title (dark)
// ════════════════════════════════════════════════════════════════════════════
async function slide1() {
  const s = pres.addSlide();
  s.background = { color: C.navy };

  // background teal circle (decorative blob)
  s.addShape(pres.shapes.OVAL, {
    x: 7.2, y: -1.0, w: 4.5, h: 4.5,
    fill: { color: C.teal, transparency: 82 },
    line: { color: C.teal, width: 0 },
  });
  s.addShape(pres.shapes.OVAL, {
    x: -1.5, y: 3.5, w: 3.5, h: 3.5,
    fill: { color: C.mint, transparency: 95 },
    line: { color: C.mint, width: 0 },
  });

  // eyebrow
  s.addText("CONVIVA  ·  AI CORNER", {
    x: MARGIN, y: 0.55, w: 6, h: 0.3,
    fontSize: 10, color: C.mint, fontFace: "Calibri", bold: true,
    charSpacing: 3, margin: 0,
  });

  // main title
  s.addText("SSD Bot", {
    x: MARGIN, y: 1.1, w: 8, h: 1.35,
    fontSize: 72, bold: true, color: C.white, fontFace: "Calibri", margin: 0,
  });

  // subtitle
  s.addText("An AI Agent for Pipeline Support", {
    x: MARGIN, y: 2.5, w: 7.5, h: 0.55,
    fontSize: 22, color: C.lteal, fontFace: "Calibri", margin: 0,
  });

  // description line
  s.addText("From 30–60 min manual triage → under 1 minute, right in Slack", {
    x: MARGIN, y: 3.1, w: 7.5, h: 0.4,
    fontSize: 14, color: "93C5FD", fontFace: "Calibri", margin: 0, italic: true,
  });

  // pill tags
  tag(s, "15 min", MARGIN, 3.85, C.teal);
  tag(s, "Live Demo", MARGIN + 1.0, 3.85, C.gold, C.dark);
  tag(s, "Engineer Audience", MARGIN + 2.3, 3.85, "94A3B8", C.dark);

  // bottom line
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: H - 0.06, w: W, h: 0.06,
    fill: { color: C.teal }, line: { color: C.teal, width: 0 },
  });

  s.addNotes(
    "Welcome everyone. I'm going to spend about 15 minutes showing you a tool I built for the SSD team, then do a live demo in Slack. Feel free to ask questions as we go."
  );
  return s;
}

// ════════════════════════════════════════════════════════════════════════════
// SLIDE 2 — The Problem
// ════════════════════════════════════════════════════════════════════════════
async function slide2() {
  const s = pres.addSlide();
  s.background = { color: C.white };

  slideTitle(s, "Diagnosing a Pipeline Failure Is a Multi-System Hunt");
  divider(s, 0.87);

  // left col — pain points
  const painY = [1.1, 2.15, 3.2];
  const pains = [
    { icon: fa.FaSearch,  label: "Cross-system hunting",     desc: "Airflow logs → Jira history → Confluence runbooks → Slack threads. Each in a different tool." },
    { icon: fa.FaBrain,   label: "Tribal knowledge required", desc: "Senior engineers hold the context. New team members start from zero every time." },
    { icon: fa.FaClock,   label: "30–60 min per incident",    desc: "Every PagerDuty alert is a context switch. Multiply by several pipelines a week." },
  ];

  for (let i = 0; i < pains.length; i++) {
    const p = pains[i];
    const y = painY[i];
    // icon circle
    s.addShape(pres.shapes.OVAL, {
      x: MARGIN, y, w: 0.6, h: 0.6,
      fill: { color: i === 2 ? "FEF3C7" : C.lteal },
      line: { color: i === 2 ? C.gold : C.teal, width: 1 },
    });
    const ico = await icon(p.icon, i === 2 ? C.gold : C.teal, 128);
    s.addImage({ data: ico, x: MARGIN + 0.08, y: y + 0.08, w: 0.44, h: 0.44 });
    // label
    s.addText(p.label, {
      x: MARGIN + 0.75, y: y, w: 4.0, h: 0.3,
      fontSize: 14, bold: true, color: C.dark, fontFace: "Calibri", margin: 0,
    });
    s.addText(p.desc, {
      x: MARGIN + 0.75, y: y + 0.3, w: 4.0, h: 0.55,
      fontSize: 11, color: C.gray, fontFace: "Calibri", margin: 0,
    });
  }

  // right col — "The stack" visual
  const stackX = 5.8;
  const systems = [
    { name: "Airflow",    color: "017CEE", sub: "Task logs, run state" },
    { name: "Jira",       color: "0052CC", sub: "Past tickets, history" },
    { name: "Confluence", color: "172B4D", sub: "Runbooks, SOPs" },
    { name: "Slack",      color: "4A154B", sub: "Team threads, context" },
  ];
  s.addText("You need ALL of these:", {
    x: stackX, y: 0.95, w: 3.8, h: 0.3,
    fontSize: 11, color: C.gray, fontFace: "Calibri", italic: true, margin: 0,
  });
  systems.forEach((sys, i) => {
    card(s, stackX, 1.35 + i * 0.88, 3.8, 0.72, C.card);
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
      x: stackX + 0.12, y: 1.35 + i * 0.88 + 0.12, w: 0.48, h: 0.48,
      fill: { color: sys.color }, line: { color: sys.color, width: 0 }, rectRadius: 0.05,
    });
    s.addText(sys.name, {
      x: stackX + 0.72, y: 1.35 + i * 0.88 + 0.1, w: 2.9, h: 0.28,
      fontSize: 13, bold: true, color: C.dark, fontFace: "Calibri", margin: 0,
    });
    s.addText(sys.sub, {
      x: stackX + 0.72, y: 1.35 + i * 0.88 + 0.36, w: 2.9, h: 0.22,
      fontSize: 10, color: C.gray, fontFace: "Calibri", margin: 0,
    });
  });

  s.addNotes(
    "Every SSD pipeline failure requires an engineer to open 4+ tools, mentally connect the dots, and rely on experience they may or may not have. This is the problem I wanted to solve."
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SLIDE 3 — AI Pattern Primer
// ════════════════════════════════════════════════════════════════════════════
async function slide3() {
  const s = pres.addSlide();
  s.background = { color: C.white };

  slideTitle(s, "Quick AI Primer: Three Patterns, One Right Fit");
  divider(s, 0.87);

  s.addText("Since this is an AI Corner — let's name what we built correctly.", {
    x: MARGIN, y: 0.98, w: W - MARGIN * 2, h: 0.3,
    fontSize: 13, color: C.gray, fontFace: "Calibri", italic: true, margin: 0,
  });

  const patterns = [
    {
      name: "RAG",
      full: "Retrieval-Augmented Generation",
      desc: "User asks → fetch relevant docs → LLM answers with them as context. Static lookup, no decisions.",
      example: "Example: Q&A chatbot over a knowledge base",
      color: "E0F2FE", border: "7DD3FC", textColor: "075985",
      highlight: false,
    },
    {
      name: "Workflow",
      full: "Fixed Pipeline",
      desc: "Step 1 always runs, then Step 2, then Step 3. Hardcoded sequence. LLM may be one step.",
      example: "Example: ETL pipeline, scripted triage",
      color: "FEF3C7", border: "FCD34D", textColor: "92400E",
      highlight: false,
    },
    {
      name: "Agent",
      full: "Agentic Reasoning Loop",
      desc: "LLM decides what to do next based on what it finds. Calls tools, observes results, adapts. Handles novelty.",
      example: "SSD Bot: adapts its path per failure type",
      color: C.lteal, border: C.teal, textColor: C.navy,
      highlight: true,
    },
  ];

  const cardW = 2.85, cardH = 3.4, startX = MARGIN;
  for (let i = 0; i < patterns.length; i++) {
    const p = patterns[i];
    const x = startX + i * (cardW + 0.18);
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
      x, y: 1.35, w: cardW, h: cardH,
      fill: { color: p.color },
      line: { color: p.border, width: p.highlight ? 2.5 : 1 },
      shadow: p.highlight ? makeShadow() : undefined,
      rectRadius: 0.1,
    });
    if (p.highlight) {
      tag(s, "✓ This is us", x + cardW/2 - 0.6, 1.04, C.teal);
    }
    s.addText(p.name, {
      x, y: 1.55, w: cardW, h: 0.5,
      fontSize: 26, bold: true, color: p.textColor, fontFace: "Calibri",
      align: "center", margin: 0,
    });
    s.addText(p.full, {
      x: x + 0.15, y: 2.08, w: cardW - 0.3, h: 0.3,
      fontSize: 10, color: p.textColor, fontFace: "Calibri",
      align: "center", italic: true, margin: 0,
    });
    // thin divider
    s.addShape(pres.shapes.RECTANGLE, {
      x: x + 0.4, y: 2.42, w: cardW - 0.8, h: 0.02,
      fill: { color: p.border }, line: { color: p.border, width: 0 },
    });
    s.addText(p.desc, {
      x: x + 0.15, y: 2.52, w: cardW - 0.3, h: 1.3,
      fontSize: 11.5, color: C.dark, fontFace: "Calibri", margin: 0,
    });
    s.addText(p.example, {
      x: x + 0.15, y: 3.88, w: cardW - 0.3, h: 0.7,
      fontSize: 10, color: p.textColor, fontFace: "Calibri",
      italic: true, margin: 0,
    });
  }

  s.addNotes(
    "I want to be precise about what we built because the pattern matters — agents handle non-deterministic problems. Pipeline failures at Conviva are non-deterministic: a stuck sensor is investigated differently than a Spark job crash. A fixed workflow would break on edge cases constantly."
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SLIDE 4 — How the Agent Works (reasoning loop)
// ════════════════════════════════════════════════════════════════════════════
async function slide4() {
  const s = pres.addSlide();
  s.background = { color: C.lgray };

  slideTitle(s, "The Bot Reasons Its Way to the Answer");
  divider(s, 0.87);

  // loop boxes — horizontal layout
  // User Query → [THINK → PICK TOOL → CALL API → OBSERVE] loop → Answer
  const steps = [
    { label: "THINK",      sub: "What do I know?\nWhat's missing?",  color: C.navy,  tc: C.white  },
    { label: "PICK TOOL",  sub: "Which API gives\nme what I need?",  color: C.teal,  tc: C.white  },
    { label: "CALL API",   sub: "Airflow / Jira /\nConfluence / Slack", color: "0369A1", tc: C.white },
    { label: "OBSERVE",    sub: "Read result.\nEnough to answer?",   color: "7C3AED", tc: C.white  },
  ];

  // draw user query box
  const UX = 0.38, UY = 2.05, UW = 1.3, UH = 0.85;
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x: UX, y: UY, w: UW, h: UH,
    fill: { color: "334155" }, line: { color: "334155", width: 0 }, rectRadius: 0.08,
  });
  s.addText([
    { text: "User", options: { breakLine: true } },
    { text: "Query" }
  ], {
    x: UX, y: UY, w: UW, h: UH,
    fontSize: 13, bold: true, color: C.white, fontFace: "Calibri",
    align: "center", valign: "middle", margin: 0,
  });

  // arrow user→think
  s.addShape(pres.shapes.LINE, {
    x: UX + UW, y: UY + UH/2, w: 0.35, h: 0,
    line: { color: C.gray, width: 1.5, endArrowType: "arrow" },
  });

  // loop box background
  const LX = 1.95, LY = 1.45, LW = 6.2, LH = 2.0;
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x: LX, y: LY, w: LW, h: LH,
    fill: { color: C.white },
    line: { color: "CBD5E1", width: 1 },
    shadow: makeShadow(),
    rectRadius: 0.12,
  });
  s.addText("Agentic Loop  (up to 10 iterations)", {
    x: LX + 0.15, y: LY + 0.08, w: LW - 0.3, h: 0.28,
    fontSize: 10, color: C.gray, fontFace: "Calibri", italic: true, margin: 0,
  });

  // step boxes inside loop
  const SX0 = LX + 0.2, SY = LY + 0.44, SW = 1.35, SH = 0.72;
  const gap = (LW - 0.4 - 4 * SW) / 3;
  for (let i = 0; i < steps.length; i++) {
    const t = steps[i];
    const sx = SX0 + i * (SW + gap);
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
      x: sx, y: SY, w: SW, h: SH,
      fill: { color: t.color }, line: { color: t.color, width: 0 }, rectRadius: 0.07,
    });
    s.addText(t.label, {
      x: sx, y: SY + 0.05, w: SW, h: 0.25,
      fontSize: 10, bold: true, color: t.tc, fontFace: "Calibri",
      align: "center", margin: 0,
    });
    s.addText(t.sub, {
      x: sx + 0.05, y: SY + 0.3, w: SW - 0.1, h: 0.38,
      fontSize: 8.5, color: t.tc, fontFace: "Calibri",
      align: "center", margin: 0, italic: true,
    });
    // arrow between steps
    if (i < steps.length - 1) {
      s.addShape(pres.shapes.LINE, {
        x: sx + SW, y: SY + SH/2, w: gap, h: 0,
        line: { color: C.gray, width: 1.2, endArrowType: "arrow" },
      });
    }
  }

  // back arrow (loop): OBSERVE → THINK (below the boxes)
  const arrowY = SY + SH + 0.12;
  s.addShape(pres.shapes.LINE, {
    x: SX0 + 3*(SW+gap) + SW - 0.01, y: arrowY - 0.12 + 0.12, w: 0, h: 0.25,
    line: { color: C.gray, width: 1.2 },
  });
  s.addShape(pres.shapes.LINE, {
    x: SX0, y: arrowY + 0.25 - 0.12, w: 3*(SW+gap) + SW - 0.01, h: 0,
    line: { color: C.gray, width: 1.2 },
  });
  s.addShape(pres.shapes.LINE, {
    x: SX0, y: SY + SH, w: 0, h: 0.25,
    line: { color: C.gray, width: 1.2, endArrowType: "arrow" },
  });
  s.addText("if more info needed", {
    x: SX0 + 0.5, y: arrowY + 0.05, w: 3.5, h: 0.22,
    fontSize: 9, color: C.gray, fontFace: "Calibri", italic: true, margin: 0, align: "center",
  });

  // arrow loop→answer
  s.addShape(pres.shapes.LINE, {
    x: LX + LW, y: UY + UH/2, w: 0.3, h: 0,
    line: { color: C.gray, width: 1.5, endArrowType: "arrow" },
  });

  // answer box
  const AX = LX + LW + 0.3, AY = UY, AW = 1.3, AH = UH;
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x: AX, y: AY, w: AW, h: AH,
    fill: { color: C.teal }, line: { color: C.teal, width: 0 }, rectRadius: 0.08,
  });
  s.addText("Answer\nin Slack", {
    x: AX, y: AY, w: AW, h: AH,
    fontSize: 13, bold: true, color: C.white, fontFace: "Calibri",
    align: "center", valign: "middle", margin: 0,
  });

  // bottom note
  s.addText("Claude (Anthropic API) drives the reasoning. Your query never needs a fixed script.", {
    x: MARGIN, y: H - 0.6, w: W - MARGIN * 2, h: 0.35,
    fontSize: 11, color: C.gray, fontFace: "Calibri", italic: true, margin: 0, align: "center",
  });

  s.addNotes(
    "The key insight: the LLM decides what to do next, not the code. The code provides tools and enforces a max iteration limit. The reasoning adapts — if the run isn't failed but stuck, it checks different things. If the task is a sensor, it looks upstream. This is why it handles novel failure modes."
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SLIDE 5 — The Toolbox
// ════════════════════════════════════════════════════════════════════════════
async function slide5() {
  const s = pres.addSlide();
  s.background = { color: C.white };

  slideTitle(s, "Six Tools — Infinite Investigation Paths");
  divider(s, 0.87);

  const tools = [
    { icon: fa.FaHistory,    color: "017CEE", label: "DAG Run History",    desc: "Find the most recent failed run and its run_id" },
    { icon: fa.FaFileAlt,    color: C.teal,   label: "Task Log Fetch",     desc: "Pull actual log text — the last 200 lines of a task attempt" },
    { icon: fa.FaCode,       color: "7C3AED", label: "DAG Source Code",    desc: "Read the pipeline Python to understand sensors and dependencies" },
    { icon: fa.FaJira,       color: "0052CC", label: "Jira Search",        desc: "Search CE, DFS, SE projects for similar past incidents" },
    { icon: fa.FaBook,       color: "172B4D", label: "Confluence Lookup",  desc: "Fetch runbooks, SOPs, and customer-specific procedures" },
    { icon: fa.FaSlack,      color: "4A154B", label: "Slack History",      desc: "Search past channel threads for context on this pipeline" },
  ];

  const cols = 3, TW = 2.85, TH = 1.35;
  const startX = MARGIN, startY = 1.1;
  const gapX = (W - MARGIN * 2 - cols * TW) / (cols - 1);
  const gapY = 0.22;

  for (let i = 0; i < tools.length; i++) {
    const t = tools[i];
    const col = i % cols, row = Math.floor(i / cols);
    const tx = startX + col * (TW + gapX);
    const ty = startY + row * (TH + gapY);

    card(s, tx, ty, TW, TH);

    // icon circle
    s.addShape(pres.shapes.OVAL, {
      x: tx + 0.18, y: ty + 0.22, w: 0.55, h: 0.55,
      fill: { color: t.color, transparency: 88 },
      line: { color: t.color, width: 1.5 },
    });
    const ico = await icon(t.icon, t.color, 128);
    s.addImage({ data: ico, x: tx + 0.26, y: ty + 0.3, w: 0.39, h: 0.39 });

    s.addText(t.label, {
      x: tx + 0.88, y: ty + 0.22, w: TW - 1.0, h: 0.3,
      fontSize: 12.5, bold: true, color: C.dark, fontFace: "Calibri", margin: 0,
    });
    s.addText(t.desc, {
      x: tx + 0.88, y: ty + 0.54, w: TW - 1.0, h: 0.58,
      fontSize: 10.5, color: C.gray, fontFace: "Calibri", margin: 0,
    });
  }

  s.addNotes(
    "The bot has 6 tools. On any given query it might use 2 or 5 depending on what it finds. Task logs are the most valuable — you get the actual Spark exception, not just a status badge. Jira search surfaces past incidents with similar signatures — RAG-style recall within the agent."
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SLIDE 6 — Live Demo (dark)
// ════════════════════════════════════════════════════════════════════════════
async function slide6() {
  const s = pres.addSlide();
  s.background = { color: C.dark };

  // decorative circles
  s.addShape(pres.shapes.OVAL, {
    x: 6.5, y: -0.8, w: 4.5, h: 4.5,
    fill: { color: C.teal, transparency: 94 },
    line: { color: C.teal, width: 0 },
  });

  const demoIco = await icon(fa.FaPlay, C.white, 128);
  s.addShape(pres.shapes.OVAL, {
    x: 0.8, y: 1.55, w: 1.1, h: 1.1,
    fill: { color: C.teal }, line: { color: C.teal, width: 0 },
  });
  s.addImage({ data: demoIco, x: 1.08, y: 1.83, w: 0.54, h: 0.54 });

  s.addText("Live Demo", {
    x: 2.2, y: 1.55, w: 7, h: 0.75,
    fontSize: 48, bold: true, color: C.white, fontFace: "Calibri", margin: 0,
  });
  s.addText("We'll ask the bot in Slack right now:", {
    x: 2.2, y: 2.38, w: 7, h: 0.35,
    fontSize: 16, color: C.lteal, fontFace: "Calibri", italic: true, margin: 0,
  });

  // command box
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x: 0.8, y: 2.95, w: 8.4, h: 0.85,
    fill: { color: "0F172A" },
    line: { color: C.teal, width: 1.5 },
    rectRadius: 0.08,
  });
  s.addText("why is the Echostar DPI event pipeline failing?", {
    x: 1.1, y: 2.95, w: 8.0, h: 0.85,
    fontSize: 18, color: C.mint, fontFace: "Courier New",
    valign: "middle", margin: 0,
  });

  s.addText("Watch it call Airflow, read the task log, and explain the root cause — automatically.", {
    x: MARGIN, y: 4.1, w: W - MARGIN * 2, h: 0.35,
    fontSize: 12, color: "94A3B8", fontFace: "Calibri", italic: true,
    align: "center", margin: 0,
  });

  s.addNotes(
    "SWITCH TO SLACK NOW. Open the #ssd-support channel (or your demo channel). Type the exact prompt shown. Point out what the bot is doing at each step — the thinking messages, which tool it calls, what it returns. Let it finish before explaining the output."
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SLIDE 7 — What the Bot Found (Echostar example)
// ════════════════════════════════════════════════════════════════════════════
async function slide7() {
  const s = pres.addSlide();
  s.background = { color: C.white };

  slideTitle(s, "What Just Happened: Root Cause in 3 Tool Calls");
  divider(s, 0.87);

  // left — error box
  s.addText("The Actual Exception (buried in 200+ log lines):", {
    x: MARGIN, y: 1.0, w: 4.8, h: 0.28,
    fontSize: 11, color: C.gray, fontFace: "Calibri", italic: true, margin: 0,
  });
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x: MARGIN, y: 1.32, w: 4.8, h: 1.5,
    fill: { color: "0F172A" }, line: { color: "334155", width: 0.5 }, rectRadius: 0.08,
  });
  s.addText([
    { text: "AnalysisException:", options: { color: C.red, bold: true, breakLine: true } },
    { text: "CSV data source does not\nsupport ", options: { color: "E2E8F0", breakLine: false } },
    { text: "map<string,string>", options: { color: C.gold, bold: true, breakLine: true } },
    { text: "data type.\n\n", options: { color: "E2E8F0", breakLine: true } },
    { text: "at FileLoader$.sinkFile(FileLoader.scala:109)", options: { color: "64748B", italic: true } },
  ], {
    x: MARGIN + 0.18, y: 1.44, w: 4.44, h: 1.26,
    fontSize: 10.5, fontFace: "Courier New", margin: 0, valign: "top",
  });

  // middle arrow
  const arrowIco = await icon(fa.FaArrowRight, C.teal, 128);
  s.addImage({ data: arrowIco, x: 5.15, y: 2.3, w: 0.5, h: 0.5 });

  // right — bot's plain English answer
  s.addText("Bot's Plain-English Explanation:", {
    x: 5.85, y: 1.0, w: 3.7, h: 0.28,
    fontSize: 11, color: C.gray, fontFace: "Calibri", italic: true, margin: 0,
  });
  card(s, 5.85, 1.32, 3.7, 1.5, C.lteal);
  s.addText(
    "The Spark job crashed when writing to CSV. One column in the DPI event schema is typed map<string,string> — CSV can't serialize map types. This has happened all 4 retry attempts. Fix: stringify the map column with to_json() before writing, or switch format to Parquet.",
    {
      x: 6.05, y: 1.44, w: 3.3, h: 1.26,
      fontSize: 11, color: C.navy, fontFace: "Calibri", margin: 0, valign: "top",
    }
  );

  // tool call chain
  s.addText("Steps the bot took:", {
    x: MARGIN, y: 3.02, w: W - MARGIN * 2, h: 0.28,
    fontSize: 11, color: C.gray, fontFace: "Calibri", italic: true, margin: 0, align: "center",
  });

  const steps2 = [
    { n: "1", label: "get_airflow_dag_runs", sub: "state=failed", color: "017CEE" },
    { n: "2", label: "get_airflow_task_instances", sub: "find failed task + try_number", color: C.teal },
    { n: "3", label: "get_airflow_task_log", sub: "read last 200 lines → find exception", color: "7C3AED" },
  ];
  const chainW = 2.8, chainH = 0.9;
  const chainStartX = (W - 3 * chainW - 2 * 0.25) / 2;
  for (let i = 0; i < steps2.length; i++) {
    const t = steps2[i];
    const cx = chainStartX + i * (chainW + 0.25);
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
      x: cx, y: 3.4, w: chainW, h: chainH,
      fill: { color: t.color, transparency: 88 },
      line: { color: t.color, width: 1.5 }, rectRadius: 0.08,
    });
    s.addShape(pres.shapes.OVAL, {
      x: cx + 0.12, y: 3.52, w: 0.3, h: 0.3,
      fill: { color: t.color }, line: { color: t.color, width: 0 },
    });
    s.addText(t.n, {
      x: cx + 0.12, y: 3.52, w: 0.3, h: 0.3,
      fontSize: 9, bold: true, color: C.white, fontFace: "Calibri",
      align: "center", valign: "middle", margin: 0,
    });
    s.addText(t.label, {
      x: cx + 0.52, y: 3.5, w: chainW - 0.62, h: 0.28,
      fontSize: 10, bold: true, color: C.dark, fontFace: "Calibri", margin: 0,
    });
    s.addText(t.sub, {
      x: cx + 0.52, y: 3.78, w: chainW - 0.62, h: 0.44,
      fontSize: 9, color: C.gray, fontFace: "Calibri", italic: true, margin: 0,
    });
    if (i < steps2.length - 1) {
      s.addShape(pres.shapes.LINE, {
        x: cx + chainW, y: 3.4 + chainH/2, w: 0.25, h: 0,
        line: { color: C.gray, width: 1.2, endArrowType: "arrow" },
      });
    }
  }

  s.addNotes(
    "This is the Echostar Test pipeline from June 23rd. The exception is on attempt 4 of 4 — it's been failing consistently. The bot found the map<string,string> issue by reading the log via Airflow REST API. The engineering fix is to call to_json() on that column before the CSV write."
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SLIDE 8 — Under the Hood
// ════════════════════════════════════════════════════════════════════════════
async function slide8() {
  const s = pres.addSlide();
  s.background = { color: C.lgray };

  slideTitle(s, "Under the Hood: Where the Intelligence Lives");
  divider(s, 0.87);

  const pillars = [
    {
      icon: fa.FaBrain,
      color: C.navy,
      label: "Domain Knowledge",
      desc: "The system prompt encodes Conviva-specific triage logic: which Airflow instance hosts which pipeline type, sensor vs. processing task investigation paths, customer-specific context.",
    },
    {
      icon: fa.FaHistory,
      color: C.teal,
      label: "100-Ticket Foundation",
      desc: "I analyzed a year of real SSD escalation tickets to identify recurring patterns. That institutional knowledge is baked into the prompt — every new query benefits from it.",
    },
    {
      icon: fa.FaTools,
      color: "7C3AED",
      label: "Tool Selection Logic",
      desc: "The model decides which tools to call and in what order. It knows to use task logs for processing failures, upstream sensors for data-missing issues, Jira for pattern matching.",
    },
  ];

  const pW = 2.85, pH = 3.5, pY = 1.1;
  const pGap = (W - MARGIN * 2 - 3 * pW) / 2;
  for (let i = 0; i < pillars.length; i++) {
    const p = pillars[i];
    const px = MARGIN + i * (pW + pGap);
    card(s, px, pY, pW, pH, C.white);
    s.addShape(pres.shapes.OVAL, {
      x: px + pW/2 - 0.38, y: pY + 0.25, w: 0.76, h: 0.76,
      fill: { color: p.color, transparency: 88 },
      line: { color: p.color, width: 1.5 },
    });
    const ico = await icon(p.icon, p.color, 128);
    s.addImage({ data: ico, x: px + pW/2 - 0.27, y: pY + 0.36, w: 0.54, h: 0.54 });
    s.addText(p.label, {
      x: px + 0.15, y: pY + 1.2, w: pW - 0.3, h: 0.35,
      fontSize: 13.5, bold: true, color: C.dark, fontFace: "Calibri",
      align: "center", margin: 0,
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: px + pW/2 - 0.6, y: pY + 1.58, w: 1.2, h: 0.03,
      fill: { color: p.color }, line: { color: p.color, width: 0 },
    });
    s.addText(p.desc, {
      x: px + 0.2, y: pY + 1.7, w: pW - 0.4, h: 1.65,
      fontSize: 11, color: C.gray, fontFace: "Calibri", margin: 0,
    });
  }

  s.addNotes(
    "The system prompt is where most of the value lives. A generic LLM knows what Airflow is. Our bot knows WHICH Airflow, HOW we name DAGs, WHAT error patterns mean in Conviva's context. That specificity is what makes it useful versus generic."
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SLIDE 9 — Impact (dark)
// ════════════════════════════════════════════════════════════════════════════
async function slide9() {
  const s = pres.addSlide();
  s.background = { color: C.navy };

  // eyebrow
  s.addText("IMPACT", {
    x: MARGIN, y: 0.38, w: W - MARGIN * 2, h: 0.3,
    fontSize: 10, color: C.mint, fontFace: "Calibri", bold: true, charSpacing: 4, margin: 0,
  });
  slideTitle(s, "What Changes When Investigation Becomes Instant", C.white, 0.72);

  const stats = [
    { num: "< 1 min",  label: "triage time",          sub: "vs. 30–60 min manual",       col: C.mint   },
    { num: "100",      label: "tickets analyzed",      sub: "to build the knowledge base", col: C.gold   },
    { num: "6",        label: "tools per query",       sub: "Airflow, Jira, Confluence,\nSlack, DAG source & more", col: "93C5FD" },
    { num: "10",       label: "reasoning iterations",  sub: "max per query, adapts to complexity", col: "C4B5FD" },
  ];

  const sW = 2.0, sH = 2.2, sY = 1.55;
  const sGap = (W - MARGIN * 2 - 4 * sW) / 3;
  for (let i = 0; i < stats.length; i++) {
    const t = stats[i];
    const sx = MARGIN + i * (sW + sGap);
    darkCard(s, sx, sY, sW, sH, "1E3A5F");
    s.addText(t.num, {
      x: sx, y: sY + 0.22, w: sW, h: 0.75,
      fontSize: t.num.includes(" ") ? 28 : 44, bold: true, color: t.col, fontFace: "Calibri",
      align: "center", margin: 0,
    });
    s.addText(t.label, {
      x: sx + 0.1, y: sY + 1.02, w: sW - 0.2, h: 0.35,
      fontSize: 13, bold: true, color: C.white, fontFace: "Calibri",
      align: "center", margin: 0,
    });
    s.addText(t.sub, {
      x: sx + 0.1, y: sY + 1.4, w: sW - 0.2, h: 0.55,
      fontSize: 10, color: "93C5FD", fontFace: "Calibri",
      align: "center", italic: true, margin: 0,
    });
  }

  // bottom insight
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x: MARGIN, y: H - 0.85, w: W - MARGIN * 2, h: 0.58,
    fill: { color: "0F172A" }, line: { color: C.teal, width: 1 }, rectRadius: 0.07,
  });
  s.addText("New engineers and 10-year veterans now have the same starting point when a PagerDuty alert fires at 2am.", {
    x: MARGIN + 0.25, y: H - 0.82, w: W - MARGIN * 2 - 0.5, h: 0.52,
    fontSize: 12, color: C.lteal, fontFace: "Calibri", italic: true,
    valign: "middle", margin: 0, align: "center",
  });

  s.addNotes(
    "The time saving is real, but the knowledge democratization is the bigger win long-term. Tribal knowledge is a fragility risk — when the person who knows leaves, the team regresses. This bot externalizes that knowledge."
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SLIDE 10 — What's Next
// ════════════════════════════════════════════════════════════════════════════
async function slide10() {
  const s = pres.addSlide();
  s.background = { color: C.white };

  slideTitle(s, "What's Next");
  divider(s, 0.87);

  const items = [
    {
      n: "1",
      title: "Structured Incident Response",
      desc: "When PagerDuty fires, auto-produce: blast radius assessment, affected customers list, draft stakeholder communication. Currently the bot responds; next it proactively prepares.",
      status: "Planned",
      statusColor: C.teal,
      icon: fa.FaBell,
      color: C.teal,
    },
    {
      n: "2",
      title: "Persistent Incident Memory",
      desc: "After a resolved incident, the bot learns from the fix. Future similar failures get a richer answer — not just the root cause but the proven resolution path.",
      status: "Planned",
      statusColor: "7C3AED",
      icon: fa.FaDatabase,
      color: "7C3AED",
    },
    {
      n: "3",
      title: "Thread State Persistence",
      desc: "Bot state currently resets on restart. Engineering fix to persist active thread context so the 'yes/no' confirmation flow survives restarts.",
      status: "In Backlog",
      statusColor: C.gold,
      icon: fa.FaComments,
      color: C.gold,
    },
  ];

  const iW = 2.85, iH = 3.3, iY = 1.08;
  const iGap = (W - MARGIN * 2 - 3 * iW) / 2;
  for (let i = 0; i < items.length; i++) {
    const t = items[i];
    const ix = MARGIN + i * (iW + iGap);
    card(s, ix, iY, iW, iH, C.card);
    s.addShape(pres.shapes.OVAL, {
      x: ix + iW/2 - 0.35, y: iY + 0.2, w: 0.7, h: 0.7,
      fill: { color: t.color, transparency: 88 },
      line: { color: t.color, width: 1.5 },
    });
    const ico = await icon(t.icon, t.color, 128);
    s.addImage({ data: ico, x: ix + iW/2 - 0.25, y: iY + 0.3, w: 0.5, h: 0.5 });
    tag(s, t.status, ix + 0.18, iY + 1.05, t.statusColor);
    s.addText(t.title, {
      x: ix + 0.18, y: iY + 1.42, w: iW - 0.36, h: 0.5,
      fontSize: 13, bold: true, color: C.dark, fontFace: "Calibri", margin: 0,
    });
    s.addText(t.desc, {
      x: ix + 0.18, y: iY + 1.96, w: iW - 0.36, h: 1.16,
      fontSize: 10.5, color: C.gray, fontFace: "Calibri", margin: 0,
    });
  }

  s.addNotes(
    "The bot is in production now for the SSD team. These three items are the natural next steps based on how engineers actually use it. Happy to discuss prioritization or any of these in more depth."
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SLIDE 11 — Discussion / Q&A (dark closing)
// ════════════════════════════════════════════════════════════════════════════
async function slide11() {
  const s = pres.addSlide();
  s.background = { color: C.dark };

  s.addShape(pres.shapes.OVAL, {
    x: 7.0, y: -1.0, w: 5.0, h: 5.0,
    fill: { color: C.teal, transparency: 85 },
    line: { color: C.teal, width: 0 },
  });

  s.addText("Let's Talk", {
    x: MARGIN, y: 1.1, w: 7, h: 0.9,
    fontSize: 52, bold: true, color: C.white, fontFace: "Calibri", margin: 0,
  });

  const qs = [
    "How would you use this for your own team's pipelines?",
    "What tools would you add to the toolbox?",
    "Where do you see the biggest risk or limitation?",
  ];
  for (let i = 0; i < qs.length; i++) {
    s.addShape(pres.shapes.OVAL, {
      x: MARGIN, y: 2.3 + i * 0.65, w: 0.28, h: 0.28,
      fill: { color: C.teal }, line: { color: C.teal, width: 0 },
    });
    s.addText(`${i + 1}`, {
      x: MARGIN, y: 2.3 + i * 0.65, w: 0.28, h: 0.28,
      fontSize: 9, bold: true, color: C.white, fontFace: "Calibri",
      align: "center", valign: "middle", margin: 0,
    });
    s.addText(qs[i], {
      x: MARGIN + 0.42, y: 2.3 + i * 0.65, w: 6, h: 0.38,
      fontSize: 14, color: C.lteal, fontFace: "Calibri", margin: 0, valign: "middle",
    });
  }

  s.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: H - 0.06, w: W, h: 0.06,
    fill: { color: C.teal }, line: { color: C.teal, width: 0 },
  });
  s.addText("github/internal  ·  #ssd-support  ·  dwan@conviva.com", {
    x: MARGIN, y: H - 0.45, w: W - MARGIN * 2, h: 0.3,
    fontSize: 10, color: "94A3B8", fontFace: "Calibri", margin: 0, align: "center",
  });

  s.addNotes(
    "Open floor. Suggested discussion starters on screen. If the group is quiet, try: 'Who here has spent more than an hour debugging a pipeline failure in the last month?' — then show how the bot would have cut that down."
  );
}

// ─── main ────────────────────────────────────────────────────────────────────
(async () => {
  await slide1();
  await slide2();
  await slide3();
  await slide4();
  await slide5();
  await slide6();
  await slide7();
  await slide8();
  await slide9();
  await slide10();
  await slide11();

  const outPath = "/sessions/clever-zealous-einstein/mnt/outputs/SSD_Bot_AI_Corner.pptx";
  await pres.writeFile({ fileName: outPath });
  console.log("Written:", outPath);
})();
