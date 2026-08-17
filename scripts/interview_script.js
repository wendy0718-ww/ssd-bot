const { Document, Packer, Paragraph, TextRun, HeadingLevel, AlignmentType,
        BorderStyle, ShadingType, WidthType, Table, TableRow, TableCell } = require('docx');
const fs = require('fs');

const BLUE   = "1F4E79";
const LBLUE  = "D6E4F0";
const ACCENT = "2E75B6";
const GRAY   = "F5F7FA";

const contentWidth = 9360; // US Letter 1" margins

function hr() {
  return new Paragraph({
    border: { bottom: { style: BorderStyle.SINGLE, size: 8, color: ACCENT, space: 1 } },
    spacing: { before: 0, after: 180 },
    children: [new TextRun("")]
  });
}

function gap(n = 120) {
  return new Paragraph({ spacing: { before: 0, after: n }, children: [new TextRun(" ")] });
}

function sectionLabel(text) {
  return new Paragraph({
    spacing: { before: 240, after: 80 },
    children: [new TextRun({ text: text.toUpperCase(), bold: true, size: 18, color: ACCENT, font: "Arial" })]
  });
}

function body(text, opts = {}) {
  return new Paragraph({
    spacing: { before: 0, after: 160 },
    children: [new TextRun({ text, size: 22, font: "Arial", ...opts })]
  });
}

function note(text) {
  return new Paragraph({
    spacing: { before: 0, after: 120 },
    shading: { fill: GRAY, type: ShadingType.CLEAR },
    children: [new TextRun({ text: `[${text}]`, size: 20, font: "Arial", italics: true, color: "888888" })]
  });
}

function blockQuote(text) {
  return new Paragraph({
    spacing: { before: 80, after: 80 },
    indent: { left: 360, right: 360 },
    border: {
      left: { style: BorderStyle.SINGLE, size: 12, color: ACCENT, space: 12 }
    },
    children: [new TextRun({ text, size: 22, font: "Arial", italics: true })]
  });
}

function highlightBox(label, value) {
  return new Table({
    width: { size: contentWidth, type: WidthType.DXA },
    columnWidths: [2000, 7360],
    rows: [new TableRow({
      children: [
        new TableCell({
          width: { size: 2000, type: WidthType.DXA },
          shading: { fill: ACCENT, type: ShadingType.CLEAR },
          margins: { top: 80, bottom: 80, left: 120, right: 120 },
          verticalAlign: "center",
          children: [new Paragraph({
            alignment: AlignmentType.CENTER,
            children: [new TextRun({ text: label, bold: true, size: 20, color: "FFFFFF", font: "Arial" })]
          })]
        }),
        new TableCell({
          width: { size: 7360, type: WidthType.DXA },
          shading: { fill: GRAY, type: ShadingType.CLEAR },
          margins: { top: 80, bottom: 80, left: 180, right: 120 },
          children: [new Paragraph({
            children: [new TextRun({ text: value, size: 21, font: "Arial" })]
          })]
        })
      ]
    })]
  });
}

const doc = new Document({
  styles: {
    default: { document: { run: { font: "Arial", size: 22 } } },
    paragraphStyles: [
      {
        id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 36, bold: true, color: BLUE, font: "Arial" },
        paragraph: { spacing: { before: 0, after: 160 }, outlineLevel: 0 }
      },
      {
        id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 26, bold: true, color: BLUE, font: "Arial" },
        paragraph: { spacing: { before: 200, after: 100 }, outlineLevel: 1 }
      }
    ]
  },
  sections: [{
    properties: {
      page: {
        size: { width: 12240, height: 15840 },
        margin: { top: 1080, right: 1440, bottom: 1080, left: 1440 }
      }
    },
    children: [

      // ── TITLE BLOCK ──
      new Paragraph({
        spacing: { before: 0, after: 80 },
        children: [new TextRun({ text: "Interview Project Introduction Script", bold: true, size: 40, color: BLUE, font: "Arial" })]
      }),
      new Paragraph({
        spacing: { before: 0, after: 60 },
        children: [new TextRun({ text: "AI-Powered Pipeline Support Agent — SSD Bot", size: 26, color: ACCENT, font: "Arial", italics: true })]
      }),
      new Paragraph({
        spacing: { before: 0, after: 40 },
        children: [new TextRun({ text: "Role: Lead Technical Support Engineer", size: 20, color: "666666", font: "Arial" })]
      }),
      new Paragraph({
        spacing: { before: 0, after: 40 },
        children: [new TextRun({ text: "Delivery time: ~2.5–3 minutes spoken", size: 20, color: "666666", font: "Arial" })]
      }),
      hr(),
      gap(80),

      // ── QUICK REFERENCE ──
      sectionLabel("Quick Stats at a Glance"),
      gap(40),
      highlightBox("Built with",  "Claude API (Anthropic) • Python Slack Bolt • Airflow REST API • Jira / Confluence APIs"),
      gap(60),
      highlightBox("Pattern",     "AI Agent — agentic reasoning loop, up to 10 tool calls per query"),
      gap(60),
      highlightBox("Tools",       "Airflow log fetch • Jira ticket search • Confluence runbook lookup • Slack history search"),
      gap(60),
      highlightBox("Data basis",  "100 real escalation tickets analyzed • 3 Jira projects • Multiple Airflow instances"),
      gap(60),
      highlightBox("Impact",      "30–60 min manual investigation → under 1 minute automated root cause analysis"),
      gap(160),

      // ═══════════════════════════════════════════
      // SECTION 1 — OPENING
      // ═══════════════════════════════════════════
      new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun({ text: "01  Opening  —  Set the Context", font: "Arial", bold: true, size: 36, color: BLUE })] }),
      note("~30 seconds • Establish the business problem before talking about tech"),
      gap(60),

      blockQuote(
        "“One of the projects I’m most proud of recently is an AI-powered support bot I built for our Structured Data Delivery team at Conviva. We manage data pipeline deliveries for large streaming customers — Echostar, SlingTV, QVC — and when a pipeline fails, the on-call engineer has to manually cross-reference Airflow task logs, Jira tickets, Confluence runbooks, and Slack history just to understand what went wrong. That process can take 30 to 60 minutes every time, and it requires a lot of tribal knowledge that’s hard to transfer.”"
      ),
      gap(80),
      note("Pause here. Let the problem land before jumping to the solution."),
      gap(160),

      // ═══════════════════════════════════════════
      // SECTION 2 — WHAT I BUILT
      // ═══════════════════════════════════════════
      new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun({ text: "02  What I Built", font: "Arial", bold: true, size: 36, color: BLUE })] }),
      note("~45 seconds • Lead with behavior, then explain the mechanism"),
      gap(60),

      blockQuote(
        "“I built a Slack bot that works as an AI agent. When an engineer asks ‘why is the Echostar pipeline failing?’, the bot doesn’t just look up a status — it reasons through the problem step by step. It calls the Airflow REST API to find the failed run, identifies which specific task failed, fetches the actual task log from the worker node, extracts the error, and then cross-references past Jira cases for similar incidents. It synthesizes all of that into a plain-language root cause explanation, right in Slack.”"
      ),
      gap(100),

      blockQuote(
        "“I’m using the word ‘agent’ deliberately — this isn’t a scripted workflow or a keyword lookup. The Claude API decides which tool to call next based on what it discovers at each step. If the run is stuck instead of failed, it takes a different path. If the failing task is a sensor, it checks the upstream dependency instead. That adaptive reasoning is what makes it genuinely useful.”"
      ),
      gap(160),

      // ═══════════════════════════════════════════
      // SECTION 3 — TECHNICAL ARCHITECTURE
      // ═══════════════════════════════════════════
      new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun({ text: "03  Technical Architecture", font: "Arial", bold: true, size: 36, color: BLUE })] }),
      note("~30 seconds • Show depth without drowning in detail"),
      gap(60),

      blockQuote(
        "“Under the hood it’s built on the Anthropic Claude API with an agentic reasoning loop — up to ten tool calls per query. The tools include Airflow REST API integration across multiple instances, Jira search across three projects, Confluence runbook lookup, and Slack history search. To inform the bot’s triage logic, I analyzed 100 real escalation tickets from the past year, identified the top recurring failure patterns, and encoded that institutional knowledge directly into the system prompt.”"
      ),
      gap(160),

      // ═══════════════════════════════════════════
      // SECTION 4 — IMPACT
      // ═══════════════════════════════════════════
      new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun({ text: "04  Business Impact", font: "Arial", bold: true, size: 36, color: BLUE })] }),
      note("~30 seconds • Always quantify where you can"),
      gap(60),

      blockQuote(
        "“The impact is meaningful. What used to take a senior engineer 30 to 60 minutes of manual investigation can now be surfaced in under a minute. But more importantly, it captures institutional knowledge that previously lived only in people’s heads. A new team member and a ten-year veteran now have the same starting point when a PagerDuty alert fires at 2am.”"
      ),
      gap(160),

      // ═══════════════════════════════════════════
      // SECTION 5 — LEADERSHIP / DESIGN JUDGMENT
      // ═══════════════════════════════════════════
      new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun({ text: "05  Design Judgment & Leadership", font: "Arial", bold: true, size: 36, color: BLUE })] }),
      note("~30 seconds • This is the differentiator for a ‘Lead’ role — show you make architectural decisions, not just execute them"),
      gap(60),

      blockQuote(
        "“What I’m particularly proud of is the design judgment involved. I had to choose between several AI patterns — a simple knowledge base lookup, a hardcoded workflow, or a true reasoning agent. I chose the agent pattern specifically because pipeline failures at Conviva are non-deterministic — each one has a unique investigation path depending on what you find. A fixed workflow would have broken on edge cases constantly. The agent handles novelty.”"
      ),
      gap(100),

      blockQuote(
        "“I also pushed back on one early direction — there was a suggestion to add proactive monitoring. My view was that Airflow already has its own alerting system, so layering another monitor on top would just create noise without adding value. The real leverage was in response quality: when the alert fires, make the triage instant and accurate.”"
      ),
      gap(160),

      // ═══════════════════════════════════════════
      // SECTION 6 — CLOSING
      // ═══════════════════════════════════════════
      new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun({ text: "06  Closing", font: "Arial", bold: true, size: 36, color: BLUE })] }),
      note("~15 seconds • Land on a theme that connects to the role you’re interviewing for"),
      gap(60),

      blockQuote(
        "“This project sits at the intersection of my technical background in data pipelines and my support expertise. It’s a good example of how I think about leverage as a lead: instead of just getting faster at manual investigation, I automated the investigation itself — and made every engineer on the team faster in the process.”"
      ),
      gap(160),

      hr(),

      // ═══════════════════════════════════════════
      // FOLLOW-UP Q&A PREP
      // ═══════════════════════════════════════════
      new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun({ text: "Likely Follow-Up Questions", font: "Arial", bold: true, size: 36, color: BLUE })] }),
      gap(80),

      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "Q: What’s the difference between an agent and a regular chatbot?", font: "Arial", bold: true, size: 26, color: BLUE })] }),
      body("A regular chatbot responds from what it already knows. An agent has tools it can call — APIs, databases, logs — and it decides which to use and in what order based on what it discovers. The reasoning loop is the key difference: observe, think, act, repeat."),
      gap(80),

      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "Q: How accurate is it? What happens when it’s wrong?", font: "Arial", bold: true, size: 26, color: BLUE })] }),
      body("It reads actual logs and actual Jira tickets — it’s not hallucinating data. The analysis interpretation can be imperfect, which is why it’s designed as a first-responder aid, not an autonomous fixer. Engineers still make the call. The bot accelerates triage, not decision-making."),
      gap(80),

      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "Q: How did you decide what tools to give it?", font: "Arial", bold: true, size: 26, color: BLUE })] }),
      body("I analyzed 100 real escalation tickets to see what information engineers actually needed to resolve issues. Airflow logs, past Jira cases, and Confluence runbooks came up in the vast majority of cases. I started with those and iterated based on real usage."),
      gap(80),

      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "Q: What was the hardest technical challenge?", font: "Arial", bold: true, size: 26, color: BLUE })] }),
      body("Getting the system prompt right. The bot needs to know Conviva-specific context — which Airflow instance hosts which pipeline type, how to interpret SSD-specific error patterns, when to check upstream vs. the task itself. That domain encoding is what separates a generic LLM from a useful support tool."),
      gap(80),

      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "Q: What would you improve next?", font: "Arial", bold: true, size: 26, color: BLUE })] }),
      body("Two things: first, structured incident response — when a PagerDuty alert fires, automatically draft a blast radius assessment and stakeholder communication. Second, improve memory persistence so the bot builds up a knowledge base from resolved incidents over time, making each future triage faster."),

      gap(200),
      hr(),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { before: 120, after: 0 },
        children: [new TextRun({ text: "Prepared with Claude • Conviva SSD Team", size: 18, color: "AAAAAA", font: "Arial", italics: true })]
      }),
    ]
  }]
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync("/sessions/clever-zealous-einstein/mnt/outputs/SSD_Bot_Interview_Script.docx", buf);
  console.log("Done.");
});
