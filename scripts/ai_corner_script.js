const { Document, Packer, Paragraph, TextRun, HeadingLevel, AlignmentType,
        BorderStyle, ShadingType, WidthType, Table, TableRow, TableCell } = require('docx');
const fs = require('fs');

const BLUE  = "1E3A5F";
const TEAL  = "0D9488";
const GOLD  = "F59E0B";
const RED   = "DC2626";
const GRAY  = "64748B";
const LGRAY = "F8FAFC";
const LTEAL = "CCFBF1";
const W     = 9360; // content width DXA

const border = (c = "E2E8F0") => ({ style: BorderStyle.SINGLE, size: 1, color: c });
const borders = (c = "E2E8F0") => ({ top: border(c), bottom: border(c), left: border(c), right: border(c) });

function gap(n = 160) {
  return new Paragraph({ spacing: { before: 0, after: n }, children: [new TextRun(" ")] });
}

function hr(color = TEAL) {
  return new Paragraph({
    border: { bottom: { style: BorderStyle.SINGLE, size: 6, color, space: 1 } },
    spacing: { before: 0, after: 200 },
    children: [new TextRun("")]
  });
}

function h1(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_1,
    spacing: { before: 360, after: 80 },
    children: [new TextRun({ text, font: "Arial", size: 34, bold: true, color: BLUE })]
  });
}

function h2(text, color = BLUE) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_2,
    spacing: { before: 240, after: 80 },
    children: [new TextRun({ text, font: "Arial", size: 26, bold: true, color })]
  });
}

function body(text, opts = {}) {
  return new Paragraph({
    spacing: { before: 0, after: 140 },
    children: [new TextRun({ text, size: 22, font: "Arial", color: "1E293B", ...opts })]
  });
}

function note(text) {
  return new Paragraph({
    spacing: { before: 60, after: 60 },
    shading: { fill: "FEF3C7", type: ShadingType.CLEAR },
    children: [
      new TextRun({ text: "⚡ ", size: 20, font: "Arial" }),
      new TextRun({ text, size: 20, font: "Arial", italics: true, color: "92400E" })
    ]
  });
}

function speakBox(text) {
  return new Paragraph({
    spacing: { before: 80, after: 80 },
    indent: { left: 360 },
    border: { left: { style: BorderStyle.SINGLE, size: 16, color: TEAL, space: 16 } },
    children: [new TextRun({ text, size: 22, font: "Arial", italics: true, color: "1E3A5F" })]
  });
}

function actionBox(label, command) {
  return new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [1800, 7560],
    rows: [new TableRow({
      children: [
        new TableCell({
          width: { size: 1800, type: WidthType.DXA },
          shading: { fill: TEAL, type: ShadingType.CLEAR },
          margins: { top: 80, bottom: 80, left: 120, right: 120 },
          borders: borders(TEAL),
          children: [new Paragraph({
            alignment: AlignmentType.CENTER,
            children: [new TextRun({ text: label, bold: true, size: 18, color: "FFFFFF", font: "Arial" })]
          })]
        }),
        new TableCell({
          width: { size: 7560, type: WidthType.DXA },
          shading: { fill: "0F172A", type: ShadingType.CLEAR },
          margins: { top: 80, bottom: 80, left: 180, right: 120 },
          borders: borders("334155"),
          children: [new Paragraph({
            children: [new TextRun({ text: command, size: 21, font: "Courier New", color: "14B8A6" })]
          })]
        })
      ]
    })]
  });
}

function timingRow(slide, time, label) {
  return new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [1200, 1400, 6760],
    rows: [new TableRow({
      children: [
        new TableCell({
          width: { size: 1200, type: WidthType.DXA },
          shading: { fill: BLUE, type: ShadingType.CLEAR },
          margins: { top: 60, bottom: 60, left: 100, right: 100 },
          borders: borders(BLUE),
          children: [new Paragraph({
            alignment: AlignmentType.CENTER,
            children: [new TextRun({ text: slide, bold: true, size: 18, color: "FFFFFF", font: "Arial" })]
          })]
        }),
        new TableCell({
          width: { size: 1400, type: WidthType.DXA },
          shading: { fill: LTEAL, type: ShadingType.CLEAR },
          margins: { top: 60, bottom: 60, left: 100, right: 100 },
          borders: borders(TEAL),
          children: [new Paragraph({
            alignment: AlignmentType.CENTER,
            children: [new TextRun({ text: time, size: 18, color: TEAL, font: "Arial", bold: true })]
          })]
        }),
        new TableCell({
          width: { size: 6760, type: WidthType.DXA },
          shading: { fill: LGRAY, type: ShadingType.CLEAR },
          margins: { top: 60, bottom: 60, left: 160, right: 100 },
          borders: borders("E2E8F0"),
          children: [new Paragraph({
            children: [new TextRun({ text: label, size: 20, color: "1E293B", font: "Arial" })]
          })]
        }),
      ]
    })]
  });
}

const doc = new Document({
  styles: {
    default: { document: { run: { font: "Arial", size: 22 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 34, bold: true, color: BLUE, font: "Arial" },
        paragraph: { spacing: { before: 360, after: 80 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 26, bold: true, color: BLUE, font: "Arial" },
        paragraph: { spacing: { before: 240, after: 80 }, outlineLevel: 1 } },
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

      // ── TITLE ──
      new Paragraph({
        spacing: { before: 0, after: 80 },
        children: [new TextRun({ text: "AI Corner Demo Script", bold: true, size: 42, color: BLUE, font: "Arial" })]
      }),
      new Paragraph({
        spacing: { before: 0, after: 60 },
        children: [new TextRun({ text: "SSD Bot: AI Agent for Pipeline Support", size: 26, color: TEAL, font: "Arial", italics: true })]
      }),
      new Paragraph({
        spacing: { before: 0, after: 40 },
        children: [new TextRun({ text: "Audience: Engineers  ·  Slot: 15–20 minutes  ·  Format: Slides + Live Slack demo", size: 20, color: GRAY, font: "Arial" })]
      }),
      hr(),

      // ── SETUP CHECKLIST ──
      h1("Before You Start — Setup Checklist"),
      note("Do this 10 minutes before the session, not right before you present."),
      gap(80),

      ...["Open the Slack channel you'll demo in (e.g. #ssd-support or a dedicated demo channel).",
          "Make sure the bot (ssd-bot) is running on rccd602-23a: systemctl status ssd-bot.",
          "Have the Airflow URL bookmarked: http://airflow-prod.mds.conviva.com:8080/",
          "Open the deck on one screen; keep Slack visible on another (or use one screen and alt-tab).",
          "Know which pipeline you'll demo — Echostar DPI Event is the best example (it has a clear error).",
          "Have a backup plan: screenshot of the bot's output in case of network issues."
      ].map(t => new Paragraph({
        spacing: { before: 0, after: 100 },
        indent: { left: 360, hanging: 240 },
        children: [
          new TextRun({ text: "☐  ", bold: true, size: 22, font: "Arial", color: TEAL }),
          new TextRun({ text: t, size: 22, font: "Arial" })
        ]
      })),

      gap(80),
      hr(),

      // ── RUN-OF-SHOW ──
      h1("Run of Show — Timing Overview"),
      gap(60),
      timingRow("Slide 1",    "0:00–0:45",  "Title — introduce yourself & the project"),
      gap(40),
      timingRow("Slide 2",    "0:45–2:00",  "The Problem — paint the pain before showing the solution"),
      gap(40),
      timingRow("Slide 3",    "2:00–3:30",  "AI Primer — agent vs. RAG vs. workflow (engineers love this)"),
      gap(40),
      timingRow("Slide 4",    "3:30–5:00",  "How the Agent Works — the reasoning loop"),
      gap(40),
      timingRow("Slide 5",    "5:00–6:00",  "The Toolbox — 6 tools overview"),
      gap(40),
      timingRow("Slide 6",    "6:00–6:30",  "Live Demo slide — transition to Slack"),
      gap(40),
      timingRow("LIVE DEMO",  "6:30–10:30", "Live Slack demo — bot investigates Echostar pipeline"),
      gap(40),
      timingRow("Slide 7",    "10:30–12:00","What the Bot Found — debrief the demo output"),
      gap(40),
      timingRow("Slide 8",    "12:00–13:00","Under the Hood — where the intelligence lives"),
      gap(40),
      timingRow("Slide 9",    "13:00–14:00","Impact numbers"),
      gap(40),
      timingRow("Slide 10",   "14:00–15:00","What's Next — roadmap"),
      gap(40),
      timingRow("Slide 11",   "15:00–20:00","Discussion & Q&A"),
      gap(120),
      hr(),

      // ── SLIDE-BY-SLIDE ──
      h1("Slide-by-Slide Script"),

      // ── S1 ──
      h2("Slide 1 — Title  [0:00–0:45]"),
      speakBox(
        "Hey everyone, thanks for coming. I'm going to show you something I built for the SSD team over the last few months. It's a Slack bot — but not a FAQ bot. It's an actual AI agent that can investigate pipeline failures automatically. By the end of this session you'll have seen it do a live diagnosis in real time, and I'll explain exactly how it works under the hood."
      ),
      note("Don't rush this. Let people settle. Make eye contact."),
      gap(100),

      // ── S2 ──
      h2("Slide 2 — The Problem  [0:45–2:00]"),
      speakBox(
        "Before I show the solution, let's talk about the problem. Every time an SSD pipeline fails, the on-call engineer has to open at least four different tools: Airflow for the run status and logs, Jira to look for similar past tickets, Confluence for the runbook, and Slack to see if anyone's discussed this before. Then they have to mentally connect all of that."
      ),
      speakBox(
        "And the harder issue: a lot of that knowledge is in people's heads. If you've been here three years you know what a 'sensor timeout on Echostar' usually means. A new engineer doesn't. That's a fragility — and it's exactly what this bot addresses."
      ),
      note("Pause after 'that's a fragility.' Let it land before moving on."),
      gap(100),

      // ── S3 ──
      h2("Slide 3 — AI Pattern Primer  [2:00–3:30]"),
      speakBox(
        "Since this is an AI corner, I want to be precise about what we built. There are three common AI patterns. RAG is a lookup pattern — you search a knowledge base and the LLM answers using those documents. A workflow is a hardcoded sequence — step one always runs, then step two. An agent is different: the LLM decides what to do next based on what it finds. It reasons, picks a tool, observes the result, and adapts."
      ),
      speakBox(
        "The SSD bot is an agent. And I chose that pattern deliberately — pipeline failures are non-deterministic. A stuck sensor gets investigated differently than a Spark job crash. A fixed workflow would fail on edge cases constantly. The agent handles novelty."
      ),
      note("If someone asks 'what model does it use?' — Claude by Anthropic, via API. Same family as Claude.ai."),
      gap(100),

      // ── S4 ──
      h2("Slide 4 — How the Agent Works  [3:30–5:00]"),
      speakBox(
        "Here's the reasoning loop. The user asks a question in Slack. The bot thinks: what do I know, what's missing? It picks a tool — maybe 'get recent failed runs for this DAG.' It calls the Airflow API, reads the response, and thinks again. Maybe it now needs the task log. It fetches that. Maybe it finds a Spark exception and cross-references Jira. Each iteration builds on the last — up to 10 rounds. Then it answers."
      ),
      speakBox(
        "The important thing: the code doesn't dictate this sequence. The model does. The code just provides the tools and enforces the iteration limit."
      ),
      gap(100),

      // ── S5 ──
      h2("Slide 5 — The Toolbox  [5:00–6:00]"),
      speakBox(
        "Six tools available to the agent. DAG run history, task logs, DAG source code, Jira search across three projects, Confluence runbooks, and Slack history. On any query it might use two or five of these depending on what it finds. Task logs are the most valuable — you get the actual exception, not just a red badge."
      ),
      gap(100),

      // ── S6 ──
      h2("Slide 6 — Live Demo Slide  [6:00–6:30]"),
      speakBox("Okay, let's stop talking about it and actually see it work."),
      note("Switch to Slack now. Don't narrate the switch — just do it."),
      gap(100),

      // ── LIVE DEMO ──
      h2("LIVE DEMO — Slack  [6:30–10:30]", RED),
      note("This is the centerpiece. Take your time. Point out what's happening at each step."),
      gap(80),

      body("Step 1 — Type the query:", { bold: true }),
      gap(40),
      actionBox("TYPE IN SLACK", "why is the Echostar DPI event pipeline failing?"),
      gap(80),

      body("Step 2 — While the bot thinks, narrate what it's doing:", { bold: true }),
      speakBox("You can see the bot is working — it's calling the Airflow API to find the most recent failed run. Now it's getting the task instances to find which specific task failed. Now it's fetching the actual task log from the worker node."),
      note("The bot may show 'thinking...' messages as it iterates. Point these out — they show the reasoning loop in action."),
      gap(80),

      body("Step 3 — When the response arrives, highlight these:", { bold: true }),
      ...[
        "Which tool calls it made (shown in the response or thinking trace)",
        "The specific exception it found: 'CSV data source does not support map<string,string> data type'",
        "That it identified this was attempt 4 of 4 — it's been failing consistently",
        "The plain-language explanation: map column in the schema, CSV can't serialize it",
        "The suggested fix: stringify the column with to_json() before writing",
      ].map(t => new Paragraph({
        spacing: { before: 0, after: 100 },
        indent: { left: 400, hanging: 240 },
        children: [
          new TextRun({ text: "→  ", bold: true, size: 22, font: "Arial", color: TEAL }),
          new TextRun({ text: t, size: 22, font: "Arial" })
        ]
      })),

      gap(80),
      body("Step 4 — Optional follow-up query if time allows:", { bold: true }),
      gap(40),
      actionBox("TYPE IN SLACK", "are there any similar past incidents in Jira?"),
      gap(80),
      note("If the bot is slow or network drops: switch back to slides and show the screenshot on Slide 7. The output is identical — it's a real example from June 23rd."),
      gap(100),

      // ── S7 ──
      h2("Slide 7 — What the Bot Found  [10:30–12:00]"),
      speakBox(
        "Let me debrief what just happened. The bot made three tool calls: it fetched recent failed runs to get the run ID, listed task instances to find the failing task and its retry count, then fetched the actual task log — 200+ lines of output. In that log, buried in Spark's stack trace, was one line: 'AnalysisException: CSV data source does not support map<string,string> data type.'"
      ),
      speakBox(
        "That's the root cause. The DPI event data has evolved — a new column with map type appeared in the schema. Spark's CSV writer can't handle map types. It crashes on every retry. The fix is a code change in the Spark job, not anything we can fix from the Airflow side."
      ),
      speakBox(
        "What used to take 30 minutes of manual log reading took the bot about 45 seconds."
      ),
      gap(100),

      // ── S8 ──
      h2("Slide 8 — Under the Hood  [12:00–13:00]"),
      speakBox(
        "Three things make this work. First, domain knowledge in the system prompt — the bot knows Conviva's Airflow instances, how we name DAGs, what a sensor timeout means versus a Spark crash, which paths to check for which customers. A generic LLM knows what Airflow is. This one knows how we use it."
      ),
      speakBox(
        "Second, 100 real escalation tickets I analyzed. I identified the top recurring failure patterns across a year of SSD support, and baked that institutional knowledge into the prompt. Every query benefits from it."
      ),
      speakBox(
        "Third, the tool selection logic. The model knows when to check upstream versus reading the task log, when to cross-reference Jira, when to pull the DAG source to understand sensor dependencies."
      ),
      gap(100),

      // ── S9 ──
      h2("Slide 9 — Impact  [13:00–14:00]"),
      speakBox(
        "The numbers: 30 to 60 minutes of manual investigation, down to under a minute. But the bigger win is knowledge democratization. Tribal knowledge is a fragility — when the person who knows something leaves, the team regresses. This bot externalizes that knowledge. A new engineer and a ten-year veteran now have the same starting point when a PagerDuty alert fires at 2am."
      ),
      gap(100),

      // ── S10 ──
      h2("Slide 10 — What's Next  [14:00–15:00]"),
      speakBox(
        "Three things on the roadmap. Structured incident response: when PD fires, the bot automatically prepares a blast radius assessment and a draft stakeholder communication — not just answering questions but proactively doing the pre-work. Persistent incident memory: after a resolved incident, the bot learns the fix so future similar failures get richer answers. And thread state persistence — right now bot state resets on restart, which breaks the 'yes/no' confirmation flow. That's an engineering fix we have planned."
      ),
      gap(100),

      // ── S11 ──
      h2("Slide 11 — Discussion  [15:00–20:00]"),
      speakBox("Okay, I'll stop talking. What questions do you have?"),
      note("If the room is quiet: 'Who here has spent more than an hour debugging a pipeline failure in the last month?' — then point to the bot."),
      gap(80),

      body("Common questions and suggested answers:", { bold: true }),
      gap(60),

      h2("Q: How accurate is it?", GRAY),
      body("It reads actual logs and actual Jira tickets — it's not hallucinating. The analysis interpretation can be wrong on unusual failures, which is why it's a first-responder aid, not an autonomous fixer. Engineers make the call; the bot accelerates the triage."),
      gap(60),

      h2("Q: What happens when the Airflow API is down?", GRAY),
      body("It gracefully fails with an error message and falls back to whatever information it can get. Jira search and Confluence lookup still work without Airflow connectivity."),
      gap(60),

      h2("Q: Could we use this for our team's pipelines?", GRAY),
      body("Yes — the bot is extensible. Adding a new Airflow instance is one environment variable. The system prompt can be updated to include team-specific knowledge. Happy to pair with anyone who wants to try it."),
      gap(60),

      h2("Q: How do you prevent it from taking actions — like clearing a run?", GRAY),
      body("The bot is read-only by design. It fetches and reads; it never writes, clears, or triggers anything. The agentic loop has no write tools. Actions require a human."),
      gap(160),

      hr(),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { before: 120, after: 0 },
        children: [new TextRun({ text: "Prepared with Claude  ·  Conviva SSD Team", size: 18, color: "AAAAAA", font: "Arial", italics: true })]
      }),
    ]
  }]
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync("/sessions/clever-zealous-einstein/mnt/outputs/SSD_Bot_Demo_Script.docx", buf);
  console.log("Done.");
});
