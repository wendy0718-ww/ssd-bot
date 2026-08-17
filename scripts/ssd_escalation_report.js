const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  AlignmentType, BorderStyle, WidthType, ShadingType, VerticalAlign,
  Header, Footer, PageNumber, PageBreak, HeadingLevel, LevelFormat,
  ExternalHyperlink
} = require('docx');
const fs = require('fs');

// ── Layout: US Letter, 1" margins ─────────────────────────────────────────────
const PW = 9360; // 12240 - 2*1440

// ── Colors ────────────────────────────────────────────────────────────────────
const NAVY    = "1A3A5C";
const TEAL    = "0B6E5A";
const RED     = "922B21";
const AMBER   = "935116";
const LGRAY   = "F4F6F7";
const MGRAY   = "D5D8DC";
const WHITE   = "FFFFFF";
const BODY    = "1C2833";
const GREEN   = "1A5C3A";
const PURPLE  = "4A235A";
const ORANGE  = "BA4A00";
const STEEL   = "2E6DA4";

// ── Border helpers ─────────────────────────────────────────────────────────────
const nb = { style: BorderStyle.NONE, size: 0, color: "FFFFFF" };
const noB = { top: nb, bottom: nb, left: nb, right: nb };
const tb = { style: BorderStyle.SINGLE, size: 4, color: MGRAY };
const allB = { top: tb, bottom: tb, left: tb, right: tb };
const headerBorder = { style: BorderStyle.SINGLE, size: 8, color: NAVY };

function run(text, { bold=false, italic=false, size=22, color=BODY, font="Arial" }={}) {
  return new TextRun({ text, bold, italic, size, color, font });
}
function para(children, { before=60, after=60, align, indent }={}) {
  if (typeof children === 'string') children = run(children);
  const opts = { spacing: { before, after }, children: Array.isArray(children) ? children : [children] };
  if (align) opts.alignment = align;
  if (indent) opts.indent = indent;
  return new Paragraph(opts);
}
function h1(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_1,
    spacing: { before: 300, after: 120 },
    border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: NAVY, space: 1 } },
    children: [new TextRun({ text, bold: true, size: 28, color: NAVY, font: "Arial" })]
  });
}
function h2(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_2,
    spacing: { before: 240, after: 80 },
    children: [new TextRun({ text, bold: true, size: 24, color: TEAL, font: "Arial" })]
  });
}
function h3(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_3,
    spacing: { before: 180, after: 60 },
    children: [new TextRun({ text, bold: true, size: 22, color: NAVY, font: "Arial" })]
  });
}
function bullet(text, { bold=false, color=BODY }={}) {
  return new Paragraph({
    numbering: { reference: "bullets", level: 0 },
    spacing: { before: 30, after: 30 },
    children: [new TextRun({ text, bold, size: 22, color, font: "Arial" })]
  });
}
function subBullet(text) {
  return new Paragraph({
    numbering: { reference: "sub-bullets", level: 0 },
    spacing: { before: 20, after: 20 },
    children: [new TextRun({ text, size: 20, color: BODY, font: "Arial" })]
  });
}
function gap(n=120) { return para(run(" "), { before: n, after: 0 }); }

// ── Stat box (3-column) ────────────────────────────────────────────────────────
function statBox(stats) {
  // stats = [{label, value, sub, color}]
  const w = Math.floor(PW / stats.length);
  return new Table({
    width: { size: PW, type: WidthType.DXA },
    columnWidths: stats.map(() => w),
    rows: [new TableRow({
      children: stats.map(s => new TableCell({
        width: { size: w, type: WidthType.DXA },
        borders: noB,
        shading: { fill: s.color || LGRAY, type: ShadingType.CLEAR },
        margins: { top: 120, bottom: 120, left: 160, right: 160 },
        verticalAlign: VerticalAlign.CENTER,
        children: [
          para(run(s.value, { bold: true, size: 52, color: s.valColor || NAVY, font: "Arial" }), { before: 0, after: 20, align: AlignmentType.CENTER }),
          para(run(s.label, { bold: true, size: 18, color: WHITE, font: "Arial" }), { before: 0, after: 10, align: AlignmentType.CENTER }),
          s.sub ? para(run(s.sub, { size: 17, color: "D5D8DC", font: "Arial" }), { before: 0, after: 0, align: AlignmentType.CENTER }) : para(run(" "), { before: 0, after: 0 }),
        ]
      }))
    })]
  });
}

// ── Two-column data table (header + rows) ─────────────────────────────────────
function dataTable(headers, rows, colWidths) {
  const border = { style: BorderStyle.SINGLE, size: 4, color: MGRAY };
  const borders = { top: border, bottom: border, left: border, right: border };

  function makeRow(cells, isHeader) {
    return new TableRow({
      tableHeader: isHeader,
      children: cells.map((c, i) => new TableCell({
        width: { size: colWidths[i], type: WidthType.DXA },
        borders,
        shading: { fill: isHeader ? NAVY : (cells._shade || WHITE), type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 120, right: 120 },
        children: [para(run(c, { bold: isHeader, size: isHeader ? 19 : 20, color: isHeader ? WHITE : BODY, font: "Arial" }), { before: 0, after: 0 })]
      }))
    });
  }

  return new Table({
    width: { size: PW, type: WidthType.DXA },
    columnWidths: colWidths,
    rows: [
      makeRow(headers, true),
      ...rows.map(r => makeRow(r, false))
    ]
  });
}

// ── Open ticket row (highlight) ────────────────────────────────────────────────
function openTicketTable(tickets) {
  const cols = [1100, 4200, 1300, 900, 1860];
  const headers = ["Ticket", "Summary", "Status", "Priority", "Assignee"];
  const border = { style: BorderStyle.SINGLE, size: 4, color: MGRAY };
  const borders = { top: border, bottom: border, left: border, right: border };

  const statusColor = { "Open": "F9EBEA", "In Progress": "FDFEFE", "Info Request": "EBF5FB" };
  const priColor    = { "P1": RED, "P2": AMBER };

  function makeHdr() {
    return new TableRow({
      tableHeader: true,
      children: headers.map((h, i) => new TableCell({
        width: { size: cols[i], type: WidthType.DXA },
        borders,
        shading: { fill: NAVY, type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 120, right: 120 },
        children: [para(run(h, { bold: true, size: 19, color: WHITE, font: "Arial" }), { before: 0, after: 0 })]
      }))
    });
  }

  function makeRow(t) {
    const rowData = [t.key, t.summary, t.status, t.priority, t.assignee];
    const shade = statusColor[t.status] || WHITE;
    return new TableRow({
      children: rowData.map((v, i) => new TableCell({
        width: { size: cols[i], type: WidthType.DXA },
        borders,
        shading: { fill: i === 3 ? "FFFFFF" : shade, type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 120, right: 120 },
        children: [para(run(v, { size: 19, bold: i === 3, color: i === 3 ? (priColor[v] || BODY) : BODY, font: "Arial" }), { before: 0, after: 0 })]
      }))
    });
  }

  return new Table({
    width: { size: PW, type: WidthType.DXA },
    columnWidths: cols,
    rows: [makeHdr(), ...tickets.map(makeRow)]
  });
}

// ── Section banner ─────────────────────────────────────────────────────────────
function banner(text, color) {
  return new Table({
    width: { size: PW, type: WidthType.DXA },
    rows: [new TableRow({ children: [new TableCell({
      width: { size: PW, type: WidthType.DXA },
      borders: noB,
      shading: { fill: color, type: ShadingType.CLEAR },
      margins: { top: 80, bottom: 80, left: 160, right: 160 },
      children: [para(run(text, { bold: true, size: 22, color: WHITE, font: "Arial" }), { before: 0, after: 0 })]
    })]})],
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// DOCUMENT
// ═══════════════════════════════════════════════════════════════════════════════
const doc = new Document({
  numbering: {
    config: [
      { reference: "bullets", levels: [{
        level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 720, hanging: 360 } } }
      }]},
      { reference: "sub-bullets", levels: [{
        level: 0, format: LevelFormat.BULLET, text: "◦", alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 1080, hanging: 360 } } }
      }]},
      { reference: "numbers", levels: [{
        level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 720, hanging: 360 } } }
      }]},
    ]
  },
  styles: {
    default: { document: { run: { font: "Arial", size: 22, color: BODY } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 28, bold: true, font: "Arial", color: NAVY },
        paragraph: { spacing: { before: 300, after: 120 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 24, bold: true, font: "Arial", color: TEAL },
        paragraph: { spacing: { before: 240, after: 80 }, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 22, bold: true, font: "Arial", color: NAVY },
        paragraph: { spacing: { before: 180, after: 60 }, outlineLevel: 2 } },
    ]
  },
  sections: [{
    properties: {
      page: {
        size: { width: 12240, height: 15840 },
        margin: { top: 1080, right: 1440, bottom: 1080, left: 1440 }
      }
    },
    headers: {
      default: new Header({ children: [
        new Paragraph({
          spacing: { before: 0, after: 100 },
          border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: NAVY, space: 1 } },
          children: [
            new TextRun({ text: "SSD Customer Escalation Report  —  Confidential", bold: true, size: 19, color: NAVY, font: "Arial" }),
            new TextRun({ text: "    |    CE Project, Jira  |    Analysis Date: June 23, 2026", size: 18, color: "7F8C8D", font: "Arial" }),
          ]
        })
      ]})
    },
    footers: {
      default: new Footer({ children: [
        new Paragraph({
          spacing: { before: 80, after: 0 },
          border: { top: { style: BorderStyle.SINGLE, size: 4, color: MGRAY, space: 1 } },
          children: [
            new TextRun({ text: "Conviva  |  Customer Success Engineering  |  Internal Use Only     ", size: 18, color: "7F8C8D", font: "Arial" }),
            new TextRun({ text: "Page ", size: 18, color: "7F8C8D", font: "Arial" }),
            new TextRun({ children: [PageNumber.CURRENT], size: 18, color: "7F8C8D", font: "Arial" }),
          ]
        })
      ]})
    },

    children: [

      // ── COVER BLOCK ──────────────────────────────────────────────────────────
      new Table({ width: { size: PW, type: WidthType.DXA }, rows: [new TableRow({ children: [
        new TableCell({
          width: { size: PW, type: WidthType.DXA },
          borders: noB,
          shading: { fill: NAVY, type: ShadingType.CLEAR },
          margins: { top: 200, bottom: 200, left: 300, right: 300 },
          children: [
            para([
              run("SSD Customer Escalation Report", { bold: true, size: 44, color: WHITE, font: "Arial" }),
            ], { before: 0, after: 80, align: AlignmentType.LEFT }),
            para([
              run("Scheduled Data Delivery (SSD)  |  Customer Escalations (CE) Project", { size: 22, color: "AED6F1", font: "Arial" }),
            ], { before: 0, after: 60, align: AlignmentType.LEFT }),
            para([
              run("Period: June 2025 – June 2026  |  Prepared by: Wendy (CSE Team)  |  Date: June 23, 2026", { size: 20, color: "D5D8DC", font: "Arial" }),
            ], { before: 0, after: 0, align: AlignmentType.LEFT }),
          ]
        })
      ]})]})  ,

      gap(200),

      // ── EXECUTIVE SUMMARY ────────────────────────────────────────────────────
      h1("Executive Summary"),

      para([
        run("This report analyzes "),
        run("100 customer escalation tickets", { bold: true }),
        run(" filed against Conviva's Scheduled Data Delivery (SSD) platform between June 2025 and June 2026. The SSD platform delivers scheduled data feeds (hourly, daily, and minutely) via S3, GCS, SFTP, and Azure to tier-1 streaming customers including Disney, DAZN, SlingTV, Sky, Qurate, and MLB."),
      ], { before: 0, after: 80 }),

      para([
        run("The portfolio spans "),
        run("81 P2 and 19 P1 escalations", { bold: true }),
        run(". Of these, "),
        run("9 tickets remain unresolved", { bold: true, color: RED }),
        run(" as of the report date, including two chronic infrastructure-level issues that require engineering action: Kubernetes pod startup hangs blocking all SSD deliveries (CE-12054, open since May 29) and missing Global Custom Tags in DPI feeds (CE-11960, open since April 20)."),
      ], { before: 0, after: 80 }),

      para([
        run("The most frequently affected customers are "),
        run("BBCK-PerformGroup/DAZN (11 tickets), MLB/ESPN/Disney (11), SlingTV/Echostar (9), and Sky (8)", { bold: true }),
        run(". Together these four account groups represent approximately 39% of all escalations. Key recurring root causes include pipeline falsely reporting success without delivering files, DST/timezone boundary bugs, and data discrepancy between SSD and Pulse/Trends."),
      ], { before: 0, after: 60 }),

      gap(80),

      // ── KPI STAT BOXES ────────────────────────────────────────────────────────
      statBox([
        { label: "Total Tickets", value: "100", sub: "Jun 2025 – Jun 2026", color: NAVY, valColor: "AED6F1" },
        { label: "P1 Escalations", value: "19", sub: "High severity", color: RED, valColor: "FADBD8" },
        { label: "Still Open / In Progress", value: "9", sub: "Require action", color: ORANGE, valColor: "FAD7A0" },
        { label: "Resolved / Closed", value: "91", sub: "91% resolution rate", color: TEAL, valColor: "A9DFBF" },
      ]),

      gap(200),

      // ── TICKET VOLUME & TRENDS ───────────────────────────────────────────────
      h1("Ticket Volume & Trends"),

      h2("Monthly Distribution"),

      para([
        run("Ticket volume has been broadly consistent across the 12-month period with a slight uptick in mid-2025 and again in early June 2026. Notable spikes align with major incidents:"),
      ], { before: 0, after: 60 }),

      bullet("October 2025: DST changeover (CE-11650) plus SlingTV DPI field-name corruption (CE-11657) drove 3 P1s in a single week."),
      bullet("November–December 2025: Legacy-to-Connect migration activity dominated with 10+ migration tickets as the Box SSD deprecation deadline approached."),
      bullet("March 2026: The ESPN hourly SSD delay (CE-11901) and SlingTV raw Event Feed pipeline setup (CE-11915) both opened concurrently."),
      bullet("May–June 2026: A May 22 upstream DPI outage (CE-12045) cascaded across all 7 DPI Flow Feed pipelines. K8S pod startup hangs emerged as a chronic blocker (CE-12054)."),

      gap(80),

      h2("Priority Breakdown"),

      dataTable(
        ["Priority", "Count", "% of Total", "Notes"],
        [
          ["P1", "19", "19%", "Immediate customer impact; SLA breach risk"],
          ["P2", "81", "81%", "Significant but not immediately service-impacting"],
        ],
        [1200, 900, 1400, 5860]
      ),

      gap(80),

      h2("Status Breakdown"),

      dataTable(
        ["Status", "Count", "Notes"],
        [
          ["Resolved", "80", "Formally closed with resolution noted in Jira"],
          ["Closed", "11", "Closed without explicit resolution entry"],
          ["Open", "4", "Active, no assignee action taken yet"],
          ["In Progress", "4", "Assignee working; no resolution yet"],
          ["Info Request", "1", "Awaiting customer response"],
          ["Total Unresolved", "9", "9% of all tickets remain open"],
        ],
        [1800, 900, 6660]
      ),

      gap(200),

      // ── ISSUE CATEGORY BREAKDOWN ─────────────────────────────────────────────
      h1("Issue Category Breakdown"),

      para("Tickets were grouped into the following functional categories based on root cause and work type:", { before: 0, after: 80 }),

      dataTable(
        ["Category", "Count", "% Share", "Representative Tickets"],
        [
          ["Device / Geo Metadata Issues", "12", "12%", "CE-11992, CE-11942, CE-11905, CE-11364"],
          ["SSD Delivery Failures (missing/delayed files)", "11", "11%", "CE-12054, CE-12105, CE-12022, CE-11463"],
          ["Legacy → Connect Migration", "10", "10%", "CE-11725, CE-11689, CE-11565, CE-11814"],
          ["SSD Schema / Field Issues", "9", "9%", "CE-12117, CE-11657, CE-11976, CE-11802"],
          ["Pipeline Setup / Configuration", "6", "6%", "CE-12064, CE-11915, CE-11658, CE-11421"],
          ["DPI / Event Feed Issues", "6", "6%", "CE-11960, CE-11838, CE-11950, CE-11657"],
          ["UI / Platform Issues", "6", "6%", "CE-12091, CE-12098, CE-11914, CE-11506"],
          ["SSD Data Backfill / Resend", "6", "6%", "CE-12045, CE-12004, CE-11873, CE-11807"],
          ["Data Discrepancy (SSD vs Pulse/Trends)", "3", "3%", "CE-11783, CE-11702, CE-11343"],
          ["MDDI Feed", "2", "2%", "CE-12014, CE-11523"],
          ["Other (analysis, misc, ad-hoc)", "29", "29%", "Various"],
        ],
        [3000, 800, 1000, 4560]
      ),

      gap(80),

      h2("Key Observations by Category"),

      h3("Device & Geo Metadata (12 tickets)"),
      para("This is the highest-volume recurring category and spans the full period. Issues include wrong ASN-to-ISP mappings (ITV, SkyShowtime), cities reporting as Unknown (SkyShowtime, Galicia Spain), MVNO misclassification (Dish/Boost Mobile), and device name misidentification (iPhone models, Disney Android VR). MaxMind is the common data provider; stale or incorrect MaxMind mappings drive the majority of these.", { before: 0, after: 60 }),

      h3("SSD Delivery Failures (11 tickets)"),
      para("Delivery failures break down into three sub-patterns: (1) Kubernetes pod startup hangs blocking Spark jobs — chronic issue affecting all SSD customers since May 2026; (2) Upstream DPI data gaps causing missing files for SlingTV/Echostar; (3) GCS credential expiry or S3 access issues preventing file delivery. Notably, several pipelines reported success in Airflow while delivering zero files (Coupang CE-11903, Telstra CE-11484).", { before: 0, after: 60 }),

      h3("Legacy to Connect Migration (10 tickets)"),
      para("The Box SSD deprecation drove a significant migration workload in Q4 2025 and Q1 2026. Migration side effects included BigQuery schema incompatibility post-migration (Telus CE-11725), inflated session counts when shifting from daily to hourly granularity (Disney CE-11814), and duplicate pipelines running in parallel. The migration affected Telus, SkyShowtime, MLB, Qurate, Univision, DAZN, NBCU-D2C, FOX-NL, and others.", { before: 0, after: 60 }),

      h3("Schema / Field Issues (9 tickets)"),
      para("Field-level schema bugs caused significant downstream impact. The most severe was CE-11657 (SlingTV, Oct 2025) where all DPI dimension field names had '_10523' appended, completely breaking customer dashboards. Other issues include Ad SSD using wrong metric names (VideoPlaybackFailures vs ADPlaybackFailures), missing fields in exported reports, and custom dimension values truncated at the wrong character limit.", { before: 0, after: 60 }),

      gap(200),

      // ── TOP CUSTOMERS ────────────────────────────────────────────────────────
      h1("Top Customers by Escalation Volume"),

      dataTable(
        ["Customer / Account Group", "Ticket Count", "Severity Profile", "Most Common Issue Type"],
        [
          ["BBCK-PerformGroup / DAZN", "11", "Mostly P2", "Schema additions, data discrepancy, delivery delays"],
          ["MLB / ESPN / Disney group", "11", "Mix of P1 & P2", "Duplicate sessions, missing hours, field additions"],
          ["SlingTV / Echostar", "9", "P1 heavy", "DPI Event Feed missing files, raw feed setup, field corruption"],
          ["Sky (ITV, SkyShowtime, Sky UK)", "8", "P2", "Metadata errors, ASN mapping, midnight boundary sessions"],
          ["Qurate (QVC+HSN)", "5", "P2", "HHID pipeline, IPv4/IPv6 type impact, migration"],
          ["SkyShowtime", "5", "P2", "Geo/city unknown, duplicate pipelines, migration"],
          ["NBCU / Peacock", "4", "P1 & P2", "MDDI missing minutes, DPI data issue, legacy disable"],
          ["Airtel", "4", "P2", "Backfill requests, ViewerID reports"],
          ["NFL Digital Media", "4", "P2", "Duplicate FlowIDs in SSD files"],
          ["Thmanyah", "3", "P1 & P2", "Historical data retention, DPI pipeline setup, backfill"],
          ["OSNTV / Gotham", "3", "P2", "DPI flow sample, delivery failures, pipeline alerts"],
          ["Telus", "3", "P2", "GCS migration, BigQuery ingest failure, backfill"],
        ],
        [3000, 1400, 1600, 3360]
      ),

      gap(80),

      para([
        run("SlingTV/Echostar", { bold: true }),
        run(" warrants special attention: despite relatively fewer tickets (9), a disproportionate share are P1 and involve core infrastructure (missing Event SSD files, raw feed architecture, DPI field corruption). This customer has the highest operational risk profile in the SSD portfolio."),
      ], { before: 0, after: 60 }),

      para([
        run("DAZN and Disney", { bold: true }),
        run(" have the highest absolute ticket count but most issues are P2 schema enhancement requests and data discrepancies — lower operational risk, higher engineering time cost due to volume."),
      ], { before: 0, after: 60 }),

      gap(200),

      // ── RECURRING ROOT CAUSES ────────────────────────────────────────────────
      new Paragraph({ pageBreakBefore: true, children: [new TextRun({ text: " " })] }),

      h1("Recurring Root Causes"),

      para("Ten distinct root cause patterns appeared across multiple tickets. These are ranked by business impact:", { before: 0, after: 80 }),

      // Root cause 1
      banner("ROOT CAUSE 1  |  Kubernetes Pod Startup Hangs  (1 open ticket, affects all SSD customers)", NAVY),
      para([
        run("Ticket: ", { bold: true }),
        run("CE-12054 (Open, May 29 2026)"),
      ], { before: 60, after: 20 }),
      para("Kubernetes pods consistently hang during Spark job triggers and copy jobs for SSD deliveries, requiring manual engineering intervention for each occurrence. This has been blocking both minutely and hourly deliveries since late May 2026. No permanent fix is in place. This is the highest-priority infrastructure risk in the current backlog.", { before: 0, after: 60 }),
      para([
        run("Recommendation: ", { bold: true }),
        run("Assign dedicated engineering sprint to diagnose K8S hang root cause. Implement automated pod health checks and restart policies as a short-term mitigation."),
      ], { before: 0, after: 80 }),

      // Root cause 2
      banner("ROOT CAUSE 2  |  Pipeline Reports Success Without Delivering Files", ORANGE),
      para([
        run("Tickets: ", { bold: true }),
        run("CE-11903 (Coupang, Mar 2026), CE-11484 (Telstra, Aug 2025)"),
      ], { before: 60, after: 20 }),
      para("Airflow DAG reports success but no files are delivered because of a 'success flag already exists' shortcut condition. Customers discover the gap only when their downstream ingestion fails hours or days later. This silent failure mode is particularly dangerous because monitoring does not catch it.", { before: 0, after: 60 }),
      para([
        run("Recommendation: ", { bold: true }),
        run("Add a post-delivery validation step to SSD pipelines that verifies at least one output file exists at the target path before marking the run success. Alert on zero-file deliveries."),
      ], { before: 0, after: 80 }),

      // Root cause 3
      banner("ROOT CAUSE 3  |  DST / Timezone Boundary Bugs  (3 separate incidents)", AMBER),
      para([
        run("Tickets: ", { bold: true }),
        run("CE-11650 (CME/RTVE/TAG, Oct 2025), CE-11959 (Ziggo/SKO, Apr 2026), CE-11419 (Sky UK, Jul 2025)"),
      ], { before: 60, after: 20 }),
      para("Three separate incidents all stem from timezone handling bugs: (1) BST-to-UTC DST transition caused a pipeline to re-deliver the previous day's file. (2) Cross-midnight sessions in Amsterdam timezone only appeared in one day's file instead of both. (3) Sessions at the midnight hour boundary were missing entirely from the GCS bucket. Each incident required a manual backfill and customer notification.", { before: 0, after: 60 }),
      para([
        run("Recommendation: ", { bold: true }),
        run("Implement regression tests for DST boundary conditions. Add session boundary validation to ensure cross-midnight sessions appear in both the first day's file (as NotEnded) and the second day's file."),
      ], { before: 0, after: 80 }),

      // Root cause 4
      banner("ROOT CAUSE 4  |  Upstream DPI Data Gaps Cascading to 7 Pipelines", RED),
      para([
        run("Tickets: ", { bold: true }),
        run("CE-12045 (May 2026 incident), CE-12105, CE-12022 (SlingTV, Jun/May 2026)"),
      ], { before: 60, after: 20 }),
      para("The May 22 DPI upstream outage (05:06–11:21 UTC) caused missing data across all 7 active DPI Flow Feed pipelines simultaneously: Thmanyah, SlingTV (sling_single_event_flow), OSG, OSNTV, Gotham, and Echostar-DANY. SlingTV also had separate missing-hour incidents in May and June 2026. Backfill took over 2 weeks to complete (CE-12045 resolved June 8).", { before: 0, after: 60 }),
      para([
        run("Recommendation: ", { bold: true }),
        run("Implement an automated DPI upstream health check that fires a PagerDuty alert within 5 minutes of a gap. Document a fast-path backfill playbook for the 7 DPI pipelines so recovery time is under 4 hours."),
      ], { before: 0, after: 80 }),

      // Root cause 5
      banner("ROOT CAUSE 5  |  Data Discrepancy Between SSD and Pulse/Trends", TEAL),
      para([
        run("Tickets: ", { bold: true }),
        run("CE-11783 (CW, Jan 2026), CE-11702 (DAZN, Dec 2025), CE-11343 (FIFA-Plus, Jun 2025), CE-11674 (Disney, Nov 2025)"),
      ], { before: 60, after: 20 }),
      para("Multiple customers report that SSD numbers do not match Trends UI or Viewer Module. Root causes vary per ticket: session deduplication differences, is_live field discrepancy, and VSF-T calculation timing. Each incident requires manual investigation per customer, consuming significant CSE engineering time. No systematic cross-check exists.", { before: 0, after: 60 }),
      para([
        run("Recommendation: ", { bold: true }),
        run("Build a reconciliation script that runs daily to compare SSD row counts and key metric totals against Pulse/Trends for top-tier customers. Flag deviations above a configurable threshold before customers discover them."),
      ], { before: 0, after: 80 }),

      // Root cause 6
      banner("ROOT CAUSE 6  |  Legacy-to-Connect Migration Side Effects", PURPLE),
      para([
        run("Tickets: ", { bold: true }),
        run("CE-11725 (Telus BigQuery failure), CE-11814 (Disney inflated session counts), CE-11565 (duplicate pipelines)"),
      ], { before: 60, after: 20 }),
      para("The Box SSD deprecation migration introduced unexpected side effects: Telus could not ingest post-migration files due to BigQuery schema incompatibility. Disney saw inflated session counts when migrating from daily to hourly granularity (sessions spanning multiple hours were double-counted). Several customers ended up with both Legacy and Connect pipelines running in parallel, creating duplicate deliveries.", { before: 0, after: 60 }),
      para([
        run("Recommendation: ", { bold: true }),
        run("Create a migration pre-flight checklist that includes: (a) schema compatibility test against customer ingestion system, (b) session-count reconciliation for the first 3 days post-cutover, (c) verification that Legacy pipeline is disabled before signing off the migration."),
      ], { before: 0, after: 60 }),

      gap(200),

      // ── OPEN ITEMS ───────────────────────────────────────────────────────────
      h1("Open & In-Progress Tickets"),

      para("The following 9 tickets are unresolved as of June 23, 2026:", { before: 0, after: 80 }),

      openTicketTable([
        { key: "CE-12117", summary: "Custom Dimension 'live video name' Null in SSD (STV)", status: "Info Request", priority: "P2", assignee: "Emma Liu" },
        { key: "CE-12105", summary: "c3.Echostar-SlingTV: Event SSD files missing 2026/06/14/18", status: "Open", priority: "P1", assignee: "Bharadwaj N" },
        { key: "CE-12100", summary: "VSI: Paramount-Plus — UI slowness / Error Loading data", status: "In Progress", priority: "P1", assignee: "Prashanth Vasa" },
        { key: "CE-12086", summary: "c3.ABS-CBN — Plays/Attempts up, opposite trend for other metrics", status: "In Progress", priority: "P1", assignee: "Aniruddha V" },
        { key: "CE-12073", summary: "World Cup Prep — Ad Planning (capacity/SDK analysis)", status: "Open", priority: "P1", assignee: "Shubhansh V" },
        { key: "CE-12054", summary: "K8S pods hang on SSD delivery — blocking minute/hourly deliveries", status: "Open", priority: "P2", assignee: "Jingyuan Zhang" },
        { key: "CE-12035", summary: "[Fancode] SSD delivery: S3 to GCS destination change", status: "Open", priority: "P2", assignee: "Saraswathi M" },
        { key: "CE-11960", summary: "Missing Global Custom Tags in Raw ECO (DPI) Feeds (SlingTV, DANY)", status: "In Progress", priority: "P2", assignee: "Glauco Valdes" },
        { key: "CE-11914", summary: "c3.ITV: Incorrect device metadata on Pulse dashboard", status: "In Progress", priority: "P2", assignee: "Klaudia Legutko" },
      ]),

      gap(80),

      para([
        run("Priority escalation recommended: ", { bold: true }),
        run("CE-12054 (K8S hang) and CE-11960 (missing custom tags in DPI) have been open for 25 and 64 days respectively without resolution. CE-11960 affects SlingTV — a P1-risk customer — and involves data integrity in raw ECO feeds."),
      ], { before: 0, after: 60 }),

      gap(200),

      // ── RECOMMENDATIONS ───────────────────────────────────────────────────────
      h1("Recommendations & Next Steps"),

      h2("Immediate (This Sprint)"),
      bullet("Escalate CE-12054 (K8S pod hang) to platform engineering with SLA — this is blocking all SSD deliveries and has no workaround.", { bold: false }),
      bullet("Drive CE-12105 (SlingTV missing files Jun 14/18) to resolution; this is a P1 with a major SSD customer open for 8 days."),
      bullet("Add delivery validation to SSD pipelines: verify at least one output file exists at destination before marking Airflow run as success."),

      gap(60),
      h2("Near-Term (Next 30 Days)"),
      bullet("Build a DPI upstream health monitor that fires a PagerDuty alert within 5 minutes of a data gap — preventing multi-hour outages from cascading across all 7 DPI pipelines."),
      bullet("Document a fast-path DPI backfill runbook. The May 22 incident took 17 days to fully resolve; it should take under 4 hours."),
      bullet("Add a daily SSD vs Pulse reconciliation check for DAZN, Disney, CW, and SlingTV — the four customers with recurring discrepancy tickets."),
      bullet("Fix MaxMind ASN mapping for ITV (AS8801) and review all ISP entries flagged as incorrect in the last 12 months."),

      gap(60),
      h2("Strategic (Next Quarter)"),
      bullet("Conduct a post-mortem on the Legacy-to-Connect migration and publish an updated migration checklist with schema pre-validation, session-count reconciliation, and a 3-day parallel-run requirement before Legacy cutoff."),
      bullet("Invest in DST regression tests for all SSD pipelines: simulate BST/EST/AEST transitions and verify cross-midnight session boundary behavior before each changeover date."),
      bullet("Evaluate whether the SSD platform's 30-day data retention policy meets customer needs. Three separate backfill requests (Thmanyah requesting 90 days, others) indicate the limit is too short for some use cases."),
      bullet("Review Kubernetes pod startup reliability with the platform team; consider moving SSD Spark jobs to a dedicated node pool with guaranteed resource allocation to eliminate startup hangs."),

      gap(200),

      // ── APPENDIX ─────────────────────────────────────────────────────────────
      h1("Appendix: All 100 Tickets — Summary Table"),

      para("Complete list of tickets analyzed, sorted by creation date (newest first):", { before: 0, after: 80 }),

      dataTable(
        ["Ticket", "Summary", "Status", "Pri", "Created"],
        [
          ["CE-12117","Custom Dimension 'live video name' Null in SSD (STV)","Info Request","P2","2026-06-22"],
          ["CE-12109","Tracking case: SSD impacts for IIRV2-448","Resolved","P1","2026-06-16"],
          ["CE-12105","c3.Echostar-SlingTV: Event SSD files missing 2026/06/14/18","Open","P1","2026-06-15"],
          ["CE-12100","VSI: Paramount-Plus UI loading slowness / Error Loading data","In Progress","P1","2026-06-14"],
          ["CE-12098","VSI: Paramount-Plus API and UI slowness since Jun 11","Closed","P1","2026-06-12"],
          ["CE-12091","Connect Pipeline UI — Blank Page","Resolved","P1","2026-06-12"],
          ["CE-12086","c3.ABS-CBN — Plays/Attempts up, opposite trend other metrics","In Progress","P1","2026-06-10"],
          ["CE-12073","World Cup Prep — Ad Planning (capacity/SDK analysis)","Open","P1","2026-06-05"],
          ["CE-12072","c3.Meo — Sample MMDI report request","Resolved","P2","2026-06-05"],
          ["CE-12068","c3.Qurate: HHID impact due to CE-12060","Resolved","P2","2026-06-04"],
          ["CE-12064","c3.DT: Ads — MDDI setup for Ads use cases","Resolved","P2","2026-06-03"],
          ["CE-12057","HHID Attribution impact from May 20 & 24/25 data loss","Resolved","P2","2026-06-01"],
          ["CE-12054","K8S pods hang — blocking minute/hour SSD deliveries","Open","P2","2026-05-29"],
          ["CE-12049","Qurate: UTM 256-byte frequency analysis","Resolved","P2","2026-05-27"],
          ["CE-12045","Backfill DPI upstream data (SlingTV + all Flow Feed customers)","Resolved","P2","2026-05-25"],
          ["CE-12035","[Fancode] SSD delivery destination: S3 → GCS","Open","P2","2026-05-21"],
          ["CE-12022","c3.Echostar-SlingTV: Event SSD files missing (several hours)","Resolved","P1","2026-05-19"],
          ["CE-12014","MDDI: Stop MDDI feed (Showmax decommission)","Resolved","P2","2026-05-12"],
          ["CE-12011","c3.SN — SSD set up for Azure (Rogers)","Resolved","P2","2026-05-12"],
          ["CE-12004","c3.VZY-Test: Backfill SSD Apr 22 – May 5 2026","Resolved","P1","2026-05-08"],
          ["CE-11992","c3.SkyShowtime — Geolocation: All cities Unknown (Galicia, Spain)","Resolved","P2","2026-05-04"],
          ["CE-11991","C3.Racing.com — iOS/Android apps not reporting data in SSD","Resolved","P2","2026-05-04"],
          ["CE-11976","AD SSD schema: wrong metric field names","Resolved","P2","2026-04-26"],
          ["CE-11960","Missing Global Custom Tags in Raw ECO (DPI) Data Feeds","In Progress","P2","2026-04-20"],
          ["CE-11959","Cross-day sessions missing from first day SSD file (Ziggo/SKO)","Resolved","P2","2026-04-20"],
          ["CE-11953","c3.VivaMedia: No DPI data since March 24 2026","Resolved","P1","2026-04-16"],
          ["CE-11950","FlowFeed: Can't upload DPI feed to SFTP folder","Resolved","P2","2026-04-15"],
          ["CE-11942","Wrong ASN mapping — c3.ITV","Resolved","P2","2026-04-13"],
          ["CE-11930","Dish/Boost Mobile — MVNO ISP reporting via MaxMind","Resolved","P2","2026-04-06"],
          ["CE-11928","VSI: 'Unexpected Metadata' missing from QA export (Disney)","Resolved","P2","2026-04-06"],
          ["CE-11923","c3.DSS-SVOD: EBVS split sessions same session ID","Resolved","P2","2026-03-31"],
          ["CE-11915","SlingTV: Set up DPI Event Feed with raw event enabled","Resolved","P2","2026-03-27"],
          ["CE-11914","c3.ITV: Incorrect device metadata on Pulse dashboard","In Progress","P2","2026-03-27"],
          ["CE-11905","BBCK-PerformGroup: Expose lat/lon from MaxMind (DAZN)","Resolved","P2","2026-03-24"],
          ["CE-11903","Coupang: ADSSD/SSD pipeline success but no files delivered","Resolved","P2","2026-03-23"],
          ["CE-11901","Disney: ESPN Routing hourly SSD delays blocking ingestion","Resolved","P2","2026-03-22"],
          ["CE-11897","Enable offline SSD for Apple VisionPro device","Resolved","P2","2026-03-19"],
          ["CE-11881","c3.STV: Increase dimension value char limit to 300","Resolved","P2","2026-03-11"],
          ["CE-11880","Flow feed: SlingTV minute delivery failed 2026-03-06 22:04","Resolved","P2","2026-03-10"],
          ["CE-11874","Qurate: Investigate fixed IP format for certain ISPs","Resolved","P2","2026-03-08"],
          ["CE-11873","Telus: Resend Legacy SSD files Feb 27 – Mar 4 2026","Resolved","P2","2026-03-06"],
          ["CE-11853","Connect: Pulse UI for pipeline management not working","Resolved","P2","2026-02-27"],
          ["CE-11838","DPI: c3.NBCU-NowUK SSD data issue","Resolved","P2","2026-02-19"],
          ["CE-11814","c3.MLB-ESPN-ROUTING: Inflated session counts daily→hourly SSD","Resolved","P2","2026-02-09"],
          ["CE-11807","c3.Thmanyah — Request historical data for 90 days","Resolved","P1","2026-02-02"],
          ["CE-11806","c3.Thmanyah — Error creating SSD pipeline for DPI","Resolved","P2","2026-01-29"],
          ["CE-11802","c3.MLB/ESPN: Add PostalCode, DMA, timestamps to Connect feed","Resolved","P2","2026-01-26"],
          ["CE-11789","c3.ABS-CBN-CMS-Test: Video session expired with heartbeats","Resolved","P2","2026-01-21"],
          ["CE-11783","c3.CW: Discrepancy Unique Devices SSD vs UI (20K gap)","Resolved","P2","2026-01-20"],
          ["CE-11741","c3.BBCK-PerformGroup: SSD data repair Dec 20-21 2025 (DAZN)","Closed","P1","2025-12-23"],
          ["CE-11737","Precision API tag transformations documentation","Resolved","P2","2025-12-19"],
          ["CE-11734","c3.BBCK-PerformGroup: Add PauseTime/PausedRatio/NumBitrateSwitches (DAZN)","Resolved","P2","2025-12-18"],
          ["CE-11728","c3.BBCK-PerformGroup: Add PostalCode to hourly SSD (DAZN)","Resolved","P2","2025-12-16"],
          ["CE-11725","c3.Telus: Cannot ingest SSD files after Legacy migration","Closed","P2","2025-12-12"],
          ["CE-11721","c3.BBCK-PerformGroup-Test: Add NumBitrateSwitches hourly SSD","Closed","P2","2025-12-10"],
          ["CE-11710","Coupang: Sample query for EDPR via SSD feed","Resolved","P2","2025-12-04"],
          ["CE-11705","c3.BBCK-PerformGroup-Test: Add PausedRatio/PauseTime/PostalCode","Resolved","P2","2025-12-01"],
          ["CE-11702","c3.BBCK-PerformGroup: Data discrepancy Trends vs SSD (DAZN)","Resolved","P2","2025-12-01"],
          ["CE-11692","Restrict customer access to Pipeline List Page","Closed","P2","2025-11-25"],
          ["CE-11689","c3.MLB-Baseball: Migrate Legacy SSD to GCS","Resolved","P2","2025-11-24"],
          ["CE-11685","C3.Airtel: SSD files backfill (Nov 17-19 IST)","Resolved","P2","2025-11-20"],
          ["CE-11674","Disney: Content Category 'Unknown' in Pulse vs SSD","Resolved","P1","2025-11-13"],
          ["CE-11671","c3.DSS-Test: Touchstone shows 'Android Unknown' device name","Closed","P2","2025-11-12"],
          ["CE-11669","Legacy SSD Migration — Deprecation of Box SSD Delivery","Resolved","P2","2025-11-11"],
          ["CE-11667","ESPN GOAL: Latest iPhone model unknown in DPI","Closed","P2","2025-11-11"],
          ["CE-11658","c3.Echostar-DANY — ECO Event SSD pipeline setup","Resolved","P2","2025-11-03"],
          ["CE-11657","DPI Event SSD: All field names have '_10523' appended (SlingTV)","Closed","P1","2025-10-31"],
          ["CE-11650","CME: Connect file not delivered due to DST change","Closed","P1","2025-10-27"],
          ["CE-11645","VSI: Drop data for unsupported OS (PS4/5, LG, Tizen)","Resolved","P2","2025-10-24"],
          ["CE-11644","VSI: Increase custom metadata tags at Gateway","Resolved","P2","2025-10-23"],
          ["CE-11614","Determine active/in-use Ad SSD accounts (internal audit)","Resolved","P2","2025-10-15"],
          ["CE-11595","Legacy SSD disable (c3.NBCU-D2C)","Resolved","P2","2025-10-08"],
          ["CE-11588","Sample Event SSD files for c3.Gotham","Resolved","P2","2025-10-07"],
          ["CE-11565","Sunset duplicate Legacy on Connect pipelines (Telus/Sky/Qurate)","Resolved","P2","2025-09-23"],
          ["CE-11557","Legacy SSD Migration: Disable former customer accounts","Resolved","P2","2025-09-21"],
          ["CE-11551","c3.NFL-DIGITAL-MEDIA: Duplicate FlowId in SSD files","Resolved","P2","2025-09-17"],
          ["CE-11545","VSI: Heartbeat loss for c3.Astro-Sooka Samsung/LG TV","Resolved","P2","2025-09-11"],
          ["CE-11540","Disney: SSD data missing for last hour","Closed","P2","2025-09-05"],
          ["CE-11523","Peacock — MDDI Data Feed: Missing minutes Aug 24","Closed","P1","2025-08-25"],
          ["CE-11511","VSI: Map Apple Simulator traffic for device identification","Resolved","P1","2025-08-20"],
          ["CE-11507","c3.SkyShowtime: Get daily Connect file with CDN filters","Resolved","P2","2025-08-19"],
          ["CE-11506","Portal API: Wrong timezone userlist for SSD","Resolved","P2","2025-08-19"],
          ["CE-11493","Ad Uptime/SPI calculation from Ad SSD data (MLB)","Resolved","P2","2025-08-12"],
          ["CE-11486","NBCU-D2C: RequestId format changed (Sky raised)","Resolved","P2","2025-08-07"],
          ["CE-11484","c3.Telstra-AFL: AdsSSD files not visible in S3 bucket","Resolved","P2","2025-08-06"],
          ["CE-11463","Gotham & OSNTV — DPI SSD flow feed delivery failures Jul 24+","Resolved","P2","2025-07-28"],
          ["CE-11442","c3.STV: No individual event metrics from Flows feed since Jul 11","Resolved","P1","2025-07-17"],
          ["CE-11431","c3.SunNXT: Session Tag in Pulse but not in Legacy SSD","Resolved","P2","2025-07-16"],
          ["CE-11421","c3.MLB: Error creating Connect Ad SSD pipeline with traffic rules","Resolved","P2","2025-07-14"],
          ["CE-11420","c3.BBCK-PerformGroup: 3+ hour delay hourly SSD Jul 14 (DAZN)","Resolved","P2","2025-07-14"],
          ["CE-11419","Missing midnight boundary sessions in daily data (Sky UK)","Resolved","P2","2025-07-14"],
          ["CE-11414","Disney: Duplicate sessionIds in SSDs across different c3s","Resolved","P1","2025-07-10"],
          ["CE-11407","OSNTV: Deliver DPI flow sample for migration","Resolved","P2","2025-07-09"],
          ["CE-11405","c3.PLDT — Decommission Smart Livestream Platform (data purge)","Resolved","P2","2025-07-08"],
          ["CE-11373","c3.starhub: Workflow stopped — missing data after Mar 25","Resolved","P2","2025-06-22"],
          ["CE-11364","c3.SkyShowtime: ASN showing 2 ISPs post rebranding","Resolved","P2","2025-06-19"],
          ["CE-11349","c3.Airtel: Viewer ID report filtered by country","Resolved","P2","2025-06-13"],
          ["CE-11343","FIFA-Plus-Test: SSD vs Trends/Viewer metric discrepancy","Resolved","P2","2025-06-11"],
          ["CE-11342","BBCK-PerformGroup: article_id tag values differ across SSD files","Resolved","P2","2025-06-11"],
          ["CE-11341","c3.BBCK-PerformGroup: isAutoPlay attribute in hourly SSD files?","Resolved","P2","2025-06-11"],
        ],
        [1100, 5300, 1300, 700, 960]
      ),

    ]
  }]
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync("/sessions/clever-zealous-einstein/mnt/outputs/SSD_Escalation_Report.docx", buf);
  console.log("Written: SSD_Escalation_Report.docx");
});
