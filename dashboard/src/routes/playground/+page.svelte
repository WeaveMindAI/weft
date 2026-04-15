<script lang="ts">
	import { page } from "$app/stores";
	import { browser } from "$app/environment";
	import { parseWeft } from "$lib/ai/weft-parser";
	import type { ProjectDefinition, NodeExecution, NodeExecutionTable } from "$lib/types";
	import ProjectEditor from "$lib/components/project/ProjectEditor.svelte";
	import { onMount } from "svelte";

	let project = $state<ProjectDefinition | null>(null);
	let editorKey = $state(0);
	let executionState = $state<{
		isRunning: boolean;
		activeEdges: Set<string>;
		nodeOutputs: Record<string, unknown>;
		nodeStatuses: Record<string, string>;
		nodeExecutions: NodeExecutionTable;
	} | undefined>(undefined);

	// Demo projects keyed by ID
	const DEMOS: Record<string, string> = {
		'hello-world': `# Project: Hello World
# Description: Displays a greeting

hello = Text {
  label: "Greeting"
  value: "Hello, world!"
}

output = Debug { label: "Output" }
output.data = hello.value`,

		'llm-analysis': `# Project: Poem Generator
# Description: Writes a poem about any topic

topic = Text {
  label: "Topic"
  value: "the silence between stars"
}

config = LlmConfig {
  label: "Config"
  model: "anthropic/claude-sonnet-4.6"
  systemPrompt: "Write a short, beautiful poem (4-6 lines) about the given topic. Just the poem, nothing else."
  temperature: "0.8"
}

poet = LlmInference -> (response: String) {
  label: "Poet"
}
poet.prompt = topic.value
poet.config = config.config

output = Debug { label: "Poem" }
output.data = poet.response`,

		'lead-pipeline': `# Project: Lead Pipeline
# Description: Scores and routes leads with human review

input_name = Text { label: "Name", value: "Alice Johnson" }
input_company = Text { label: "Company", value: "Acme Corp" }

scorer = Group(name: String, company: String) -> (score: Number, tier: String) {
  # Enriches and scores a lead

  enricher = ApolloEnrich { label: "Enrich" }
  enricher.name = self.name
  enricher.company = self.company

  classifier = ExecPython(data: String) -> (score: Number, tier: String) {
    label: "Classify"
    code: \`\`\`
score = min(100, int(data.get("revenue", 0)) / 1000)
tier = "hot" if score > 80 else "warm" if score > 50 else "cold"
return {"score": score, "tier": tier}
    \`\`\`
  }
  classifier.data = enricher.result
  scorer.score = classifier.score
  scorer.tier = classifier.tier
}
scorer.name = input_name.value
scorer.company = input_company.value

output = Debug { label: "Result" }
output.data = scorer.tier`,

		'recursive': `# Project: Lead Pipeline
# Description: Scores and routes leads

pipeline = Group(name: String, company: String) -> (result: String) {
  # Enriches, scores, and routes a lead end-to-end

  scorer = Group(name: String, company: String) -> (score: Number, tier: String) {
    # Enriches and scores a lead

    enricher = ExecPython(name: String, company: String) -> (revenue: Number, employees: Number) {
      label: "Enrich"
      code: \`\`\`
data = {
    "Acme Corp": {"revenue": 95000, "employees": 500},
    "Startup Inc": {"revenue": 12000, "employees": 15},
}
info = data.get(company, {"revenue": 50000, "employees": 100})
return {"revenue": info["revenue"], "employees": info["employees"]}
      \`\`\`
    }
    enricher.name = self.name
    enricher.company = self.company

    classifier = ExecPython(revenue: Number, employees: Number) -> (score: Number, tier: String) {
      label: "Classify"
      code: \`\`\`
score = min(100, revenue // 1000)
tier = "hot" if score > 80 else "warm" if score > 50 else "cold"
return {"score": score, "tier": tier}
      \`\`\`
    }
    classifier.revenue = enricher.revenue
    classifier.employees = enricher.employees

    self.score = classifier.score
    self.tier = classifier.tier
  }
  scorer.name = self.name
  scorer.company = self.company

  router = ExecPython(tier: String, name: String, score: Number) -> (hot: String?, warm: String?) {
    label: "Route"
    code: \`\`\`
msg = f"{name} (score: {score})"
return {
    "hot": msg if tier == "hot" else None,
    "warm": msg if tier != "hot" else None
}
    \`\`\`
  }
  router.tier = scorer.tier
  router.name = self.name
  router.score = scorer.score

  # Only hot leads pass through. Warm leads become null,
  # demonstrating null propagation downstream.
  self.result = router.hot
}

input_name = Text { label: "Name", value: "Alice Johnson" }
input_company = Text { label: "Company", value: "Acme Corp" }
pipeline.name = input_name.value
pipeline.company = input_company.value

output = Debug { label: "Result" }
output.data = pipeline.result`,

		'first-class': `# Project: First-Class Citizens
# Description: LLM analyzes a lead, a human approves or rejects, Slack notifies on approval

lead_data = Text {
  label: "Lead Data"
  value: "Alice Johnson, VP of Engineering at Acme Corp"
}

# --- First-class citizen: AI ---
llm_config = LlmConfig {
  label: "LLM Config"
  model: "anthropic/claude-sonnet-4.6"
  systemPrompt: "Analyze this lead. Write a short assessment: who they are, why they matter, and a recommended action."
  temperature: "0.3"
}

analyzer = LlmInference -> (response: String) {
  label: "AI: Analyze Lead"
}
analyzer.prompt = lead_data.value
analyzer.config = llm_config.config

# --- First-class citizen: Human ---
review = HumanQuery {
  label: "Human: Approve or Reject"
  title: "Review this lead"
  description: "Read the AI assessment and decide whether to notify the sales team."
  fields: [
    {"fieldType": "display", "key": "assessment", "required": true},
    {"fieldType": "approve_reject", "key": "decision"},
    {"fieldType": "textarea", "key": "notes"}
  ]
}
review.assessment = analyzer.response

# --- First-class citizen: Service ---
# Only runs if approved. If rejected, decision_approved is null,
# so the required text input skips the node via null propagation.
slack_config = SlackConfig {
  label: "Slack Config"
}

channel_id = Text {
  label: "Slack Channel"
  value: "C0123456789"
}

format_message = ExecPython(assessment: String, approved: Boolean, notes: String?) -> (message: String) {
  label: "Format Message"
  code: \`\`\`
lines = [f"✅ Lead approved\\n\\n{assessment}"]
if notes:
    lines.append(f"\\nManager notes: {notes}")
return {"message": "\\n".join(lines)}
  \`\`\`
}
format_message.assessment = analyzer.response
format_message.approved = review.decision_approved
format_message.notes = review.notes

notify = SlackSend { label: "Service: Notify Sales" }
notify.text = format_message.message
notify.config = slack_config.config
notify.channelId = channel_id.value

# If rejected, this debug node shows it (also via null propagation from the other branch)
rejected = Debug { label: "Rejected" }
rejected.data = review.decision_rejected`,

		'if-it-compiles': `# Project: If It Compiles, It Runs
# Description: 7 bugs the compiler catches before anything runs

lead_name = Text { label: "Lead Name", value: "Alice Johnson" }
lead_revenue = Number { value: 95000 }

llm_config = LlmConfig {
  label: "LLM Config"
  model: "anthropic/claude-sonnet-4.6"
  systemPrompt: "Analyze this lead and return a one-sentence assessment."
  temperature: "0.3"
}

email_config = EmailConfig {
  label: "Email Config"
  host: "smtp.gmail.com"
  port: "587"
  security: "starttls"
}

# Bug 1: Type mismatch. revenue is Number, but prompt expects String.
analyzer = LlmInference -> (response: String) { label: "Analyze Lead" }
analyzer.prompt = lead_revenue.value
analyzer.config = llm_config.config

# Bug 2: Wrong config node. LlmInference validates that config
# comes from LlmConfig, not EmailConfig.
second_llm = LlmInference -> (response: String) { label: "Second Opinion" }
second_llm.prompt = lead_name.value
second_llm.config = email_config.config

# Bug 3: Typo in port name. "analysis" doesn't exist, it's "response".
scorer = ExecPython(text: String) -> (score: Number) {
  label: "Score Lead"
  code: \`\`\`
score = len(text) * 2
return {"score": score}
  \`\`\`
}
scorer.text = analyzer.analysis

# Bug 4: Missing required connection. WebSearch needs a query.
search = WebSearch { label: "Research Company" }

# Bug 5: Custom ports on a node that doesn't support them.
search2 = WebSearch -> (summary: String) { label: "Custom Search" }
search2.query = lead_name.value

# Bug 6: Referencing a node that doesn't exist.
output = Debug { label: "Final Result" }
output.data = enricher.result`,

		'feedback-triage': `# Project: Customer Feedback Triage
# Description: Incoming support emails are classified by AI, routed by severity, reviewed by a human for critical issues, and dispatched to the right Slack channel

# --- Email Trigger ---

email_config = EmailConfig {
  label: "Email Config"
  protocol: "imap"
  host: "imap.gmail.com"
  port: "993"
  security: "tls"
}

incoming_email = EmailReceive {
  label: "Incoming Email"
  mailbox: "INBOX"
}
incoming_email.config = email_config.config

# --- Classification Group ---

classify = Group(
  sender: String,
  subject: String,
  body: String
) -> (
  category: String,
  severity: String,
  summary: String,
  suggested_action: String,
  formatted_context: String
) {
  # Classifies the email using an LLM into category, severity, and next steps

  llm_config = LlmConfig {
    label: "Classifier Config"
    model: "anthropic/claude-sonnet-4.6"
    systemPrompt: \`\`\`
You are a customer support triage system. Classify incoming emails and extract structured information.

Categories:
- bug_report: Something is broken or not working as expected
- feature_request: User wants new functionality or improvements
- churn_risk: User is frustrated, threatening to leave, or asking about cancellation
- general_question: How-to questions, billing inquiries, general support

Severity levels:
- critical: Production down, data loss, security issue, or angry enterprise customer
- high: Major feature broken, churn signals from paying customer
- medium: Minor bug, feature request from active user
- low: General question, nice-to-have request

Return ONLY valid JSON:
{
  "category": "bug_report|feature_request|churn_risk|general_question",
  "severity": "critical|high|medium|low",
  "summary": "One-sentence summary of the issue",
  "suggested_action": "Brief recommended next step"
}
    \`\`\`
    temperature: "0.1"
  }

  prompt_template = Text {
    label: "Classification Prompt"
    value: \`\`\`
Classify this customer email:

From: {{sender}}
Subject: {{subject}}

Body:
{{body}}
    \`\`\`
  }

  build_prompt = Template(sender: String, subject: String, body: String) {
    label: "Build Prompt"
  }
  build_prompt.template = prompt_template.value
  build_prompt.sender = self.sender
  build_prompt.subject = self.subject
  build_prompt.body = self.body

  classifier = LlmInference -> (response: String) {
    label: "Classify Email"
  }
  classifier.prompt = build_prompt.text
  classifier.config = llm_config.config

  parse = ExecPython(raw: String, sender: String, subject: String) -> (
    category: String,
    severity: String,
    summary: String,
    suggested_action: String,
    formatted_context: String
  ) {
    label: "Parse Classification"
    code: \`\`\`
import json
try:
    data = json.loads(raw)
    category = data.get("category", "general_question")
    severity = data.get("severity", "medium")
    summary = data.get("summary", "No summary")
    suggested_action = data.get("suggested_action", "Review manually")
except (json.JSONDecodeError, ValueError, TypeError):
    category = "general_question"
    severity = "medium"
    summary = "Failed to parse classification"
    suggested_action = "Review manually"

severity_emoji = {"critical": "\\U0001f534", "high": "\\U0001f7e0", "medium": "\\U0001f7e1", "low": "\\U0001f7e2"}.get(severity, "\\u26aa")
category_label = {"bug_report": "Bug Report", "feature_request": "Feature Request", "churn_risk": "Churn Risk", "general_question": "General Question"}.get(category, category)

formatted_context = f"{severity_emoji} *{category_label}* ({severity})\\n*From:* {sender}\\n*Subject:* {subject}\\n*Summary:* {summary}\\n*Action:* {suggested_action}"

return {
    "category": category,
    "severity": severity,
    "summary": summary,
    "suggested_action": suggested_action,
    "formatted_context": formatted_context
}
    \`\`\`
  }
  parse.raw = classifier.response
  parse.sender = self.sender
  parse.subject = self.subject

  self.category = parse.category
  self.severity = parse.severity
  self.summary = parse.summary
  self.suggested_action = parse.suggested_action
  self.formatted_context = parse.formatted_context
}

classify.sender = incoming_email.from
classify.subject = incoming_email.subject
classify.body = incoming_email.body

# --- Route by Priority ---

router = ExecPython(
  category: String,
  severity: String,
  formatted_context: String
) -> (
  critical_alert: String?,
  feature_log: String?,
  low_priority: String?
) {
  label: "Route by Priority"
  code: \`\`\`
critical_alert = None
feature_log = None
low_priority = None

if severity in ("critical", "high") or category == "churn_risk":
    critical_alert = formatted_context
elif category == "feature_request":
    feature_log = formatted_context
else:
    low_priority = formatted_context

return {
    "critical_alert": critical_alert,
    "feature_log": feature_log,
    "low_priority": low_priority
}
  \`\`\`
}
router.category = classify.category
router.severity = classify.severity
router.formatted_context = classify.formatted_context

# --- Human Review for Critical Items ---

review = HumanQuery {
  label: "Review Critical Issue"
  title: "Critical Feedback Requires Review"
  description: "An urgent customer issue needs your attention. Review and decide how to proceed."
  fields: [
    {"fieldType": "display", "key": "alert_details", "required": true},
    {"fieldType": "display", "key": "suggested_action"},
    {"fieldType": "approve_reject", "key": "decision"},
    {"fieldType": "textarea", "key": "reviewer_notes"}
  ]
}
review.alert_details = router.critical_alert
review.suggested_action = classify.suggested_action

# --- Slack Setup ---

slack_config = SlackConfig {
  label: "Slack Config"
}

eng_channel = Text {
  label: "Engineering Channel ID"
  value: ""
}

product_channel = Text {
  label: "Product Channel ID"
  value: ""
}

# --- Send Critical Alerts (after approval) ---

build_critical_msg = ExecPython(
  context: String,
  notes: String?
) -> (message: String) {
  label: "Build Critical Alert"
  code: \`\`\`
msg = "\\U0001f6a8 *CRITICAL CUSTOMER ISSUE*\\n\\n" + context
if notes:
    msg += f"\\n\\n*Reviewer Notes:* {notes}"
return {"message": msg}
  \`\`\`
}
build_critical_msg.context = router.critical_alert
build_critical_msg.notes = review.reviewer_notes

critical_gate = Gate {
  label: "Approval Gate"
}
critical_gate.pass = review.decision_approved
critical_gate.value = build_critical_msg.message

send_critical = SlackSend {
  label: "Alert Engineering"
}
send_critical.config = slack_config.config
send_critical.channelId = eng_channel.value
send_critical.text = critical_gate.value

# --- Send Feature Requests (no review needed) ---

build_feature_msg = ExecPython(context: String) -> (message: String) {
  label: "Build Feature Log"
  code: \`\`\`
msg = "\\U0001f4a1 *New Feature Request*\\n\\n" + context
return {"message": msg}
  \`\`\`
}
build_feature_msg.context = router.feature_log

send_feature = SlackSend {
  label: "Log to Product"
}
send_feature.config = slack_config.config
send_feature.channelId = product_channel.value
send_feature.text = build_feature_msg.message

# --- Debug ---

debug_classification = Debug { label: "Classification Result" }
debug_classification.data = classify.formatted_context`,
	};

	// Nodes to pre-expand per demo
	const DEMO_EXPANDED: Record<string, Set<string>> = {
		'hello-world': new Set(['hello', 'output']),
		'llm-analysis': new Set(['output', 'poet']),
		'feedback-triage': new Set(['debug_classification']),
	};

	// Fake execution results per demo, shows what a completed run looks like
	const DEMO_RESULTS: Record<string, Record<string, { output: unknown; input?: unknown; status?: string }>> = {
		'hello-world': {
			'hello': { output: { value: 'Hello, world!' } },
			'output': { output: { data: 'Hello, world!' }, input: { data: 'Hello, world!' } },
		},
		'llm-analysis': {
			'topic': { output: { value: 'the silence between stars' } },
			'config': { output: { config: { model: 'anthropic/claude-sonnet-4.6' } } },
			'poet': { output: { response: 'Between the stars, a hush so wide\nno wave has ever crossed its span.\nIt holds the light on every side\nand asks for nothing back from man.' } },
			'output': { output: { data: 'Between the stars, a hush so wide\nno wave has ever crossed its span.\nIt holds the light on every side\nand asks for nothing back from man.' }, input: { data: 'Between the stars, a hush so wide\nno wave has ever crossed its span.\nIt holds the light on every side\nand asks for nothing back from man.' } },
		},
		'feedback-triage': {
			'email_config': { output: { config: { protocol: 'imap', host: 'imap.gmail.com' } } },
			'incoming_email': { output: { from: 'sarah@bigcorp.com', subject: 'URGENT: Production API returning 500 errors', body: 'Hi, our entire integration with your API has been down for 2 hours. We have a board meeting in 30 minutes and need this resolved immediately. This is affecting all 200 of our users. If this isn\'t fixed today we will need to evaluate alternatives.' } },
			'classify': { output: { category: 'churn_risk', severity: 'critical', summary: 'Enterprise customer experiencing production API outage, threatening to leave', suggested_action: 'Escalate to engineering lead immediately, provide status update within 15 minutes', formatted_context: '🔴 *Churn Risk* (critical)\n*From:* sarah@bigcorp.com\n*Subject:* URGENT: Production API returning 500 errors\n*Summary:* Enterprise customer experiencing production API outage, threatening to leave\n*Action:* Escalate to engineering lead immediately' } },
			'classify.llm_config': { output: { config: { model: 'anthropic/claude-sonnet-4.6' } } },
			'classify.prompt_template': { output: { value: 'Classify this customer email:\n\nFrom: {{sender}}\nSubject: {{subject}}\n\nBody:\n{{body}}' } },
			'classify.build_prompt': { output: { text: 'Classify this customer email:\n\nFrom: sarah@bigcorp.com\nSubject: URGENT: Production API returning 500 errors\n\nBody:\nHi, our entire integration with your API has been down for 2 hours...' } },
			'classify.classifier': { output: { response: '{"category": "churn_risk", "severity": "critical", "summary": "Enterprise customer experiencing production API outage, threatening to leave", "suggested_action": "Escalate to engineering lead immediately, provide status update within 15 minutes"}' } },
			'classify.parse': { output: { category: 'churn_risk', severity: 'critical', summary: 'Enterprise customer experiencing production API outage, threatening to leave', suggested_action: 'Escalate to engineering lead immediately', formatted_context: '🔴 *Churn Risk* (critical)\n*From:* sarah@bigcorp.com\n*Subject:* URGENT: Production API returning 500 errors\n*Summary:* Enterprise customer experiencing production API outage, threatening to leave\n*Action:* Escalate to engineering lead immediately' } },
			'router': { output: { critical_alert: '🔴 *Churn Risk* (critical)\n*From:* sarah@bigcorp.com\n*Subject:* URGENT: Production API returning 500 errors\n*Summary:* Enterprise customer experiencing production API outage, threatening to leave\n*Action:* Escalate to engineering lead immediately', feature_log: null, low_priority: null } },
			'review': { output: { alert_details: '🔴 *Churn Risk* (critical)\n...', decision_approved: true, decision_rejected: null, reviewer_notes: 'Confirmed outage. Engineering notified via PagerDuty. Customer updated.' }, status: 'completed' },
			'slack_config': { output: { config: { token: '***' } } },
			'eng_channel': { output: { value: 'C0ENGINEERING' } },
			'product_channel': { output: { value: 'C0PRODUCT' } },
			'build_critical_msg': { output: { message: '🚨 *CRITICAL CUSTOMER ISSUE*\n\n🔴 *Churn Risk* (critical)\n*From:* sarah@bigcorp.com\n*Summary:* Enterprise customer experiencing production API outage\n\n*Reviewer Notes:* Confirmed outage. Engineering notified via PagerDuty. Customer updated.' } },
			'critical_gate': { output: { value: '🚨 *CRITICAL CUSTOMER ISSUE*\n\n🔴 *Churn Risk* (critical)...' } },
			'send_critical': { output: { messageId: '1234567890.123456' }, status: 'completed' },
			'build_feature_msg': { output: { message: null }, status: 'skipped' },
			'send_feature': { output: {}, status: 'skipped' },
			'debug_classification': { output: { data: '🔴 *Churn Risk* (critical)\n*From:* sarah@bigcorp.com\n*Subject:* URGENT: Production API returning 500 errors\n*Summary:* Enterprise customer experiencing production API outage, threatening to leave\n*Action:* Escalate to engineering lead immediately' }, input: { data: '🔴 *Churn Risk* (critical)\n*From:* sarah@bigcorp.com\n...' } },
		},
	};

	function buildFakeExecution(demoId: string, nodeIds: string[]): typeof executionState {
		const results = DEMO_RESULTS[demoId];
		if (!results) return undefined;

		const nodeOutputs: Record<string, unknown> = {};
		const nodeStatuses: Record<string, string> = {};
		const nodeExecutions: NodeExecutionTable = {};

		for (const nodeId of nodeIds) {
			const result = results[nodeId];
			if (result) {
				nodeOutputs[nodeId] = result.output;
				nodeStatuses[nodeId] = result.status || 'completed';
				nodeExecutions[nodeId] = [{
					id: `exec-${nodeId}`,
					nodeId,
					status: (result.status || 'completed') as any,
					pulseIdsAbsorbed: [],
					pulseId: `pulse-${nodeId}`,
					startedAt: Date.now() - 1000,
					completedAt: Date.now(),
					input: result.input || {},
					output: result.output,
					costUsd: 0,
					logs: [],
					color: 'c1',
					lane: [],
				}];
			}
		}

		return {
			isRunning: false,
			activeEdges: new Set(),
			nodeOutputs,
			nodeStatuses,
			nodeExecutions,
		};
	}

	onMount(() => {
		if (!browser) return;
		const demoId = $page.url.searchParams.get('demo') || 'hello-world';
		const showResults = $page.url.searchParams.get('results') !== 'false';
		const code = DEMOS[demoId];
		if (!code) return;

		const weftBlock = '````weft\n' + code + '\n````';
		const result = parseWeft(weftBlock);
		if (result.projects.length > 0) {
			const parsed = result.projects[0];
			project = parsed.project;
			project.weftCode = code;

			// Pre-expand specified nodes
			const expandedSet = DEMO_EXPANDED[demoId];
			if (expandedSet) {
				for (const node of project.nodes) {
					if (expandedSet.has(node.id)) {
						(node.config as Record<string, unknown>).expanded = true;
					}
				}
			}

			if (showResults) {
				const nodeIds = parsed.project.nodes.map(n => n.id);
				executionState = buildFakeExecution(demoId, nodeIds);
			}

			editorKey++;
		}
	});

	function handleSave(_data: { weftCode?: string }) {
		// In playground mode, don't persist and don't re-parse.
		// The editor manages its own state internally.
	}
</script>

{#if project}
	<div class="h-screen w-screen">
		{#key editorKey}
			<ProjectEditor
				{project}
				onSave={handleSave}
				viewMode="builder"
				autoOrganizeOnMount={true}
				fitViewAfterOrganize={true}
				structuralLock={false}
				playground={true}
				{executionState}
			/>
		{/key}
	</div>
{:else}
	<div class="h-screen flex items-center justify-center">
		<p class="text-zinc-500">Loading playground...</p>
	</div>
{/if}
