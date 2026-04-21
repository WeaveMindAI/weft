// Tangle chat participant. Registered under the native VS Code
// chat panel so users invoke it with `@weft` alongside Copilot and
// other participants.
//
// Phase A2: routes every prompt to the dispatcher's AI builder
// endpoint; falls back to the user's VS Code language model
// (typically Copilot) if the dispatcher has nothing to say yet.
// Phase B: dispatcher hosts the real Tangle backend with per-project
// catalog context, our preferred model, and cost tracking.

import * as vscode from 'vscode';
import { DispatcherClient } from './dispatcher';

const SYSTEM_PROMPT = `You are Tangle, the AI builder for Weft. Weft is a visual/textual dataflow language where graphs are built from typed nodes connected by edges. Respond in clear markdown; when you scaffold code, emit a fenced \`\`\`weft\`\`\` block. Keep responses short unless the user asks for depth.`;

export function registerTangleParticipant(
  context: vscode.ExtensionContext,
  dispatcher: DispatcherClient,
): vscode.ChatParticipant {
  const handler: vscode.ChatRequestHandler = async (
    request: vscode.ChatRequest,
    _chatContext: vscode.ChatContext,
    stream: vscode.ChatResponseStream,
    token: vscode.CancellationToken,
  ): Promise<void> => {
    try {
      const replied = await streamFromDispatcher(dispatcher, request, stream, token);
      if (replied) return;
    } catch (err) {
      stream.markdown(`> dispatcher unreachable (${stringifyError(err)}), falling back to VS Code's default language model.\n\n`);
    }

    await streamFromVsCodeModel(request, stream, token);
  };

  const participant = vscode.chat.createChatParticipant('weft.tangle', handler);
  context.subscriptions.push(participant);
  return participant;
}

async function streamFromDispatcher(
  dispatcher: DispatcherClient,
  request: vscode.ChatRequest,
  stream: vscode.ChatResponseStream,
  _token: vscode.CancellationToken,
): Promise<boolean> {
  // Probe /projects to confirm the dispatcher is up. The real Tangle
  // endpoint lands in Phase B; for now we surface the connection
  // state and return false so the handler falls back to VS Code's
  // model. No silent stub: the user sees why we're not using the
  // dispatcher yet.
  await dispatcher.get<unknown[]>('/projects');
  stream.markdown(
    `_dispatcher reachable; Tangle's AI endpoint lands in Phase B. Using VS Code's model for now._\n\n`,
  );
  const _ = request;
  return false;
}

async function streamFromVsCodeModel(
  request: vscode.ChatRequest,
  stream: vscode.ChatResponseStream,
  token: vscode.CancellationToken,
): Promise<void> {
  const model = request.model;
  if (!model) {
    stream.markdown('No language model available. Install GitHub Copilot or another provider.');
    return;
  }

  const prompt = request.command
    ? `/${request.command}: ${request.prompt}`
    : request.prompt;

  const messages = [
    vscode.LanguageModelChatMessage.User(SYSTEM_PROMPT),
    vscode.LanguageModelChatMessage.User(prompt),
  ];

  const response = await model.sendRequest(messages, {}, token);
  for await (const fragment of response.text) {
    stream.markdown(fragment);
  }
}

function stringifyError(err: unknown): string {
  if (err instanceof Error) return err.message;
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
}
