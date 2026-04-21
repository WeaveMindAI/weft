<script lang="ts">
  import { onMount } from 'svelte';

  interface ToastData {
    id: string;
    title: string;
    message: string;
    taskUrl?: string;
    actionUrl?: string;
    taskType?: 'Task' | 'Action';
  }

  let toasts = $state<ToastData[]>([]);

  onMount(() => {
    // Listen for toast messages from background script
    browser.runtime.onMessage.addListener((message: { type: string; toast?: ToastData }) => {
      if (message.type === 'SHOW_TOAST' && message.toast) {
        addToast(message.toast);
      }
    });
  });

  function addToast(toast: ToastData) {
    toasts = [...toasts, toast];
    // Auto-remove after 8 seconds
    setTimeout(() => {
      removeToast(toast.id);
    }, 8000);
  }

  function removeToast(id: string) {
    toasts = toasts.filter(t => t.id !== id);
  }

  function handleClick(toast: ToastData) {
    if (toast.taskType === 'Action' && toast.actionUrl) {
      // Action: open URL via background script and dismiss
      browser.runtime.sendMessage({ type: 'OPEN_AND_DISMISS_ACTION', actionUrl: toast.actionUrl });
    } else if (toast.taskUrl) {
      window.open(toast.taskUrl, '_blank');
    }
    removeToast(toast.id);
  }
</script>

<div class="weavemind-toast-container">
  {#each toasts as toast (toast.id)}
    <div 
      class="toast" 
      class:action={toast.taskType === 'Action'}
      onclick={() => handleClick(toast)} 
      onkeydown={(e) => e.key === 'Enter' && handleClick(toast)}
      role="button"
      tabindex="0"
    >
      <div class="toast-header">
        <div class="toast-dot"></div>
        <span class="toast-title">{toast.title}</span>
        <button type="button" class="toast-close" onclick={(e) => { e.stopPropagation(); removeToast(toast.id); }} aria-label="Dismiss">×</button>
      </div>
      <p class="toast-message">{toast.message}</p>
      {#if toast.taskType === 'Action' && toast.actionUrl}
        <p class="toast-hint">Click to open</p>
      {:else if toast.taskUrl}
        <p class="toast-hint">Click to review task</p>
      {/if}
    </div>
  {/each}
</div>

<style>
  .weavemind-toast-container {
    position: fixed;
    top: 16px;
    right: 16px;
    z-index: 2147483647;
    display: flex;
    flex-direction: column;
    gap: 8px;
    max-width: 360px;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  }

  .toast {
    background: white;
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15), 0 0 0 1px rgba(0, 0, 0, 0.05);
    padding: 12px 16px;
    cursor: pointer;
    animation: slideIn 0.3s ease-out;
    border-left: 3px solid #f59e0b;
  }

  .toast.action {
    border-left-color: #22c55e;
  }

  .toast.action .toast-dot {
    background: #22c55e;
  }

  .toast:hover {
    box-shadow: 0 6px 16px rgba(0, 0, 0, 0.2), 0 0 0 1px rgba(0, 0, 0, 0.05);
  }

  @keyframes slideIn {
    from {
      transform: translateX(100%);
      opacity: 0;
    }
    to {
      transform: translateX(0);
      opacity: 1;
    }
  }

  .toast-header {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 4px;
  }

  .toast-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #f59e0b;
    flex-shrink: 0;
  }

  .toast-title {
    font-size: 14px;
    font-weight: 600;
    color: #18181b;
    flex: 1;
  }

  .toast-close {
    background: none;
    border: none;
    font-size: 18px;
    color: #a1a1aa;
    cursor: pointer;
    padding: 0;
    line-height: 1;
    width: 20px;
    height: 20px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 4px;
  }

  .toast-close:hover {
    background: #f4f4f5;
    color: #71717a;
  }

  .toast-message {
    font-size: 13px;
    color: #52525b;
    margin: 0;
    line-height: 1.4;
  }

  .toast-hint {
    font-size: 11px;
    color: #a1a1aa;
    margin: 6px 0 0 0;
  }
</style>
