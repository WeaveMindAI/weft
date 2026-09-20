document.addEventListener('click', async function (event) {
  var button = event.target.closest('.copy-markdown');
  if (!button) {
    return;
  }
  var status = button.parentElement.querySelector('.copy-markdown-status');
  try {
    await navigator.clipboard.writeText(button.dataset.markdown);
    status.textContent = 'Copied!';
  } catch (error) {
    status.textContent = 'Could not copy. Allow clipboard access and try again.';
  }
});
