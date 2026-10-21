(() => {
  const sidebar = document.querySelector('#sidebar');
  const menuToggle = document.querySelector('[data-menu-toggle]');
  if (sidebar && menuToggle) {
    menuToggle.addEventListener('click', () => sidebar.classList.toggle('open'));
    document.addEventListener('click', (event) => {
      if (window.innerWidth <= 720 && sidebar.classList.contains('open') && !sidebar.contains(event.target) && event.target !== menuToggle) {
        sidebar.classList.remove('open');
      }
    });
  }

  document.querySelectorAll('[data-dismiss]').forEach((button) => {
    button.addEventListener('click', () => button.closest('.flash')?.remove());
  });

  document.querySelectorAll('[data-refresh]').forEach((button) => {
    button.addEventListener('click', () => window.location.reload());
  });

  document.querySelectorAll('[data-run-form]').forEach((form) => {
    form.addEventListener('submit', () => {
      const label = form.querySelector('[data-run-label]');
      const button = form.querySelector('button');
      if (label) label.textContent = 'Running…';
      if (button) button.disabled = true;
    });
  });

  document.querySelectorAll('.clickable-row[data-href]').forEach((row) => {
    row.addEventListener('click', () => { window.location.href = row.dataset.href; });
  });

  document.querySelectorAll('[data-range-days]').forEach((button) => {
    button.addEventListener('click', () => {
      const form = button.closest('.filter-panel')?.querySelector('form');
      if (!form) return;
      const days = Number(button.dataset.rangeDays || 7);
      const end = new Date();
      const start = new Date(end.getTime() - Math.max(days - 1, 0) * 86400000);
      const iso = (value) => value.toISOString().slice(0, 10);
      const startInput = form.querySelector('[name="start_date"]');
      const endInput = form.querySelector('[name="end_date"]');
      if (startInput) startInput.value = iso(start);
      if (endInput) endInput.value = iso(end);
      form.submit();
    });
  });

  const palette = {
    text: '#8ea4bf', grid: 'rgba(150, 181, 220, .12)', surface: '#0d223e',
  };
  document.querySelectorAll('canvas[data-chart]').forEach((canvas) => {
    if (!window.Chart) return;
    let rows = [];
    try { rows = JSON.parse(canvas.dataset.chart || '[]'); } catch (error) { return; }
    if (!rows.length) return;
    const mode = canvas.dataset.chartMode || 'analytics';
    const color = canvas.dataset.chartColor || '#73b7ff';
    const labels = mode === 'temperature' ? rows.map((row) => row.city) : mode === 'runs' ? rows.map((row) => `#${row.id}`) : rows.map((row) => row.label);
    const datasets = mode === 'runs' ? [
      { label: 'Inserted', data: rows.map((row) => row.records_inserted || 0), backgroundColor: '#73b7ff', borderRadius: 5 },
      { label: 'Rejected', data: rows.map((row) => row.records_rejected || 0), backgroundColor: '#ff8fa8', borderRadius: 5 },
      { label: 'Duplicates', data: rows.map((row) => row.duplicates_skipped || 0), backgroundColor: '#f5d27a', borderRadius: 5 },
    ] : [{
      label: canvas.dataset.chartLabel || 'Value',
      data: mode === 'temperature' ? rows.map((row) => row.temperature_c) : rows.map((row) => row.value),
      borderColor: color, backgroundColor: `${color}38`, borderWidth: 2, fill: mode !== 'temperature', tension: .35,
      pointBackgroundColor: color, pointBorderColor: palette.surface, pointBorderWidth: 2, pointRadius: mode === 'temperature' ? 4 : 3,
      borderRadius: mode === 'temperature' ? 6 : 0, maxBarThickness: 42,
    }];
    new window.Chart(canvas, {
      type: mode === 'runs' || mode === 'temperature' ? 'bar' : 'line', data: { labels, datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { intersect: false, mode: 'index' },
        onClick: (_event, elements) => {
          if (!elements.length) return;
          const row = rows[elements[0].index];
          if (row?.id) window.location.href = `/observations/${row.id}`;
          else if (row?.city) window.location.href = `/observations?city=${encodeURIComponent(row.city)}`;
        },
        plugins: { legend: { display: mode === 'runs', labels: { color: palette.text, boxWidth: 10, usePointStyle: true } }, tooltip: { displayColors: false } },
        scales: {
          y: { beginAtZero: mode === 'runs', grid: { color: palette.grid }, ticks: { color: palette.text, font: { size: 10 } } },
          x: { grid: { display: false }, ticks: { color: palette.text, font: { size: 10 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 8 } },
        },
      },
    });
  });
})();
