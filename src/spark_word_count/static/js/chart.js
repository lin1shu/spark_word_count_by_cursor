/**
 * Creates an interactive bar chart for word frequencies
 */
document.addEventListener('DOMContentLoaded', function() {
    const chartContainer = document.getElementById('word-frequency-chart');
    
    if (!chartContainer) return;
    
    // Extract data from the page
    const wordData = window.wordFrequencyData || [];
    
    if (wordData.length === 0) return;
    
    // Prepare data for Chart.js
    const labels = wordData.map(item => item.word);
    const counts = wordData.map(item => item.count);
    
    // Create the chart
    const ctx = chartContainer.getContext('2d');
    const chart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Word Frequency',
                data: counts,
                backgroundColor: 'rgba(54, 162, 235, 0.6)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Frequency'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Words'
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `Frequency: ${context.parsed.y.toLocaleString()}`;
                        }
                    }
                },
                legend: {
                    display: false
                },
                title: {
                    display: true,
                    text: 'Word Frequency Distribution'
                }
            }
        }
    });
}); 