document.addEventListener('DOMContentLoaded', function () {
    // Convert Django context data to JavaScript
    const chartDataParentProductGroup = JSON.parse(document.getElementById('chartDataParentProductGroup').textContent);

    // Prepare labels and datasets for Chart.js
    const labelsParentProductGroup = Object.keys(chartDataParentProductGroup);
    const parentGroups = [...new Set(Object.values(chartDataParentProductGroup).flatMap(Object.keys))];

    const datasetsParentProductGroup = parentGroups.map(group => ({
        label: group,
        data: labelsParentProductGroup.map(label => chartDataParentProductGroup[label][group] || 0),
        backgroundColor: getRandomColor(),
    }));

    function getRandomColor() {
        const letters = '0123456789ABCDEF';
        let color = '#';
        for (let i = 0; i < 6; i++) {
            color += letters[Math.floor(Math.random() * 16)];
        }
        return color;
    }

    const ctxParentGroup = document.getElementById('parentGroupChart').getContext('2d');
    const parentGroupChart = new Chart(ctxParentGroup, {
        type: 'bar',
        data: {
            labels: labelsParentProductGroup,
            datasets: datasetsParentProductGroup
        },
        options: {
            scales: {
                x: {
                    stacked: true
                },
                y: {
                    stacked: true
                }
            }
        }
    });

    // Repeat similar steps for other charts (productGroupChart, regionChart, customerChart)
    // Ensure you have the data prepared for each chart and initialize them similarly
});
