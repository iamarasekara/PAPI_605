// productCountTally.js

const axios = require('axios');
const fs = require('fs');

// Function to fetch published product count from Product API DB
async function fetchPublishedProductCount() {
    // Replace with actual API endpoint
    const response = await axios.get('https://api.example.com/products/count');
    return response.data.count;
}

// Function to fetch synced product count from Product Publisher Web Sync
async function fetchSyncedProductCount() {
    // Replace with actual API endpoint
    const response = await axios.get('https://sync.example.com/products/count');
    return response.data.count;
}

// Function to compare product counts
async function compareProductCounts() {
    const publishedCount = await fetchPublishedProductCount();
    const syncedCount = await fetchSyncedProductCount();
    const timestamp = new Date().toISOString();

    let result;
    if (publishedCount === syncedCount) {
        result = `Counts match at ${timestamp}: ${publishedCount}`;
    } else {
        result = `Counts do not match! Published: ${publishedCount}, Synced: ${syncedCount} at ${timestamp}`;
    }

    // Log result to a file
    fs.appendFile('productCountTally.log', result + '\n', (err) => {
        if (err) {
            console.error('Error writing to log file', err);
        }
    });
}

compareProductCounts();