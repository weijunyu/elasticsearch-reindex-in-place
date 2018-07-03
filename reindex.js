let request = require('superagent');
let fs = require('fs');
let path = require('path');

let config = require('./config');

let ca = fs.readFileSync(path.resolve(__dirname, config.ca));

let indices = config.indices; // List or object holding names of indices to refresh
let settings = config.settings;
let mappings = config.mappings;
let suffix = config.suffix; // Suffix for new indices, eg v2

let indexNames;
if (Array.isArray(indices)) {
    indexNames = indices;
} else if (typeof indices === 'object') {
    indexNames = Object.keys(indices);
}

/**
 * Step 1: Create new indices and copy the mappings from the old index.
 * Use refresh_interval = -1 and number_of_replicas = 0 to prepare for the subsequent reindexing.
 */
function createNewIndices() {
    let promises = [];
    for (indexName of indexNames) {
        let endpoint = `https://${config.elasticsearch.host}/${indexName}${suffix}`;
        let requestPromise = request.put(endpoint)
            .ca(ca)
            .send({
                // First set to these for efficient reindexing
                settings: {
                    refresh_interval: -1,
                    number_of_replicas: 0
                },
                mappings: mappings.mappings
            })
            .then(function (response) {
                console.log(`Index created: ${indexName}${suffix}.`);
                return response.body
            })

        promises.push(requestPromise);
    }

    return Promise.all(promises)
        .then(function (result) {
            console.log(JSON.stringify(result, null, 2));
        })
        .catch(function (error) {
            console.log(JSON.stringify(error));
        })
}

/**
 * Step 2: Reindex documents from the old indices to the new ones.
 */
function reindexDocuments() {
    let promises = [];
    let reindexResults = {};
    let endpoint = `https://${config.elasticsearch.host}/_reindex`;
    for (let indexName of indexNames) {
        let requestPromise = request.post(endpoint)
            .ca(ca)
            .send({
                source: {
                    index: indexName
                },
                dest: {
                    index: indexName + suffix
                }
            })
            .then(function (response) {
                console.log(`Reindexed from ${indexName} to ${indexName}${suffix}.`);
                reindexResults[indexName] = {
                    to: indexName + suffix,
                    success: true
                }
            })
            .catch(function (error) {
                console.log(`Error reindexing from ${indexName} to ${indexName}${suffix}.`);
                reindexResults[indexName] = {
                    to: indexName + suffix,
                    success: false
                }
            })
        promises.push(requestPromise);
    }
    return Promise.all(promises)
        .then(function (results) {
            console.log(JSON.stringify(reindexResults, null, 2));
        })
        .catch(function (error) {
            console.log(JSON.stringify(error));
        })
}

/**
 * Step 3: Take settings from original indices and apply them to the newly created ones
 */
function resetOldIndicesSettings() {
    let promises = [];
    let resetResults = {};
    for (let indexName of indexNames) {
        let endpoint = `https://${config.elasticsearch.host}/${indexName}${suffix}/_settings`;
        let requestPromise = request.put(endpoint)
            .ca(ca)
            .send({
                index: settings.settings.index
            })
            .then(function (response) {
                console.log(`Reset settings for ${indexName}${suffix} success.`);
                resetResults[indexName + suffix] = {
                    success: true
                }
            })
            .catch(function (error) {
                console.log(`Reset settings for ${indexName}${suffix} failed.`);
                resetResults[indexName + suffix] = {
                    success: false,
                    error: error
                }
            })
        promises.push(requestPromise);
    }
    return Promise.all(promises)
        .then(function (results) {
            console.log(JSON.stringify(resetResults, null, 2));
        })
        .catch(function (error) {
            console.log(JSON.stringify(error));
        })
}

/** Step 4: Wait till all the new indices report status green */

/**
 * Step 5: In a single update aliases request:
 * Delete the old index.
 * Add an alias with the old index name to the new index.
 * Add any aliases that existed on the old index to the new index.
 */
function updateAliases() {
    let endpoint = `https://${config.elasticsearch.host}/_aliases`
    let promises = [];
    let updateResults = {};
    for (let indexName of indexNames) {
        let requestPromise = request.post(endpoint)
            .ca(ca)
            .send({
                actions: [
                    {
                        add: {
                            index: indexName + suffix,
                            alias: indexName
                        }
                    },
                    {
                        remove_index: {
                            index: indexName
                        }
                    }
                ]
            })
            .then(function(response) {
                console.log(`Update alias for ${indexName}${suffix} success.`);
                updateResults[indexName + suffix] = {
                    success: true
                }
            })
            .catch(function(error) {
                console.log(`Update alias for ${indexName}${suffix} failed.`);
                updateResults[indexName + suffix] = {
                    success: false,
                    error: error
                }
            })
        promises.push(requestPromise);
    }
    return Promise.all(promises)
        .then(function (results) {
            console.log(JSON.stringify(updateResults, null, 2));
        })
        .catch(function (error) {
            console.log(JSON.stringify(error));
        })
}
