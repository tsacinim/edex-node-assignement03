const mongodb = require('mongodb');
const async = require('async');
const assert = require('assert');
const util = require('util');
const path = require('path');
// const fs = require('fs');
const url = 'mongodb://localhost:27017/edx-course-db';
const pathCustomers = path.join(__dirname, 'm3-customer-data.json');
const customers = require(pathCustomers);
const pathAddresses = path.join(__dirname, 'm3-customer-address-data.json');
const customerAddresses = require(pathAddresses);

// Used for testing
const customersRestoredTesting = customers.map(
  (x, i) => Object.assign({}, x, customerAddresses[i])
);

let tasks = [];
const chunkSize = parseInt(process.argv[2], 10) || 1000;

async function updateAndTest() {
  const db = await mongodb.MongoClient.connect(url);
  console.log('DB connection opened');
  try {
    const collection = db.collection('customers');

    // reset customers collection to an empty state before update (for testing)
    const prepareDB = await collection.remove({});
    let customerCount = await collection.find({}).limit(1).count();
    assert(customerCount === 0);
    console.log(prepareDB.result);

    // populate the tasks list with functions
    for (let i = 0, j = customers.length; i < j; i += chunkSize) {
      const chunkCustomers = customers.slice(i, i + chunkSize);
      const chunkAddresses = customerAddresses.slice(i, i + chunkSize);
      const chunkRestored = chunkCustomers.map(
        (x, idx) => Object.assign(x, chunkAddresses[idx])
      ); // assumes same order
      const end = i + chunkSize;
      tasks.push(done => {
        console.log(`Update ${i}-${end > j ? j : end} from ${j}`);
        collection.insert(chunkRestored, (error, data) => {
          done(error, data);
        });
      });
    }

    // Transform async.parallel from callback to promise
    const parallel = util.promisify(async.parallel);

    // Run all the update tasks in parallel
    console.log(`Launching ${tasks.length} parallel task(s)`);
    const startTime = Date.now();

    await parallel(tasks).then((data) => {
      const endTime = Date.now();
      console.log(`Execution time: ${endTime - startTime}`);
    }).catch((error) => {
      console.log(error);
    });

    // Check that the update was successfull
    customerCount = await collection.find({}).limit(1).count();

    assert(customerCount === 1000);
    console.log(`BD has ${customerCount} customers`);

    let testingDB = await collection.find({})
      .sort({id: 1}) // lexico by id (parralel inserts scramble order)
      .toArray();

    // remove the _id field (which was added by mongo and is dynamic)
    testingDB.map(x => delete x._id);
    // convert the entries to a string for testing
    const actual = JSON.stringify(testingDB, null, 2);

    // Used for testing
    const customersSorted = customersRestoredTesting
      // order lexicographically by id field (parralel inserts scramble order)
      .sort(function(a, b){ return a.id < b.id ? -1 : 1; });

    const expected = JSON.stringify(customersSorted, null, 2);

    assert(actual === expected);
    console.log('OK: Data Updated in all documents');
    // console.log(actual)
    // fs.writeFileSync('./actual.json',actual)
    // console.log(expected)
    // fs.writeFileSync('./expected.json',expected)
  } catch (error) {
    console.error(error);
  } finally {
    db.close();
    console.log('DB connection closed');
  }
}

updateAndTest();
