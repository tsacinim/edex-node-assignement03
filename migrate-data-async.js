const mongodb = require('mongodb');
const async = require('async');
const util = require('util');
const path = require('path');
const url = 'mongodb://localhost:27017/edx-course-db';
const pathCustomers = path.join(__dirname, 'm3-customer-data.json');
const customers = require(pathCustomers);
const pathAddresses = path.join(__dirname, 'm3-customer-address-data.json');
const customerAddresses = require(pathAddresses);

let tasks = [];
const chunkSize = parseInt(process.argv[2], 10) || 1000;

async function updateDB() {
  const db = await mongodb.MongoClient.connect(url);
  console.log('DB connection opened');
  try {
    const collection = db.collection('customers');

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
  } catch (error) {
    console.error(error);
  } finally {
    db.close();
    console.log('DB connection closed');
  }
}

updateDB();
