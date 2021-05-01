'use strict';
const { MongoClient } = require('mongodb');
const { MMQ, Worker} = require('./index');
const client = new MongoClient('mongodb://localhost:27017', { auth: { user: 'root', password: 'alicilin27'}, useNewUrlParser: true, useUnifiedTopology: true });
const client2 = new MongoClient('mongodb://localhost:27017', { auth: { user: 'root', password: 'alicilin27' }, useNewUrlParser: true, useUnifiedTopology: true });
const mmq1 = new MMQ({ client, servicename: 'auth', channel: 'test', dbname: 'connectter'});
const mmq2 = new MMQ({ client: client2, servicename: 'matching', channel: 'test', dbname: 'connectter'});
const mmq3 = new MMQ({ client, servicename: 'matching', channel: 'test', dbname: 'connectter' });

async function main() {
    (await mmq1.connect());
    (await mmq2.connect());
    (await mmq3.connect());
    
    // for (let i = 1; i < 1000; i++) {
    //     (await mmq1.send({ service: '*', event: 'worked', retry: 15, data: { message: 'okeyyyy - ' +i } }));
    // }

    // setTimeout(() => (mmq1.send({ service: '*', event: 'worked', retry: 15, data: { message: 'okeyyyy letsgo' } })), 3000);
    let worker = new Worker({ MMQI: mmq2, shift: true, maxWaitSeconds: 10 });
    worker.on('worked', async data => {
        // (await (new Promise(r => setTimeout(r, 1000))));
        console.log(data.data.message, 'worker 1');
    });

    let worker2 = new Worker({ MMQI: mmq3, shift: true, maxWaitSeconds: 10 });
    worker2.on('worked', data => {
        console.log(data.data.message, 'worker 2');
    });
    
    worker.start();
    worker2.start();
}

main()