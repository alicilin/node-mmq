'use strict';
const Daemon = require('./Daemon');
const MMQ = require('./MMQ');
const Worker = require('./Worker');
// const { MMQ, Worker, Daemon} = require('./index');
let moptions = [
    'mongodb://localhost:27017', 
    { 
        auth: { 
            user: 'root', 
            password: 'alicilin27' 
        }, 
        useNewUrlParser: true, 
        useUnifiedTopology: true 
    }
];

let roptions = [
    { 
        host: 'localhost', 
        port: 6379,
        key: 'mrqueue' 
    }
];

const server = new Daemon({ mongo: moptions, redis: roptions, ip: 'localhost', port: 8080, secret: 'mmq-123', transport: 'websocket' });
const mmq1 = new MMQ({ servicename: 'master', channel: 'test', ip: 'localhost', port: 8080, secret: 'mmq-123', transport: 'websocket' });
const mmq2 = new MMQ({ servicename: 'child', channel: 'test', ip: 'localhost', port: 8080, secret: 'mmq-123', transport: 'websocket' });

async function main() {
    (await server.migrations());
    (await server.start());
    
    // for (let i = 1; i < 1000; i++) {
    //     (await mmq1.send({ service: '*', event: 'worked', retry: 15, data: { message: 'okeyyyy - ' +i } }));
    // }

    setTimeout(() => (mmq1.send({ service: '^chil', event: 'worked', retry: 15, data: { message: 'okeyyyy letsgo' } })), 3 * 1000);
    let worker = new Worker({ MMQI: mmq2 });
    worker.on('worked', async data => {
        // (await (new Promise(r => setTimeout(r, 1000))));
        console.log(data.data.message, 'worker 1');
    });
    
    worker.start();
}

main()