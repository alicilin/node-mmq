# Mongodb Message Queue System

    'use strict';
	const { MongoClient } = require('mongodb');
	const { MMQ, Worker} = require('@alicilin/node-mmq');
	const  client = new  MongoClient('mongodb://localhost:27017', { useNewUrlParser:  true, useUnifiedTopology:  true });
	const  mmq1 = new  MMQ({ client, servicename:  'auth', channel:  'test'});
	const  mmq2 = new  MMQ({ client, servicename:  'matching', channel:  'test'});
	
	async  function  main() {
		(await  mmq1.connect());
		(await  mmq2.connect());
		for (let  i = 1; i < 20; i++) {
			(await  mmq1.send({ service:  '*', event:  'worked', retry:  15, data: { message:  'okeyyyy' } }));
		}
		
		let  worker = new  Worker({ MMQI: mmqi });
		worker.on('worked', data  => {
			console.log(data);
		});
	
		worker.start();
	}
	
	main()
```