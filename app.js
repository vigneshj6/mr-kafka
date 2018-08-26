const path = require('path');
const express = require('express')
const app = express();
const expressWs = require('express-ws')(app);
const bodyParser = require('body-parser');
const pty = require('node-pty')
const fs = require('fs');
const handlebars = require('handlebars')


const lowdb = require('lowdb');
const lowdbFileSync = require('lowdb/adapters/FileSync');
const lowdbAdapter = new lowdbFileSync(path.join(__dirname, 'db.json'))
const db = lowdb(lowdbAdapter)

const SERVER_PORT = 8080;

db.defaults({
    conf: { kafkaPath: "" },
    zookeeper: [],
    broker: [],
    topic: []
}).write();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.use('/node_modules/xterm', express.static(path.join(__dirname, '/node_modules/xterm')));

const terminals = {}
const logs = {}
const webSocks = {}

app.get('/console-main.js', (req, res) => res.sendFile(path.join(__dirname, 'console-main.js')));

app.get('/conf', function (req, res) {
    res.send(db.get('conf').value());
});

app.post('/conf', function (req, res) {
    if (req.body.kafkaPath != undefined)
        db.set('conf.kafkaPath', req.body.kafkaPath).write();
    res.status(200).end();
})

app.get('/zookeeper', function (req, res) {
    res.send(db.get('zookeeper').value());
});

app.post('/zookeeper', function (req, res) {
    const label = req.body.label;
    const propFile = req.body.propFile;

    if (label === undefined || propFile == undefined)
        res.status(400).end();

    const obj = {
        label: label,
        propFile: propFile,
        pid: 0
    };

    const query = db.get('zookeeper').find({ label: label });
    if (query.value() === undefined) {
        db.get('zookeeper').push(obj).write();
        res.status(200).end();
    }
    else {
        res.status(400).send('Resource already exists.');

    }
});

app.put('/zookeeper', function (req, res) {
    const label = req.body.label;
    const propFile = req.body.propFile;

    if (label === undefined || propFile == undefined)
        res.status(400).end();

    const obj = {
        label: label,
        propFile: propFile
    };

    const query = db.get('zookeeper').find({ label: label });
    if (query.value() != undefined) {
        query.assign(obj).write();
        res.status(200).end();
    }
    else {
        res.status(400).send('Resource does not exists.');
    }
});

app.delete('/zookeeper/:label', function (req, res) {
    const label = req.params.label;

    if (label == undefined) {
        res.status(400).end();
    }

    const query = db.get('zookeeper').find({ label: label });
    if (query.value() != undefined) {
        db.get('zookeeper').remove({ label: label }).write();
        res.status(200).end();
    }
    else {
        res.status(400).send('Resource does not exists.');

    }
});

app.get('/zookeeper/start/:label', function (req, res) {
    const label = req.params.label;

    if (label == undefined) {
        res.status(400).end();
    }

    const zk = db.get('zookeeper').find({ label: label }).value();
    if (zk === undefined) {
        res.status(400).send('Resource does not exists.');
    }

    const kafkaPath = db.get('conf.kafkaPath').value();
    if (kafkaPath == "") {
        res.status(400).send('KafkaPath is not set.');
    }

    const propFile = zk.propFile;
    const scriptPath = path.join(kafkaPath, 'bin/zookeeper-server-start.sh');

    const term = pty.spawn('bash', [scriptPath, propFile], {
        name: 'xterm-' + zk.label,
        cols: 150,
        rows: 30,
        cwd: process.env.PWD,
        env: process.env
    });

    console.log('Created terminal with PID: ' + term.pid);

    terminals[term.pid] = term;
    logs[term.pid] = '';
    webSocks[term.pid] = [];

    term.on('data', function (data) {
        logs[term.pid] += data;
        const socks = webSocks[term.pid];
        if(socks != undefined) {       
            socks.forEach(ws => {
                try {
                    ws.send(data);
                } catch (ex) {
                    // The WebSocket is not open, ignore
                }
            });
        }
    });

    zk['pid'] = term.pid;
    db.get('zookeeper').find({label: label}).assign(zk).write();

    response = {
        pid: term.pid.toString(),
        url: 'http://localhost:' + SERVER_PORT + '/console/' + term.pid.toString()
    };
    res.send(response);
});

app.get('/zookeeper/stop/:label', function (req, res) {
    const label = req.params.label;

    if (label == undefined) {
        res.status(400).end();
    }

    const zk = db.get('zookeeper').find({ label: label }).value();
    if (zk === undefined) {
        res.status(400).send('Resource does not exists.');
    }
    
    const term = terminals[zk.pid]
    term.kill();
    console.log('Closed terminal ' + term.pid);
    delete terminals[term.pid];
    delete logs[term.pid];
    delete webSocks[term.pid];

    zk['pid'] = '';
    db.get('zookeeper').find({label: label}).assign(zk).write();

    res.status(200).end();
});

app.get('/broker', function (req, res) {
    res.send(db.get('broker').value());
});

app.post('/broker', function (req, res) {
    const label = req.body.label;
    const propFile = req.body.propFile;

    if (label === undefined || propFile == undefined)
        res.status(400).end();

    const obj = {
        label: label,
        propFile: propFile,
        pid: 0
    };

    const query = db.get('broker').find({ label: label });
    if (query.value() === undefined) {
        db.get('broker').push(obj).write();
        res.status(200).end();
    }
    else {
        res.status(400).send('Resource already exists.');

    }
});

app.put('/broker', function (req, res) {
    const label = req.body.label;
    const propFile = req.body.propFile;

    if (label === undefined || propFile == undefined)
        res.status(400).end();

    const obj = {
        label: label,
        propFile: propFile
    };

    const query = db.get('broker').find({ label: label });
    if (query.value() != undefined) {
        query.assign(obj).write();
        res.status(200).end();
    }
    else {
        res.status(400).send('Resource does not exists.');
    }
});

app.delete('/broker/:label', function (req, res) {
    const label = req.params.label;

    if (label == undefined) {
        res.status(400).end();
    }

    const query = db.get('broker').find({ label: label });
    if (query.value() != undefined) {
        db.get('broker').remove({ label: label }).write();
        res.status(200).end();
    }
    else {
        res.status(400).send('Resource does not exists.');

    }
});

app.get('/broker/start/:label', function (req, res) {
    const label = req.params.label;

    if (label == undefined) {
        res.status(400).end();
    }

    const bk = db.get('broker').find({ label: label }).value();
    if (bk === undefined) {
        res.status(400).send('Resource does not exists.');
    }

    const kafkaPath = db.get('conf.kafkaPath').value();
    if (kafkaPath == "") {
        res.status(400).send('KafkaPath is not set.');
    }

    const propFile = bk.propFile;
    const scriptPath = path.join(kafkaPath, 'bin/kafka-server-start.sh');

    const term = pty.spawn('bash', [scriptPath, propFile], {
        name: 'xterm-' + bk.label,
        cols: 150,
        rows: 30,
        cwd: process.env.PWD,
        env: process.env
    });

    console.log('Created terminal with PID: ' + term.pid);

    terminals[term.pid] = term;
    logs[term.pid] = '';
    webSocks[term.pid] = [];

    term.on('data', function (data) {
        logs[term.pid] += data;
        const socks = webSocks[term.pid];
        if(socks != undefined) {       
            socks.forEach(ws => {
                try {
                    ws.send(data);
                } catch (ex) {
                    // The WebSocket is not open, ignore
                }
            });
        }
    });

    bk['pid'] = term.pid;
    db.get('broker').find({label: label}).assign(bk).write();
    
    response = {
        pid: term.pid.toString(),
        url: 'http://localhost:' + SERVER_PORT + '/console/' + term.pid.toString()
    };
    res.send(response);
});

app.get('/broker/stop/:label', function (req, res) {
    const label = req.params.label;

    if (label == undefined) {
        res.status(400).end();
    }

    const bk = db.get('broker').find({ label: label }).value();
    if (bk === undefined) {
        res.status(400).send('Resource does not exists.');
    }
    
    const term = terminals[bk.pid]
    
    try {
        term.kill();
        console.log('Closed terminal ' + term.pid);
    } catch(ex) {
        //Process does not exists, ignore
    }

    delete terminals[term.pid];
    delete logs[term.pid];
    delete webSocks[term.pid];

    bk['pid'] = '';
    db.get('broker').find({label: label}).assign(bk).write();

    res.status(200).end();
});

app.ws('/console/:pid', function (ws, req) {
    if (req.params.pid == undefined) {
        res.status(400).end();
    }
    const pid = parseInt(req.params.pid);

    const socks = webSocks[pid]
    socks.push(ws);
    webSocks[pid] = socks;

    ws.send(logs[pid]);
    
    const term = terminals[pid];
    ws.on('message', function (msg) {
        term.write(msg);
    });
});

app.get('/console/:pid', function(req, res){
    if(req.params.pid == undefined)
        res.status(400).end();

    const pid = parseInt(req.params.pid);
    var result = db.get('zookeeper').find({ pid: pid }).value()
              || db.get('broker').find({ pid: pid }).value();

    if(result == undefined)
        res.status(404).end();

    const hbSrc = fs.readFileSync(path.join(__dirname, 'console.handlebars')).toString();
    const hbtmp = handlebars.compile(hbSrc);
    res.send(hbtmp(result));
}); 

app.listen(SERVER_PORT, () => console.log('Server is listening in on port ' + SERVER_PORT.toString()))