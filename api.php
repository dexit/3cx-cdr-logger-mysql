<?php
echo 'Service starting...';

require_once 'savedconfig.php';

use chokidar\Chokidar;
use mysql\mysql;

echo 'Verifying connection to MySQL';

$workQueue = [
    'queueProcessing' => false,
    'queueItems' => []
];

$con = new mysql([
    'host' => $config['mysql']['host'],
    'user' => $config['mysql']['user'],
    'password' => $config['mysql']['password'],
    'database' => $config['mysql']['database']
]);

function connectMySQL() {
    global $config, $con;
    return new Promise(function ($resolve, $reject) use ($config, &$con) {
        $con = new mysql([
            'host' => $config['mysql']['host'],
            'user' => $config['mysql']['user'],
            'password' => $config['mysql']['password'],
            'database' => $config['mysql']['database']
        ]);
        $con->connect(function ($err) use ($resolve, $reject) {
            if ($err) {
                echo 'Error (MySQL-Connect): ' . $err->getMessage();
                $reject();
            }
            echo 'Connected to the MySQL server.';
            $resolve();
        });
    });
}

function checkFields() {
    global $config, $con;
    return new Promise(function ($resolve, $reject) use ($config, $con) {
        echo 'Checking field list.';
        $con->query('SHOW COLUMNS FROM `' . $config['mysql']['tablename'] . '`', function ($err, $results) use ($resolve, $reject, $config) {
            if ($err) {
                echo 'Error (MySQL): Could not get field list. ' . $err->getMessage();
                $reject();
            }

            $searchFields = array_filter($config['fields'], function ($dataField) {
                return $dataField['store'] === true;
            });
            $fieldsOkay = true;
            foreach ($searchFields as $dataField) {
                $foundField = false;
                foreach ($results as $element) {
                    if ($element['Field'] === $dataField['name']) {
                        echo 'Found field: ' . $dataField['name'];
                        $foundField = true;
                        break;
                    }
                }
                if (!$foundField) {
                    echo 'Missing field: ' . $dataField['name'];
                    $fieldsOkay = false;
                }
            }
            if ($fieldsOkay) {
                echo 'Fields are OK';
                $resolve();
            } else {
                $reject();
            }
        });
    });
}

function directoryExists() {
    global $config;
    return new Promise(function ($resolve, $reject) use ($config) {
        if (is_readable($config['watchdirectory']) && is_writable($config['watchdirectory'])) {
            echo 'Can read/write the watch folder';
            $resolve(true);
        } else {
            echo "Error: Can't read/write watch folder.";
            $reject(new Exception());
        }
    });
}

function endConnection() {
    global $con;
    echo 'Disconnecting';
    $con->end();
}

function startWatching() {
    global $config, $workQueue;
    $watcher = new Chokidar($config['watchdirectory']);
    $watcher->on('add', function ($path) use (&$workQueue) {
        echo 'File ' . $path . ' has been found.';
        echo 'Work queue is currently processing: ' . $workQueue['queueProcessing'];
        $workQueue['queueItems'][] = $path;
        if ($workQueue['queueProcessing'] === false) {
            $workQueue['queueProcessing'] = true;
            startQueue();
        }
    });
}

function startQueue() {
    global $workQueue, $con;
    connectMySQL()
        ->then(function () use (&$workQueue, $con) {
            foreach ($workQueue['queueItems'] as $queueItem) {
                try {
                    echo "Queue length (Pre): " . count($workQueue['queueItems']);
                    processCDRFile($queueItem);
                    $workQueue['queueItems'] = array_filter($workQueue['queueItems'], function ($e) use ($queueItem) {
                        return $e !== $queueItem;
                    });
                    echo "Queue length (Post): " . count($workQueue['queueItems']);
                    if (count($workQueue['queueItems']) === 0) {
                        endConnection();
                        $workQueue['queueProcessing'] = false;
                    }
                } catch (Exception $e) {
                    echo "An error occurred processing " . $queueItem;
                    $workQueue['queueItems'] = array_filter($workQueue['queueItems'], function ($e) use ($queueItem) {
                        return $e !== $queueItem;
                    });
                    echo "Queue length (Post): " . count($workQueue['queueItems']);
                    if (count($workQueue['queueItems']) === 0) {
                        endConnection();
                        $workQueue['queueProcessing'] = false;
                    }
                }
            }
        })
        ->catch(function () use ($con) {
            echo "Couldn't complete file.";
            endConnection();
        });
}

function processCDRFile($filePath) {
    global $con, $config;
    echo 'Processing file: ' . $filePath;
    return new Promise(function ($resolve, $reject) use ($filePath, $con, $config) {
        $data = file_get_contents($filePath);
        if ($data === false) {
            echo 'Error (FS): Unable to read file ' . $filePath;
            $reject($filePath);
            return;
        }
        $result = neatCsv($data, ['headers' => false]);
        $result = array_filter($result, function ($element) {
            return count($element) !== 0;
        });
        foreach ($result as $cdrRecord) {
            if (count($cdrRecord) !== count($config['fields'])) {
                echo "Error (CSV Parse): Number of fields in CDR record (" . count($cdrRecord) . ") does not match number of fields in config (" . count($config['fields']) . ").";
                $reject($filePath);
            } else {
                echo 'Creating MySQL query';
                $fieldSet = '';
                $valuePlaceholder = '';
                $valueSet = [];
                foreach ($cdrRecord as $key => $value) {
                    if ($config['fields'][$key]['store'] === true) {
                        if ($fieldSet !== '') {
                            $fieldSet .= ', ';
                            $valuePlaceholder .= ', ';
                        }
                        $fieldSet .= '`' . $config['fields'][$key]['name'] . '`';
                        $valuePlaceholder .= '?';
                        $valueSet[] = $value;
                    }
                }
                $con->query('INSERT INTO ' . $config['mysql']['tablename'] . ' (' . $fieldSet . ') VALUES (' . $valuePlaceholder . ')', $valueSet, function ($err, $results) use ($filePath, $con, $config, $resolve, $reject) {
                    if ($err) {
                        echo 'Error (MySQL): Could not insert CDR row ' . $err->getMessage();
                        $reject($filePath);
                    }
                    if ($results['affectedRows'] === 1) {
                        echo 'Successfully added one CDR row';
                        unlink($filePath, function ($err) use ($filePath, $resolve) {
                            if ($err) {
                                echo 'Error (FS): Could not delete file ' . $filePath . $err;
                                echo $err;
                                $reject($filePath);
                            } else {
                                echo "Deleted file: " . $filePath;
                                $resolve();
                            }
                        });
                    } else {
                        echo 'Error (MySQL): There was an error adding the CDR row';
                        echo $result;
                        $reject($filePath);
                    }
                });
            }
        }
    });
}

connectMySQL()
    ->then('checkFields')
    ->then('directoryExists')
    ->catch(function () {
        endConnection();
        echo "Errors detected in the pre-flight, won't go any further.";
        exit(1);
    })
    ->then(function () {
        echo 'Preflight was successful!';
        endConnection();
        startWatching();
        echo 'Now watching folder ' . $config['watchdirectory'];
    });

$ip_asterisk = $config['3cx']['ip'];
$usuario = $config['3cx']['username'];
$senha = $config['3cx']['password'];
$canal_saida = $config['3cx']['channel'];
$espera = $config['3cx']['waittime'];
$cidorigem = $config['3cx']['callerid'];
$destino = $config['3cx']['destination'];
$contexto = $config['3cx']['context'];
$codigoaccount = $config['3cx']['accountcode'];

$oSocket = fsockopen($ip_asterisk, 5038, $errnum, $errdesc) or die("Crash! Pow! Thudd!");
sleep(1); 
fputs($oSocket, "Action: login\r\n"); 
fputs($oSocket, "Username: $usuario\r\n"); 
fputs($oSocket, "Secret: $senha\r\n\r\n"); 
fputs($oSocket, "Events: off\r\n\r\n");
fputs($oSocket, "Action: originate\r\n");
fputs($oSocket, "Channel: $canal_saida\r\n");
fputs($oSocket, "WaitTime: $espera\r\n");
fputs($oSocket, "CallerId: Web $cidorigem\r\n"); 
fputs($oSocket, "Exten: $destino\r\n");
fputs($oSocket, "Context: $contexto\r\n");
fputs($oSocket, "Async: true\r\n");
// Setting the accountcode CDR field: 
fputs($oSocket, "Account: $codigoaccount\r\n");
fputs($oSocket, "Priority: 1\r\n\r\n"); 
fputs($oSocket, "Action: Logoff\r\n\r\n");
fclose($oSocket);
?>
