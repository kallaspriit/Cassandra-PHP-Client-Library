<?php

$startTime = microtime(true);

// show all the errors
error_reporting(E_ALL);
session_start();

// the only file that needs including into your project
require_once 'Cassandra.php';
require_once 'CassandraModel.php';

class UserModel extends CassandraModel {
	public $email;
	public $name;
	public $age;
}

// list of seed servers to randomly connect to
// all the parameters are optional and default to given values
$servers = array(
	array(
		'host' => '127.0.0.1',
		'port' => 9160,
		'use-framed-transport' => true,
		'send-timeout-ms' => 1000,
		'receive-timeout-ms' => 1000
	)
);

// create a named singleton, the second parameter name defaults to "main"
// you can have several named singletons with different server pools
$cassandra = Cassandra::createInstance($servers);

/*
// drop the example keyspace and ignore errors should it not exist
try {
	$cassandra->dropKeyspace('CassandraRowsPaginationExample');
} catch (Exception $e) {}
*/

// set the default connection for the model
CassandraModel::setDefaultConnection($cassandra);

try {
	// start using the created keyspace
	$cassandra->useKeyspace('CassandraRowsPaginationExample');
} catch (Exception $e) {
	// create a new keyspace, accepts extra parameters for replication options
	// normally you don't do it every time
	$cassandra->createKeyspace('CassandraRowsPaginationExample');

	// use the keyspace again properly
	$cassandra->useKeyspace('CassandraRowsPaginationExample');

	// create a standard column family with given column metadata
	$cassandra->createStandardColumnFamily(
		'CassandraRowsPaginationExample', // keyspace name
		'user',             // the column-family name
		array(              // list of columns with metadata
			array(
				'name' => 'name',
				'type' => Cassandra::TYPE_UTF8,
				'index-type' => Cassandra::INDEX_KEYS, // create secondary index
				'index-name' => 'NameIdx'
			),
			array(
				'name' => 'email',
				'type' => Cassandra::TYPE_UTF8
			),
			array(
				'name' => 'age',
				'type' => Cassandra::TYPE_INTEGER,
				'index-type' => Cassandra::INDEX_KEYS,
				'index-name' => 'AgeIdx'
			)
		)
		// actually accepts more parameters with reasonable defaults
	);

	$columns = array(
		'name' => 'Columns test',
		'age' => 99,
		'email' => 'columns-test@cpcl.com',
	);

	// create some example data
	for ($i = 0; $i < 1000; $i++) {
		UserModel::insert('user-'.sprintf('%04d', $i), array(
			'name' => 'User #'.$i,
			'age' => round($i / 10),
			'email' => 'user-'.$i.'@cpcl.com',
		));

		$columns[] = 'Column #'.$i;
	}

	UserModel::insert('columns-test', $columns);
}

echo '<pre>';

/*
$firstUser = UserModel::load('user-0000');
print_r($firstUser->getData());
*/

$reverse = false;
$start = null;

if (isset($_GET['next'])) {
	$start = $_GET['next'];
} else if (isset($_GET['previous'])) {
	$reverse = true;

	if (isset($_SESSION['start-'.$_GET['previous']])) {
		$start = $_SESSION['start-'.$_GET['previous']];
	}
}

$data = UserModel::getKeyRange($start);

$count = 0;
$itemsPerPage = 10;
$prevExists = $start != null;
$nextExists = false;
$firstKey = null;
$lastKey = null;
$items = array();

foreach ($data as $key => $user) {
	if ($count == 0) {
		$firstKey = $key;
	}

	$lastKey = $key;

	if ($count == $itemsPerPage) {
		$nextExists = true;

		break;
	}

	$items[$key] = $user;

	$count++;
}

if (!$reverse) {
	$_SESSION['start-'.$lastKey] = $start;
}

foreach ($items as $key => $item) {
	echo $key.': '.print_r($item, true);
}


if ($prevExists) {
	echo '<a href="?previous='.$firstKey.'">Previous</a>';
} else {
	echo 'Previous';
}

echo ' | ';

if ($nextExists) {
	echo '<a href="?next='.$lastKey.'">Next</a>';
} else {
	echo 'Next';
}

echo '<p>It took '.round(microtime(true) - $startTime, 3).' seconds</p>';