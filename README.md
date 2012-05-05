Cassandra PHP Client Library
============================

![Cassandra PHP Client Library logo](http://dl.dropbox.com/u/8855759/cpcl/logo.png "CPCL logo")

Cassandra PHP Client Library or **CPCL** for short allows for **managing and querying your Cassandra cluster**. It's a **high-level library** performing all the rather complex low-level heavy lifting and provides a **simple to learn and use interface**.

Features
--------
- simple and intuitive interface
- well covered with unit tests (> 90%)
- support for multiple server pools using named singletons
- requires including a single file
- uses reasonable defaults through-out
- powerful syntax for querying data
- enables managing keyspaces and column-families
- automatic packing of datatypes using column metadata
- retries failed queries using back-off strategy
- built with performance in mind (caches schema description etc)
- well documented API and a working example

Class diagram
-------------
![CPCL class diagram](https://github.com/kallaspriit/Cassandra-PHP-Client-Library/blob/master/doc/class-diagram.png?raw=true "Cassandra PHP Client Library class diagram")

The library uses the very permissive [MIT licence](http://en.wikipedia.org/wiki/MIT_License) which means you can **do pretty much anything you like with it**.

Example
-------
The following example covers most of what you need to know to use this library. It should work out-of-box on a machine with default cassandra setup running. The example file is contained in the download.

[Click here](https://github.com/kallaspriit/Cassandra-PHP-Client-Library/blob/master/example.php "Cassandra PHP Client Library example code") for prettier code.

	<?php
	
	// show all the errors
	error_reporting(E_ALL);

	// the only file that needs including into your project
	require_once 'Cassandra.php';

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

	// at any point in code, you can get the named singleton instance, the name
	// again defaults to "main" so no need to define it if using single instance
	$cassandra2 = Cassandra::getInstance();

	// drop the example keyspace and ignore errors should it not exist
	try {
		$cassandra->dropKeyspace('CassandraExample');
	} catch (Exception $e) {}


	// create a new keyspace, accepts extra parameters for replication options
	// normally you don't do it every time
	$cassandra->createKeyspace('CassandraExample');

	// start using the created keyspace
	$cassandra->useKeyspace('CassandraExample');

	// if a request fails, it will be retried for this many times, each time backing
	// down for a bit longer period to prevent floods; defaults to 5
	$cassandra->setMaxCallRetries(5);

	// create a standard column family with given column metadata
	$cassandra->createStandardColumnFamily(
		'CassandraExample', // keyspace name
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

	// create a super column family
	$cassandra->createSuperColumnFamily(
		'CassandraExample',
		'cities',
		array(
			array(
				'name' => 'population',
				'type' => Cassandra::TYPE_INTEGER
			),
			array(
				'name' => 'comment',
				'type' => Cassandra::TYPE_UTF8
			)
		),
		// see the definition for these additional optional parameters
		Cassandra::TYPE_UTF8,
		Cassandra::TYPE_UTF8,
		Cassandra::TYPE_UTF8,
		'Capitals supercolumn test',
		1000,
		1000,
		0.5
	);

	// lets fetch and display the schema of created keyspace
	$schema = $cassandra->getKeyspaceSchema('CassandraExample');
	echo 'Schema: <pre>'.print_r($schema, true).'</pre><hr/>';
	/*
	// should we need to, we can access the low-level client directly
	$version = $cassandra->getConnection()->getClient()->describe_version();
	echo 'Version directly: <pre>'.print_r($version, true).'</pre><hr/>';
	*/
	// if implemented, use the wrapped methods as these are smarter - can retry etc
	$version = $cassandra->getVersion();
	echo 'Version through wrapper: <pre>'.print_r($version, true).'</pre><hr/>';

	// cluster is a pool of connections
	$cluster = $cassandra->getCluster();
	echo 'Cluster: <pre>'.print_r($cluster, true).'</pre><hr/>';

	// you can ask the cluster for a connection to a random seed server from pool
	$connection = $cluster->getConnection();
	echo 'Connection: <pre>'.print_r($connection, true).'</pre><hr/>';

	// access column family, using the singleton syntax
	// there is shorter "cf" methid that is an alias to "columnFamily"
	$userColumnFamily = Cassandra::getInstance()->columnFamily('user');
	echo 'Column family "user": <pre>'.print_r($userColumnFamily, true).'</pre><hr/>';

	// lets insert some test data using the convinience method "set" of Cassandra
	// the syntax is COLUMN_FAMILY_NAME.KEY_NAME
	$cassandra->set(
		'user.john',
		array(
			'email' => 'john@smith.com',
			'name' => 'John Smith',
			'age' => 34
		)
	);

	// when inserting data, it's ok if key name contains ".", no need to escape them
	$cassandra->set(
		'user.jane.doe',
		array(
			'email' => 'jane@doe.com',
			'name' => 'Jane Doe',
			'age' => 24
		)
	);

	// longer way of inserting data, first getting the column family
	$cassandra->cf('user')->set(
		'chuck', // key name
		array(   // column names and values
			'email' => 'chuck@norris.com',
			'name' => 'Chuck Norris',
			'age' => 24
		),
		Cassandra::CONSISTENCY_QUORUM // optional consistency to use
		// also accepts optional custom timestamp and time to live
	);

	// lets fetch all the information about user john
	$john = $cassandra->get('user.john');
	echo 'User "john": <pre>'.print_r($john, true).'</pre><hr/>';

	// since the jane key "jane.doe" includes a ".", we have to escape it
	$jane = $cassandra->get('user.'.Cassandra::escape('jane.doe'));
	echo 'User "jane.doe": <pre>'.print_r($jane, true).'</pre><hr/>';

	// there is some syntatic sugar on the query of Cassandra::get() allowing you
	// to fetch specific columns, ranges of them, limit amount etc. for example,
	// lets only fetch columns name and age
	$chuck = $cassandra->get('user.chuck:name,age');
	echo 'User "chuck", name and age: <pre>'.print_r($chuck, true).'</pre><hr/>';

	// fetch all solumns from age to name (gets all columns in-between too)
	$chuck2 = $cassandra->get('user.chuck:age-name');
	echo 'User "chuck", columns ago to name: <pre>'.print_r($chuck2, true).'</pre><hr/>';

	// the range columns do not need to exist, we can get character ranges
	$chuck3 = $cassandra->get('user.chuck:a-z');
	echo 'User "chuck", columns a-z: <pre>'.print_r($chuck3, true).'</pre><hr/>';

	// when performing range queries, we can also limit the number of columns
	// returned (2); also the method accepts consistency level as second parameter
	$chuck4 = $cassandra->get('user.chuck:a-z|2', Cassandra::CONSISTENCY_ALL);
	echo 'User "chuck", columns a-z, limited to 2 columns: <pre>'.print_r($chuck4, true).'</pre><hr/>';

	// the Cassandra::get() is a convinience method proxying to lower level
	// CassandraColumnFamily::get(), no need to worry about escaping with this.
	// column family has additional methods getAll(), getColumns(), getColumnRange()
	// that all map to lower level get() calls with more appopriate parameters
	$jane2 = $cassandra->cf('user')->get('jane.doe');
	echo 'User "jane.doe", lower level api: <pre>'.print_r($jane2, true).'</pre><hr/>';

	// we defined a secondary index on "age" column of "user" column family so we
	// can use CassandraColumnFamily::getWhere() to fetch users of specific age.
	// this returns an iterator that you can go over with foreach or use the
	// getAll() method that fetches all the data and returns an array
	$aged24 = $cassandra->cf('user')->getWhere(array('age' => 24));
	echo 'Users at age 24: <pre>'.print_r($aged24->getAll(), true).'</pre><hr/>';

	// if we know we are going to need to values of several keys, we can request
	// them in a single query for better performance
	$chuckAndJohn = $cassandra->cf('user')->getMultiple(array('chuck', 'john'));
	echo 'Users "chuck" and "john": <pre>'.print_r($chuckAndJohn, true).'</pre><hr/>';

	/* Uncomment this when using order preserving partitioner
	// we can fetch a range of keys but this is predictable only if using an
	// order preserving partitioner, Cassandra defaults to random one
	// again as there may be more results than it's reasonable to fetch in a single
	// query, an iterator is returned that can make several smaller range queries
	// as the data is iterated
	$usersAZ = $cassandra->cf('user')->getKeyRange('a', 'z');
	echo 'Users with keys in range a-z: <pre>'.print_r($usersAZ->getAll(), true).'</pre><hr/>';
	*/

	// find the number of columns a key has, we could also request for ranges
	$chuckColumnCount = $cassandra->cf('user')->getColumnCount('chuck');
	echo 'User "chuck" column count: <pre>'.print_r($chuckColumnCount, true).'</pre><hr/>';

	// we can find column counts for several keys at once
	$chuckJaneColumnCounts = $cassandra->cf('user')->getColumnCounts(array('chuck', 'jane.doe'));
	echo 'User "chuck" and "jane.doe" column counts: <pre>'.print_r($chuckJaneColumnCounts, true).'</pre><hr/>';

	// setting supercolumn values is similar to normal column families
	$cassandra->set(
		'cities.Estonia',
		array(
			'Tallinn' => array(
				'population' => '411980',
				'comment' => 'Capital of Estonia',
				'size' => 'big'
			),
			'Tartu' => array(
				'population' => '98589',
				'comment' => 'City of good thoughts',
				'size' => 'medium'
			)
		)
	);

	// fetch all columns of Tartu in Estonia of cities
	$tartu = $cassandra->cf('cities')->getAll('Estonia', 'Tartu');
	echo 'Super-column cities.Estonia.Tartu: <pre>'.print_r($tartu, true).'</pre><hr/>';

	// we could also use the higher level Cassandra::get() to fetch supercolumn info
	// we can still use the additional filters of columns
	$tallinn = $cassandra->get('cities.Estonia.Tallinn:population,size');
	echo 'Super-column cities.Estonia.Tallinn: <pre>'.print_r($tallinn, true).'</pre><hr/>';

	// you can delete all the data in a column family using "truncate"
	$cassandra->truncate('user');

	// you may choose to drop an entire keyspace
	$cassandra->dropKeyspace('CassandraExample');