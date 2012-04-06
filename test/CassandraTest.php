<?php

require_once '../Cassandra.php';

class CassandraTest extends PHPUnit_Framework_TestCase {

	protected $servers;

	/**
	 * @var Cassandra
	 */
	protected $cassandra;

	protected static $setupComplete = false;

	public function setup() {
		if (function_exists('apc_clear_cache')) {
			apc_clear_cache();
		}

		$this->servers = array(
			array(
				'host' => '127.0.0.1',
				'port' => 9160
			)
		);

		$this->cassandra = Cassandra::createInstance($this->servers);

		if (!self::$setupComplete) {
			try {
				$this->cassandra->dropKeyspace('CassandraTest');
			} catch (Exception $e) {}

			$this->cassandra->setMaxCallRetries(5);
			$this->cassandra->createKeyspace('CassandraTest');
			$this->cassandra->useKeyspace('CassandraTest');

			$this->cassandra->createStandardColumnFamily(
				'CassandraTest',
				'user',
				array(
					array(
						'name' => 'name',
						'type' => Cassandra::TYPE_UTF8,
						'index-type' => Cassandra::INDEX_KEYS,
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
			);

			$this->cassandra->createSuperColumnFamily(
				'CassandraTest',
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
				Cassandra::TYPE_UTF8,
				Cassandra::TYPE_UTF8,
				Cassandra::TYPE_UTF8,
				'Capitals supercolumn test',
				1000,
				1000,
				0.5
			);

			self::$setupComplete = true;
		} else {
			$this->cassandra->useKeyspace('CassandraTest');
		}
	}

	public function tearDown() {
		unset($this->cassandra);

		if (function_exists('apc_clear_cache')) {
			apc_clear_cache();
		}
	}

	public function testKeyspaceCanBeUpdated() {
		try {
			$this->cassandra->dropKeyspace('CassandraTest2');
		} catch (Exception $e) {}

		$this->cassandra->createKeyspace('CassandraTest2');

		$this->assertEquals(array(
			'column-families' => array(),
			'name' => 'CassandraTest2',
			'placement-strategy' => 'org.apache.cassandra.locator.SimpleStrategy',
			'placement-strategy-options' => array('replication_factor' => 1),
			'replication-factor' => 1,
		), $this->cassandra->getKeyspaceSchema('CassandraTest2', false));

		$this->cassandra->updateKeyspace(
			'CassandraTest2',
			1,
			Cassandra::PLACEMENT_NETWORK,
			array('DC1' => 2, 'DC2' => 2)
		);

		$this->assertEquals(array(
			'column-families' => array(),
			'name' => 'CassandraTest2',
			'placement-strategy' => 'org.apache.cassandra.locator.NetworkTopologyStrategy',
			'placement-strategy-options' => array(
				'DC2' => 2,
				'DC1' => 2
			),
			'replication-factor' => null
		), $this->cassandra->getKeyspaceSchema('CassandraTest2', false));
	}

	/**
	 * @expectedException CassandraColumnFamilyNotFoundException
	 */
	public function testExceptionThrownOnGetUnexistingSchema() {
		$cf = new CassandraColumnFamily($this->cassandra, 'foobar');
		$cf->getSchema();
	}

	/**
	 * @expectedException CassandraConnectionClosedException
	 */
	public function testGetClientThrowsExceptionIfConnectionClosed() {
		$connection = $this->cassandra->getConnection();
		$client = $connection->getClient();

		if (!($client instanceof cassandra_CassandraClient)) {
			$this->fail('Instance of CassandraClient expected');
		}

		$this->cassandra->closeConnections();

		$connection->getClient();
	}

	/**
	 * @expectedException CassandraUnsupportedException
	 */
	public function testExceptionThrownUsingIndexesOnSuperColumns() {
		$this->cassandra->createSuperColumnFamily(
			'CassandraTest',
			'cities2',
			array(
				array(
					'name' => 'population',
					'type' => Cassandra::TYPE_INTEGER,
					'index-type' => Cassandra::INDEX_KEYS,
					'index-name' => 'PopulationIdx'
				),
				array(
					'name' => 'comment',
					'type' => Cassandra::TYPE_UTF8
				)
			),
			Cassandra::TYPE_UTF8,
			Cassandra::TYPE_UTF8,
			Cassandra::TYPE_UTF8,
			'Capitals supercolumn test',
			1000,
			1000,
			0.5
		);
	}

	/**
	 * @expectedException CassandraInvalidRequestException
	 */
	public function testExceptionThrownOnInvalidColumnsRequest() {
		$this->cassandra->cf('user')->getWhere(
			array('age' => 20),
			array('name', 'email'),
			'name',
			'email'
		);
	}

	/**
	 * @expectedException CassandraInvalidRequestException
	 */
	public function testExceptionThrownOnInvalidColumnsRequest2() {
		$this->cassandra->cf('user')->getWhere(
			array(array('age', -1, 20))
		);
	}

	/**
	 * @expectedException CassandraInvalidRequestException
	 */
	public function testExceptionThrownOnInvalidColumnsRequest3() {
		$this->cassandra->cf('user')->getMultiple(
			array('user1', 'user2'),
			array('name', 'email'),
			'name',
			'email'
		);
	}

	/**
	 * @expectedException CassandraInvalidRequestException
	 */
	public function testExceptionThrownOnInvalidColumnsRequest4() {
		$this->cassandra->cf('user')->getKeyRange(
			'user1',
			'userN',
			100,
			array('name', 'email'),
			'name',
			'email'
		);
	}

	/**
	 * @expectedException CassandraInvalidRequestException
	 */
	public function testExceptionThrownOnInvalidColumnsRequest5() {
		$this->cassandra->cf('user')->getColumnCount(
			'sheldon',
			array('name', 'email'),
			'name',
			'email'
		);
	}

	/**
	 * @expectedException CassandraInvalidRequestException
	 */
	public function testExceptionThrownOnInvalidColumnsRequest6() {
		$this->cassandra->cf('user')->getColumnCounts(
			array('sheldon', 'john'),
			array('name', 'email'),
			'name',
			'email'
		);
	}

	public function testFramedOrBufferedTransportIsAvailable() {
		$framed = new CassandraConnection('127.0.0.1', 9160, true, 1000, 1000);
		$buffered = new CassandraConnection('127.0.0.1', 9160, false);

		if (!($framed->getTransport() instanceof TFramedTransport)) {
			$this->fail('Expected framed transport');
		}

		if (!($buffered->getTransport() instanceof TBufferedTransport)) {
			$this->fail('Expected buffered transport');
		}
	}

	public function testCanAuthenticateToKeyspace() {
		$this->cassandra->useKeyspace('CassandraTest', 'admin', 'qwerty');
		$this->cassandra->getKeyspaceSchema();
	}

	/**
	 * @expectedException CassandraSettingKeyspaceFailedException
	 */
	public function testExceptionThrownIfUnabletToSelectKeyspace() {
		$this->cassandra->useKeyspace('foobar');
		$this->cassandra->getKeyspaceSchema();
	}

	public function testClusterKnowsActiveKeyspace() {
		$this->assertEquals(
			$this->cassandra->getCluster()->getCurrentKeyspace(),
			'CassandraTest'
		);
	}

	public function testClusterKnowsServerList() {
		$servers = array(
			array(
				'host' => '127.0.0.1',
				'port' => 9160,
				'use-framed-transport' => true,
				'send-timeout-ms' => null,
				'receive-timeout-ms' => null,
			)
		);

		$this->assertEquals(
			$this->cassandra->getCluster()->getServers(),
			$servers
		);
	}

	public function testReturnsServerVersion() {
		$this->assertNotEmpty($this->cassandra->getVersion());
	}

	/**
	 * @expectedException CassandraConnectionFailedException
	 */
	public function testClusterGetConnectionThrowsExceptionIfNoServersAdded() {
		$cluster = new CassandraCluster();

		$cluster->getConnection();
	}

	public function testClosedConnectionsAreRemovedFromCluster() {
		$connection = $this->cassandra->getConnection();

		$connection->close();

		$this->cassandra->getConnection();
	}

	/**
	 * @expectedException CassandraConnectionFailedException
	 */
	public function testExceptionIsThrownIfConnectionFails() {
		$servers = array(
			array(
				'host' => '127.0.0.1',
				'port' => 9161
			)
		);

		$cassandra = Cassandra::createInstance($servers, 'other');
		$cassandra->getConnection();
	}

	public function testGetInstanceReturnsInstanceByName() {
		$this->assertEquals(
			Cassandra::getInstance(),
			$this->cassandra
		);

		$this->assertEquals(
			Cassandra::getInstance('main'),
			$this->cassandra
		);
	}

	/**
	 * @expectedException CassandraInvalidRequestException
	 */
	public function testGetInstanceThrowsExceptionIfNameNotFound() {
		Cassandra::getInstance('foobar');
	}

	public function testUseKeyspaceCanSkipAuthOnSecondCall() {
		$this->cassandra->useKeyspace('CassandraTest', 'admin', 'qwerty');
		$this->cassandra->useKeyspace('CassandraTest');
	}

	public function testMaxCallRetriesCanBeSet() {
		$this->cassandra->setMaxCallRetries(3);
		$this->cassandra->setDefaultColumnCount(50);

		try {
			$this->cassandra->call('foobar');
		} catch (Exception $e) {
			$this->assertEquals(
				$e->getMessage(),
				'Failed calling "foobar" the maximum of 3 times'
			);
		}
	}

	/**
	 * @expectedException CassandraInvalidRequestException
	 */
	public function testExceptionIsThrownIfKeyspaceNotSetOnCall() {
		$cassandra = Cassandra::createInstance($this->servers, 'other');

		$cassandra->call('get');
	}

	public function testReturnsKeyspaceDescription() {
		$info = $this->cassandra->describeKeyspace();

		$this->assertEquals(
			$info->name,
			'CassandraTest'
		);
	}

	public function testReturnsKeyspaceSchema() {
		$info = $this->cassandra->getKeyspaceSchema('CassandraTest', true);
		$this->cassandra->getKeyspaceSchema(); // coverage

		$this->assertEquals(
			$info['column-families']['user']['name'],
			'user'
		);
	}

	public function testEscapesSpecialTokens() {
		$this->assertEquals(
			Cassandra::escape('./:/,/-/|'),
			'\\./\\:/\\,/\\-/\\|'
		);
	}

	public function testStoresAndFetchesStandardColumns() {
		$this->cassandra->set(
			'user.foobar',
			array(
				'email' => 'foobar@gmail.com',
				'name' => 'John Smith',
				'age' => 23
			)
		);

		$this->cassandra->set(
			'user.'.'foo.bar',
			array(
				'email' => 'other@gmail.com',
				'name' => 'Jane Doe',
				'age' => 18
			)
		);

		$this->cassandra->set(
			'user.'.'foo.bar',
			array(
				'email' => 'other@gmail.com',
				'name' => 'Jane Doe',
				'age' => 18
			)
		);

		$this->assertEquals(
			array(
				'email' => 'foobar@gmail.com',
				'name' => 'John Smith',
				'age' => 23
			),
			$this->cassandra->get('user.foobar')
		);

		$this->assertEquals(
			array(
				'email' => 'other@gmail.com',
				'name' => 'Jane Doe',
				'age' => 18
			),
			$this->cassandra->get('user.'.Cassandra::escape('foo.bar'))
		);

		$this->assertEquals(
			array(
				'email' => 'foobar@gmail.com'
			),
			$this->cassandra->get('user.foobar:email')
		);

		$this->assertEquals(
			array(
				'email' => 'foobar@gmail.com',
				'age' => 23
			),
			$this->cassandra->get('user.foobar:email,age')
		);

		$this->assertEquals(
			array(
				'email' => 'foobar@gmail.com',
				'age' => 23
			),
			$this->cassandra->get('user.foobar:email, age')
		);

		$this->assertEquals(
			array(
				'email' => 'foobar@gmail.com',
				'name' => 'John Smith',
				'age' => 23
			),
			$this->cassandra->get('user.foobar:age-name')
		);

		$this->assertEquals(
			array(
				'email' => 'foobar@gmail.com',
				'name' => 'John Smith',
				'age' => 23
			),
			$this->cassandra->get('user.foobar:a-o')
		);

		$this->assertEquals(
			array(
				'email' => 'foobar@gmail.com',
				'age' => 23
			),
			$this->cassandra->get('user.foobar:a-f')
		);

		$this->assertEquals(
			array(
				'email' => 'foobar@gmail.com',
				'age' => 23
			),
			$this->cassandra->get('user.foobar:a-z|2')
		);


		$this->assertEquals(
			array(
				'age' => 23,
				'email' => 'foobar@gmail.com'
			),
			$this->cassandra->get('user.foobar|2')
		);

		$this->assertEquals(
			array(
				'name' => 'John Smith',
				'email' => 'foobar@gmail.com',
			),
			$this->cassandra->get('user.foobar:z-a|2R')
		);

		$this->assertEquals(
			array(
				'name' => 'John Smith',
				'email' => 'foobar@gmail.com',
			),
			$this->cassandra->get('user.foobar|2R')
		);
	}

	public function testRemovesKeys() {
		$this->cassandra->set(
			'user.removable',
			array(
				'email' => 'john@smith.com',
				'name' => 'John Smith',
				'age' => 44
			)
		);

		$this->assertEquals(
			array(
				'email' => 'john@smith.com',
				'name' => 'John Smith',
				'age' => 44
			),
			$this->cassandra->get('user.removable')
		);

		$this->cassandra->remove('user.removable');

		$this->assertNull($this->cassandra->get('user.removable'));
	}

	public function testRemovesKeys2() {
		$this->cassandra->set(
			'user.removable',
			array(
				'email' => 'john@smith.com',
				'name' => 'John Smith',
				'age' => 44
			)
		);

		$this->assertEquals(
			array(
				'email' => 'john@smith.com',
				'name' => 'John Smith',
				'age' => 44
			),
			$this->cassandra->get('user.removable')
		);

		$this->cassandra->cf('user')->remove('removable');

		$this->assertNull($this->cassandra->get('user.removable'));
	}

	public function testRemovesColumn() {
		$this->cassandra->set(
			'user.removable',
			array(
				'email' => 'john@smith.com',
				'name' => 'John Smith',
				'age' => 44
			)
		);

		$this->assertEquals(
			array(
				'email' => 'john@smith.com',
				'name' => 'John Smith',
				'age' => 44
			),
			$this->cassandra->get('user.removable')
		);

		$this->cassandra->remove('user.removable:email');

		$this->assertEquals(
			array(
				'name' => 'John Smith',
				'age' => 44
			),
			$this->cassandra->get('user.removable')
		);
	}

	public function testSpecialCharactersAreProperlyUnpacked() {
		$this->cassandra->set(
			'user.special',
			array(
				'email' => 'Jèna@grùber.com',
				'name' => 'Jèna Grùber',
				'age' => 41
			)
		);

		$this->assertEquals(
			array(
				'email' => 'Jèna@grùber.com',
				'name' => 'Jèna Grùber',
				'age' => 41
			),
			$this->cassandra->get('user.special')
		);
	}

	/**
	 * @expectedException CassandraInvalidPatternException
	 */
	public function testInvalidGetPatternThrowsException() {
		$this->cassandra->get('foo#bar');
	}

	/**
	 * @expectedException CassandraInvalidPatternException
	 */
	public function testInvalidGetPatternThrowsException2() {
		$this->cassandra->get('user.foobar:a-f-z');
	}

	/**
	 * @expectedException CassandraInvalidPatternException
	 */
	public function testSetWithoutColumnFamilyThrowsException() {
		$this->cassandra->set('foo', array('foo' => 'bar'));
	}

	/**
	 * @expectedException CassandraInvalidRequestException
	 */
	public function testGetWithColumnsAndColumnRangeThrowsException() {
		$this->cassandra->cf('user')->get('test', array('col1', 'col2'), 'start');
	}

	public function testGettingNonexistingKeyReturnsNull() {
		$this->assertNull($this->cassandra->get('user.x'));
	}

	public function testGetAllReturnsAllColumns() {
		$this->assertEquals(
			array(
				'email' => 'foobar@gmail.com',
				'name' => 'John Smith',
				'age' => 23
			),
			$this->cassandra->cf('user')->getAll('foobar')
		);

		$this->cassandra->set(
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

		$this->assertEquals(
			array(
				'population' => '98589',
				'comment' => 'City of good thoughts',
				'size' => 'medium'
			),
			$this->cassandra->cf('cities')->getAll('Estonia', 'Tartu')
		);

		$this->assertEquals(
			array(
				'email' => 'foobar@gmail.com',
				'name' => 'John Smith'
			),
			$this->cassandra->cf('user')->getColumns('foobar', array('email', 'name'))
		);

		$this->assertEquals(
			array(
				'age' => 23,
				'email' => 'foobar@gmail.com',
			),
			$this->cassandra->cf('user')->getColumnRange('foobar', 'age', 'email')
		);
	}

	public function testStoresAndFetchesSuperColumns() {
		$this->cassandra->set(
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

		$this->assertEquals(
			array(
				'population' => '98589',
				'comment' => 'City of good thoughts',
				'size' => 'medium'
			),
			$this->cassandra->get('cities.Estonia.Tartu')
		);

		$this->assertEquals(
			array(
				'population' => '98589',
				'size' => 'medium'
			),
			$this->cassandra->get('cities.Estonia.Tartu:population,size')
		);

		$this->assertEquals(
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
			),
			$this->cassandra->get('cities.Estonia[]')
		);
	}

	public function testDataCanBeRequestedUsingIndexes() {
		$this->cassandra->set(
			'user.john',
			array(
				'email' => 'john.smith@gmail.com',
				'name' => 'John Smith',
				'age' => 34
			)
		);

		$this->cassandra->set(
			'user.jane',
			array(
				'email' => 'jane.doe@gmail.com',
				'name' => 'Jane Doe',
				'age' => 24
			)
		);

		$this->cassandra->set(
			'user.chuck',
			array(
				'email' => 'chuck.norris@gmail.com',
				'name' => 'Chuck Norris',
				'age' => 34
			)
		);

		$this->cassandra->set(
			'user.sheldon',
			array(
				'email' => 'sheldon@cooper.com',
				'name' => 'Sheldon Cooper',
				'age' => 34
			)
		);

		$this->assertEquals(
			array(
				'chuck' => array(
					'email' => 'chuck.norris@gmail.com',
					'name' => 'Chuck Norris',
					'age' => 34
				),
				'john' => array(
					'email' => 'john.smith@gmail.com',
					'name' => 'John Smith',
					'age' => 34
				),
				'sheldon' => array(
					'email' => 'sheldon@cooper.com',
					'name' => 'Sheldon Cooper',
					'age' => 34
				)
			),
			$this->cassandra->cf('user')->getWhere(array('age' => 34))->getAll()
		);

		$this->assertEquals(
			array(
				'chuck' => array(
					'email' => 'chuck.norris@gmail.com',
					'name' => 'Chuck Norris'
				),
				'john' => array(
					'email' => 'john.smith@gmail.com',
					'name' => 'John Smith'
				)
			),
			$this->cassandra->cf('user')->getWhere(
				array(
					array(
						'age',
						Cassandra::OP_EQ,
						34
					),
					array(
						'name',
						Cassandra::OP_LTE,
						'K'
					)
				),
				array(
					'name',
					'email'
				)
			)->getAll()
		);
	}

	public function testGetWhereCanReturnManyRows() {
		for ($i = 0; $i < 2010; $i++) {
			$this->cassandra->set(
				'user.test'.$i,
				array(
					'email' => 'test'.$i.'@test.com',
					'name' => 'Test #'.$i,
					'age' => 51
				)
			);
		}

		$results = array();
		$iterator = $this->cassandra->cf('user')->getWhere(array('age' => 51));

		foreach ($iterator as $key => $row) {
			$results[$key] = $row;
		}

		$this->assertEquals(2010, count($results));
	}

	public function testGetMultipleReturnsSeveralKeysData() {
		$keys = array();
		$expected = array();

		for ($i = 0; $i < 501; $i++) {
			$key = 'user.test'.$i;

			$userData = array(
				'email' => 'test'.$i.'@test.com',
				'name' => 'Test #'.$i,
				'age' => 70
			);

			$this->cassandra->set(
				$key,
				$userData
			);

			$expected['test'.$i] = $userData;
			$keys[] = 'test'.$i;
		}

		$this->assertEquals(array(
			'email' => 'test0@test.com',
			'name' => 'Test #0',
			'age' => 70
		), $this->cassandra->get('user.test0'));

		$data = $this->cassandra->cf('user')->getMultiple($keys);

		$this->assertEquals($expected, $data);
	}

	public function testRowColumnsCanBeCounted() {
		$this->cassandra->set(
			'user.sheldon',
			array(
				'email' => 'sheldon@cooper.com',
				'name' => 'Sheldon Cooper',
				'age' => 34
			)
		);

		$this->assertEquals(
			3,
			$this->cassandra->cf('user')->getColumnCount('sheldon')
		);
	}

	public function testRowColumnsCanBeCounted2() {
		$this->cassandra->set(
			'user.sheldon',
			array(
				'email' => 'sheldon@cooper.com',
				'name' => 'Sheldon Cooper',
				'age' => 34
			)
		);

		$this->cassandra->set(
			'user.john',
			array(
				'email' => 'john@wayne.com',
				'name' => 'John Wayne',
				'age' => 34,
				'profession' => 'actor'
			)
		);

		$this->assertEquals(
			array(
				'sheldon' => 3,
				'john' => 4
			),
			$this->cassandra->cf('user')->getColumnCounts(array('sheldon', 'john'))
		);
	}

	/* Run only with partitioner:
	 * org.apache.cassandra.dht.CollatingOrderPreservingPartitioner
	public function testKeysCanBeFetchedByRange() {
		$expected = array();
		$expected2 = array();

		$this->cassandra->truncate('user');

		for ($i = ord('a'); $i < ord('z'); $i++) {
			$testData = array(
				'age' => 50,
				'email' => 'test'.$i.'@test.com',
				'name' => 'Test #'.$i
			);

			$this->cassandra->set(
				'user.test-'.chr($i),
				$testData
			);

			if ($i >= 101 && $i <= 107) {
				$expected['test-'.chr($i)] = $testData;
			}
		}

		$data = $this->cassandra->cf('user')->getKeyRange('test-'.chr(101), 'test-'.(chr(107)));
		$results = $data->getAll();

		$this->assertEquals($expected, $results);
	}
	*/

	public function testKeysCanBeFetchedByRange2() {
		$expected = array();

		$this->cassandra->truncate('user');

		for ($i = ord('a'); $i < ord('z'); $i++) {
			$testData = array(
				'age' => 50,
				'email' => 'test'.$i.'@test.com',
				'name' => 'Test #'.$i
			);

			$this->cassandra->set(
				'user.test-'.chr($i),
				$testData
			);
			$expected['test-'.chr($i)] = $testData;
		}

		$data = $this->cassandra->cf('user')->getKeyRange();
		$results = $data->getAll();

		$this->assertEquals($expected, $results);
	}

}