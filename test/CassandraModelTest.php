<?php

require_once '../Cassandra.php';
require_once '../CassandraModel.php';

class UserModel extends CassandraModel {
	public $email;
	public $name;
	public $age;
}

class CassandraModelTest extends PHPUnit_Framework_TestCase {

	protected $servers;

	/**
	 * @var Cassandra
	 */
	protected $cassandra;
	protected $cassandra2;

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
		$this->cassandra2 = Cassandra::createInstance($this->servers, 'Another');

		if (!self::$setupComplete) {
			try {
				$this->cassandra->dropKeyspace('CassandraModelTest');
			} catch (Exception $e) {}

			$this->cassandra->setMaxCallRetries(5);
			$this->cassandra->createKeyspace('CassandraModelTest');
			$this->cassandra->useKeyspace('CassandraModelTest');

			$this->cassandra->createStandardColumnFamily(
				'CassandraModelTest',
				'user',
				array(
					array(
						'name' => 'email',
						'type' => Cassandra::TYPE_UTF8
					),
					array(
						'name' => 'name',
						'type' => Cassandra::TYPE_UTF8,
						'index-type' => Cassandra::INDEX_KEYS,
						'index-name' => 'NameIdx'
					),
					array(
						'name' => 'age',
						'type' => Cassandra::TYPE_INTEGER,
						'index-type' => Cassandra::INDEX_KEYS,
						'index-name' => 'AgeIdx'
					)
				)
			);

			/*
			$this->cassandra->createSuperColumnFamily(
				'CassandraModelTest',
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
			*/

			self::$setupComplete = true;
		} else {
			$this->cassandra->useKeyspace('CassandraModelTest');
		}

		$this->cassandra2->useKeyspace('CassandraModelTest');
	}

	public function tearDown() {
		unset($this->cassandra);
		unset($this->cassandra2);

		if (function_exists('apc_clear_cache')) {
			apc_clear_cache();
		}
	}

	public function testNoConnectionByDefault() {
		$this->assertNull(CassandraModel::getDefaultConnection());
	}

	public function testDefaultConnectionCanBeSet() {
		$this->assertNull(CassandraModel::getDefaultConnection());

		CassandraModel::setDefaultConnection($this->cassandra);

		$this->assertEquals(
			CassandraModel::getDefaultConnection(),
			$this->cassandra
		);
	}

	public function testDefaultConnectionIsInherited() {
		CassandraModel::setDefaultConnection($this->cassandra);

		$model = new UserModel();

		$this->assertEquals(
			$model->getConnection(),
			$this->cassandra
		);
	}

	public function testConnectionCanBeSetInContructor() {
		CassandraModel::setDefaultConnection($this->cassandra);

		$model = new UserModel(null, $this->cassandra2);

		$this->assertEquals(
			$model->getConnection(),
			$this->cassandra2
		);
	}

	public function testConnectionCanBeChangedLater() {
		CassandraModel::setDefaultConnection($this->cassandra);

		$model = new UserModel();
		$model->setConnection($this->cassandra2);

		$this->assertEquals(
			$model->getConnection(),
			$this->cassandra2
		);
	}

	public function testRowCanBeLoadedByKey() {
		$this->cassandra->set(
			'user.john',
			array(
				'email' => 'john@smith.com',
				'name' => 'John Smith',
				'age' => 23
			)
		);

		$model = UserModel::load('john');

		$this->assertNotNull($model);
		$this->assertEquals($model->email, 'john@smith.com');
		$this->assertEquals($model->name, 'John Smith');
		$this->assertEquals($model->age, 23);
	}

	public function testRowCanBeLoadedByKey2() {
		$this->cassandra->set(
			'user.john',
			array(
				'email' => 'john@smith.com',
				'name' => 'John Smith',
				'age' => 23
			)
		);

		$model = UserModel::load('john');

		$this->assertEquals(
			$model->getData(),
			array(
				'email' => 'john@smith.com',
				'name' => 'John Smith',
				'age' => 23
			)
		);
	}

	public function testKeyIsNullByDefault() {
		$model = new UserModel();

		$this->assertNull($model->key());
	}

	public function testLoadedKeyIsReturned() {
		$this->cassandra->set(
			'user.john',
			array(
				'email' => 'john@smith.com',
				'name' => 'John Smith',
				'age' => 23
			)
		);

		$model = UserModel::load('john');

		$this->assertEquals($model->key(), 'john');
	}

	public function testKeyCanBeChanged() {
		$model = new UserModel();

		$result = $model->key('test');

		$this->assertEquals($result, 'test');
		$this->assertEquals($model->key(), 'test');
	}

	public function testLoadReturnsNullIfNotFound() {
		$model = UserModel::load('non-existing');

		$this->assertNull($model);
	}

	public function testEntryCanBeAdded1() {
		$this->assertNull(UserModel::load('jane'));

		$model = new UserModel('jane');
		$model->email = 'jane@doe.com';
		$model->name = 'Jane Doe';
		$model->age = 25;
		$result = $model->save();

		$this->assertTrue($result);

		$jane = UserModel::load('jane');

		$this->assertNotNull($jane);
		$this->assertEquals($jane->email, 'jane@doe.com');
		$this->assertEquals($jane->name, 'Jane Doe');
		$this->assertEquals($jane->age, 25);
		$this->assertEquals($jane->key(), 'jane');
	}

	public function testEntryCanBeAdded2() {
		$this->assertNull(UserModel::load('eve'));

		$model = new UserModel();
		$model->email = 'eve@doe.com';
		$model->name = 'Eve Doe';
		$model->age = 23;
		$result = $model->save('eve');

		$this->assertTrue($result);

		$jane = UserModel::load('eve', null, $this->cassandra2);

		$this->assertNotNull($jane);
		$this->assertEquals($jane->email, 'eve@doe.com');
		$this->assertEquals($jane->name, 'Eve Doe');
		$this->assertEquals($jane->age, 23);
		$this->assertEquals($jane->key(), 'eve');
		$this->assertEquals($jane->getConnection(), $this->cassandra2);
	}

	public function testEntryCanBeAdded3() {
		$this->assertNull(UserModel::load('alice'));

		$model = new UserModel();
		$result = $model->save('alice', array(
			'email' => 'alice@wonderland.com',
			'name' => 'Alice',
			'age' => 18,
		));

		$this->assertTrue($result);

		$alice = UserModel::load('alice');

		$this->assertNotNull($alice);
		$this->assertEquals($alice->email, 'alice@wonderland.com');
		$this->assertEquals($alice->name, 'Alice');
		$this->assertEquals($alice->age, 18);
		$this->assertEquals($alice->key(), 'alice');
	}

	public function testEntryCanBeInserted() {
		$this->assertNull(UserModel::load('kate'));

		$result = UserModel::insert('kate', array(
			'email' => 'kate@gmail.com',
			'name' => 'Kate',
			'age' => 21,
		));

		$this->assertTrue($result);

		$kate = UserModel::load('kate');

		$this->assertNotNull($kate);
		$this->assertEquals($kate->email, 'kate@gmail.com');
		$this->assertEquals($kate->name, 'Kate');
		$this->assertEquals($kate->age, 21);
		$this->assertEquals($kate->key(), 'kate');
	}

	public function testEntryCanBeInserted2() {
		$this->assertNull(UserModel::load('katy'));

		$result = UserModel::insert(
			'katy',
			array(
				'email' => 'katy@gmail.com',
				'name' => 'Katy',
				'age' => 22,
			),
			null,
			$this->cassandra2
		);

		$this->assertTrue($result);

		$katy = UserModel::load('katy');

		$this->assertNotNull($katy);
		$this->assertEquals($katy->email, 'katy@gmail.com');
		$this->assertEquals($katy->name, 'Katy');
		$this->assertEquals($katy->age, 22);
		$this->assertEquals($katy->key(), 'katy');
	}

	public function testEntryCanBeUpdated() {
		$this->assertNull(UserModel::load('dave'));

		$result = UserModel::insert('dave', array(
			'email' => 'dave@gmail.com',
			'name' => 'Dave',
			'age' => 33,
		));

		$this->assertTrue($result);

		$dave = UserModel::load('dave');

		$this->assertNotNull($dave);
		$this->assertEquals($dave->email, 'dave@gmail.com');
		$this->assertEquals($dave->name, 'Dave');
		$this->assertEquals($dave->age, 33);
		$this->assertEquals($dave->key(), 'dave');

		$dave->email = 'dave@hotmail.com';
		$dave->save();

		$dave2 = UserModel::load('dave');

		$this->assertNotNull($dave2);
		$this->assertEquals($dave2->email, 'dave@hotmail.com');
		$this->assertEquals($dave2->name, 'Dave');
		$this->assertEquals($dave2->age, 33);
		$this->assertEquals($dave2->key(), 'dave');

		$dave2->key('david');
		$dave2->name = 'David';
		$dave2->save();

		$dave3 = UserModel::load('dave');

		$this->assertNotNull($dave3);
		$this->assertEquals($dave3->email, 'dave@hotmail.com');
		$this->assertEquals($dave3->name, 'Dave');
		$this->assertEquals($dave3->age, 33);
		$this->assertEquals($dave3->key(), 'dave');

		$dave4 = UserModel::load('david');

		$this->assertNotNull($dave4);
		$this->assertEquals($dave4->email, 'dave@hotmail.com');
		$this->assertEquals($dave4->name, 'David');
		$this->assertEquals($dave4->age, 33);
		$this->assertEquals($dave4->key(), 'david');
	}

	public function testRowsCanBeRemoved() {
		UserModel::insert('dave', array(
			'email' => 'dave@gmail.com',
			'name' => 'Dave',
			'age' => 33,
		));

		$dave = UserModel::load('dave');
		$this->assertEquals($dave->email, 'dave@gmail.com');
		$this->assertEquals($dave->name, 'Dave');
		$this->assertEquals($dave->age, 33);

		UserModel::remove('dave');

		$dave2 = UserModel::load('dave');
		$this->assertNull($dave2);
	}

	public function testRowsCanBeRemoved2() {
		UserModel::insert('dave', array(
			'email' => 'dave@gmail.com',
			'name' => 'Dave',
			'age' => 33,
		));

		$dave = UserModel::load('dave');
		$this->assertEquals($dave->email, 'dave@gmail.com');
		$this->assertEquals($dave->name, 'Dave');
		$this->assertEquals($dave->age, 33);

		$dave->delete();

		$dave2 = UserModel::load('dave');
		$this->assertNull($dave2);
	}

	/* not yet supported by CPCL
	public function testColumnsCanBeRemoved() {
		UserModel::insert('dave', array(
			'email' => 'dave@gmail.com',
			'name' => 'Dave',
			'age' => 33,
		));

		$dave = UserModel::load('dave');
		$this->assertEquals($dave->email, 'dave@gmail.com');
		$this->assertEquals($dave->name, 'Dave');
		$this->assertEquals($dave->age, 33);

		$dave->delete(array('email', 'age'));

		$dave2 = UserModel::load('dave');
		$this->assertNotNull($dave2);
		$this->assertNull($dave->email);
		$this->assertEquals($dave->name, 'Dave');
		$this->assertNull($dave->age);
	}

	public function testColumnsCanBeRemoved() {
		UserModel::insert('dave', array(
			'email' => 'dave@gmail.com',
			'name' => 'Dave',
			'age' => 33,
			'sex' => 'male'
		));

		UserModel::remove('dave', array('email', 'sex'));

		$dave = UserModel::load('dave');
		$this->assertEquals($dave->getData(), array(
			'name' => 'Dave',
			'age' => 33
		));
	}

	*/
}