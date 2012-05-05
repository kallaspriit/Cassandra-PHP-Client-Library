<?php
/**
 * Cassandra-PHP-Client Library (CPCL).
 *
 * Cassandra PHP-based client library for managing and querying your Cassandra
 * cluster. It's a high-level library performing all the rather complex
 * low-level lifting and providing a simple to learn and use interface.
 *
 * Includes ideas and code snippets from PHPCassa project.
 *
 * Copyright (C) 2011 by Priit Kallas <kallaspriit@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @author Priit Kallas <kallaspriit@gmail.com>
 * @package Cassandra
 * @version 1.0
 */

/**
 * Provides an abstraction layer on top of Cassandra where you can extend this
 * class where the class name determines the column-family name and public
 * members the columns.
 */
class CassandraModel {

	/**
	 * Default connection to use.
	 *
	 * @var Cassandra
	 */
	protected static $_defaultConnection;

	/**
	 * Active connection.
	 *
	 * @var Cassandra
	 */
	protected $_connection;

	/**
	 * Row key
	 *
	 * @var string
	 */
	protected $_key;

	/**
	 * Constructor, optionally sets the row key and connection to use.
	 *
	 * If no connection is given, uses the default connection that you should
	 * set once using CassandraModel::setDefaultConnection().
	 *
	 * @param string $key Optional row key if creating a new entry
	 * @param Cassandra $connection Connection to use instead of default
	 */
	public function __construct($key = null, Cassandra $connection = null) {
		$this->_key = $key;

		if ($connection !== null) {
			$this->setConnection($connection);
		} else {
			$this->_connection = self::$_defaultConnection;
		}
	}

	/**
	 * Static method to get an instance of the class.
	 *
	 * Optionally sets the row key and connection to use. If no connection is
	 * given, uses the default connection that you should set once using
	 * CassandraModel::setDefaultConnection().
	 *
	 * @param string $key Optional row key if creating a new entry
	 * @param Cassandra $connection Connection to use instead of default
	 * @return CassandraModel Called class instance
	 */
	public static function getInstance(
		$key = null,
		Cassandra $connection = null
	) {
		$className = get_called_class();

		return new $className($key, $connection);
	}

	/**
	 * Sets the default Cassandra connection to use.
	 *
	 * You will usually call it once during boostrapping so you wouldn't need
	 * to set it again every time in constructor or the static methods.
	 *
	 * @param Cassandra $connection Connection to use by default
	 */
	public static function setDefaultConnection(Cassandra $connection) {
		self::$_defaultConnection = $connection;
	}

	/**
	 * Returns the current default connection.
	 *
	 * @return Cassandra
	 */
	public static function getDefaultConnection() {
		return self::$_defaultConnection;
	}

	/**
	 * Overrides the default connection for current instance.
	 *
	 * @param Cassandra $connection Connection to use
	 */
	public function setConnection(Cassandra $connection) {
		$this->_connection = $connection;
	}

	/**
	 * Returns current instance Cassandra connection.
	 *
	 * @return Cassandra
	 */
	public function getConnection() {
		return $this->_connection;
	}

	/**
	 * Populates model with data.
	 *
	 * Array keys should match model public members.
	 *
	 * @param array $data Data to use
	 * @return CassandraModel Self for chaining
	 */
	public function populate(array $data) {
		foreach ($data as $key => $value) {
			$this->$key = $value;
		}

		return $this;
	}

	/**
	 * Returns the row key, also setting it if key is provided.
	 *
	 * @param string $newKey Optioal new key to use
	 * @return string Active row key
	 */
	public function key($newKey = null) {
		if (isset($newKey)) {
			$this->_key = $newKey;
		}

		return $this->_key;
	}

	/**
	 * Returns model data as an associative array.
	 *
	 * @return array
	 */
	public function getData() {
		$columns = $this->getSetColumnNames();
		$data = array();

		foreach ($columns as $column) {
			$data[$column] = $this->$column;
		}

		return $data;
	}

	/**
	 * Loads data into the model by row key.
	 *
	 * Returns the model instance if entry was found and null otherwise.
	 *
	 * @param mixed $rowKey Value of the row key to find item by.
	 * @param integer $consistency Option override of default consistency level
	 * @param Cassandra $connection If not set, the default connection is used
	 * @return CassandraModel|null Model instance or null if not found
	 */
	public static function load(
		$rowKey,
		$consistency = null,
		Cassandra $connection = null
	) {
		$columnFamily = self::getColumnFamilyName();
		$columns = self::getColumnNames();

		if (!isset($connection)) {
			$connection = self::$_defaultConnection;
		}

		if (empty($columns)) {
			$columns = null;
		}
		
		$supercolumn = null;
		$dotPos = mb_strpos($rowKey, '.');
		
		if ($dotPos !== false) {
			$supercolumn = mb_substr($rowKey, $dotPos + 1);
			$rowKey = mb_substr($rowKey, 0, $dotPos);
		}
		
		$data = $connection->cf($columnFamily)->get(
			$rowKey,
			$columns,
			null,
			null,
			false,
			100,
			$supercolumn,
			$consistency
		);

		if ($data !== null) {
			$instance = self::getInstance();
			$instance->populate($data);
			$instance->_key = $rowKey;
			$instance->setConnection($connection);

			return $instance;
		} else {
			return null;
		}
	}

	/**
	 * Returns a range of rows.
	 *
	 * This is available only when using the order preserving partitioner. Can
	 * be used to paginate data etc. Returns an iterator that fetches the data
	 * from the database in chunks.
	 *
	 * @param string $startKey Starting key, null to start with first
	 * @param string $endKey Ending key, null for all
	 * @param integer $consistency Option override of default consistency level
	 * @param Cassandra $connection If not set, the default connection is used
	 * @return CassandraRangeDataIterator Key range iterator
	 */
	public static function getKeyRange(
		$startKey = null,
		$endKey = null,
		$consistency = null,
		Cassandra $connection = null
	) {
		$columnFamily = self::getColumnFamilyName();
		$columns = self::getColumnNames();

		if (!isset($connection)) {
			$connection = self::$_defaultConnection;
		}

		return $connection->cf($columnFamily)->getKeyRange(
			$startKey,
			$endKey,
			null,
			$columns,
			null,
			null,
			false,
			100,
			null,
			$consistency,
			100
		);
	}
	
	/**
	 * Returns all columns for given row key.
	 * 
	 * @param string $rowKey Row key to get data from
	 * @param Cassandra $connection If not set, the default connection is used
	 * @return array 
	 */
	public static function getAll($rowKey, Cassandra $connection = null) {
		if (!isset($connection)) {
			$connection = self::$_defaultConnection;
		}
		
		$columnFamily = self::getColumnFamilyName();
		
		return $connection->cf($columnFamily)->getAll($rowKey);
	}

	/**
	 * Returns a range of columns for a key.
	 *
	 * @param string $key Row key
	 * @param string $startColumn Starting column, null for first
	 * @param string $endColumn Ending column, null for last
	 * @param integer $limit Maximum number of columns to return
	 * @param integer $consistency Consistency to use, null for default
	 * @param Cassandra $connection Connection to use
	 * @return array Columns of data in given range up to given limit items
	 */
	public static function getColumnRange(
		$key,
		$startColumn = null,
		$endColumn = null,
		$limit = 100,
		$consistency = null,
		Cassandra $connection = null
	) {
		$columnFamily = self::getColumnFamilyName();

		if (!isset($connection)) {
			$connection = self::$_defaultConnection;
		}

		return $connection->cf($columnFamily)->get(
			$key,
			null,
			$startColumn,
			$endColumn,
			false,
			$limit,
			null,
			$consistency
		);
	}

	/**
	 * Saves the row into the database.
	 *
	 * You can set a new key for the row and also populate the model with data
	 * if you haven't done so already.
	 *
	 * You may also set the consistency if you like it to be something else from
	 * tge default of Cassandra::CONSISTENCY_ONE
	 *
	 * @param string $newKey Optional new row key
	 * @param array $populate Optionally populate the model with data
	 * @param integer $consistency Option override of default consistency level
	 * @return boolean Was saving the entry successful
	 */
	public function save(
		$newKey = null,
		array $populate = null,
		$consistency = null
	) {
		if (isset($newKey)) {
			$this->_key = $newKey;
		}

		if (is_array($populate)) {
			$this->populate($populate);
		}

		$columnFamily = self::getColumnFamilyName();

		$this->_connection->cf($columnFamily)->set(
			$this->_key,
			$this->getData(),
			$consistency
		);

		return true;
	}

	/**
	 * Inserts a new row into the database.
	 *
	 * This is a convenience proxy to creating an instance of the model,
	 * populating it and calling save().
	 *
	 * @param string $key Row key to use
	 * @param array $data Data to populate the model with
	 * @param integer $consistency Option override of default consistency level
	 * @param Cassandra $connection If not set, the default connection is used
	 * @return boolean Was adding the entry successful
	 */
	public static function insert(
		$key,
		array $data,
		$consistency = null,
		Cassandra $connection = null
	) {
		return self::getInstance(null, $connection)->save(
			$key, $data, $consistency
		);
	}

	/**
	 * Removes an entry or some columns of it.
	 *
	 * If the columns is left to null, the entire row is deleted.
	 *
	 * If you already have an instance of the model, use delete().
	 *
	 * @param string $key Row key to delete
	 * @param array $columns Optional columns to delete
	 * @param integer $consistency Option override of default consistency level
	 * @param Cassandra $connection If not set, the default connection is used
	 * @return boolean Was removing the entry successful
	 */
	public static function remove(
		$key,
		array $columns = null,
		$consistency = null,
		Cassandra $connection = null
	) {
		$model = self::getInstance($key, $connection);

		return $model->delete($columns, $consistency);
	}

	/**
	 * Removes a supercolumn entry or some columns of it.
	 *
	 * If the columns is left to null, the entire row is deleted.
	 *
	 * If you already have an instance of the model, use deleteSuper().
	 *
	 * @param string $key Row key to delete
	 * @param string $supercolumn Supercolumn name
	 * @param array $columns Optional columns to delete
	 * @param integer $consistency Option override of default consistency level
	 * @param Cassandra $connection If not set, the default connection is used
	 * @return boolean Was removing the entry successful
	 */
	public static function removeSuper(
		$key,
		$supercolumn,
		array $columns = null,
		$consistency = null,
		Cassandra $connection = null
	) {
		$model = self::getInstance($key, $connection);

		return $model->deleteSuper($supercolumn, $columns, $consistency);
	}

	/**
	 * Removes an entry or some columns of it.
	 *
	 * If the columns is left to null, the entire row is deleted.
	 *
	 * Uses the currently set row key, you can change it with key() method.
	 *
	 * You can remove a row by calling remove() statically.
	 *
	 * @param array $columns Optional columns to delete
	 * @param integer $consistency Option override of default consistency level
	 * @return boolean Was removing the entry successful
	 */
	public function delete(array $columns = null, $consistency = null) {
		$columnFamily = self::getColumnFamilyName();

		$this->_connection->cf($columnFamily)->remove(
			$this->_key,
			$columns,
			null,
			$consistency
		);

		return true;
	}

	/**
	 * Removes an supercolumn entry or some columns of it.
	 *
	 * If the columns is left to null, the entire row is deleted.
	 *
	 * Uses the currently set row key, you can change it with key() method.
	 *
	 * You can remove a row by calling removeSuper() statically.
	 *
	 * @param array $columns Optional columns to delete
	 * @param integer $consistency Option override of default consistency level
	 * @return boolean Was removing the entry successful
	 */
	public function deleteSuper($supercolumn, array $columns = null, $consistency = null) {
		$columnFamily = self::getColumnFamilyName();

		$this->_connection->cf($columnFamily)->remove(
			$this->_key,
			$columns,
			$supercolumn,
			$consistency
		);

		return true;
	}

	/**
	 * Returns the column family name that corresponds to given model class.
	 *
	 * The transformation is done in following steps:
	 * 1. remove "Model" from the end of class name
	 * 2. add a underscore before every upper-case word except first
	 * 3. make everything lowercase
	 *
	 * So for example, class names to table names:
	 * - UserModel > user
	 * - ForumTopicsModel > forum_topics
	 *
	 * @return string The table name
	 */
	public static function getColumnFamilyName() {
		$className = get_called_class();

		$components = preg_split(
			'/([A-Z][^A-Z]*)/',
			substr($className, 0, -5),
			-1,
			PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE
		);

		return mb_strtolower(implode('_', $components));
	}

	/**
	 * Returns the column names of the model.
	 *
	 * The column names are determined from public class vars.
	 *
	 * @return array
	 */
	public static function getColumnNames() {
		return array_keys(
			get_class_public_vars(get_called_class())
		);
	}

	/**
	 * Returns the column names of the model that have been set for current
	 * object.
	 *
	 * The column names are determined from public object vars.
	 *
	 * @return array
	 */
	public function getSetColumnNames() {
		return array_keys(get_object_public_vars($this));
	}

}

if (!function_exists('get_class_public_vars')) {
	/**
	 * Returns publicly available properties of a class.
	 *
	 * @param string $class_name Class name
	 * @return array Public var names
	 */
	function get_class_public_vars($class_name) {
		return get_class_vars($class_name);
	}
}

if (!function_exists('get_object_public_vars')) {
	/**
	 * Returns publicly available properties of an object.
	 *
	 * @param object $object Object to probe
	 * @return array Public var names
	 */
	function get_object_public_vars($object) {
		return get_object_vars($object);
	}
}