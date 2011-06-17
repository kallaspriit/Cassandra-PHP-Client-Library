<?php

$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__) . '/thrift/';
define('THRIFT_PATH', $GLOBALS['THRIFT_ROOT']);

require_once $GLOBALS['THRIFT_ROOT'].'/packages/cassandra/Cassandra.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';

class CassandraConnection {
	
	protected $host;
	protected $port;
	protected $useFramedTransport;
	protected $sendTimeoutMs;
	protected $receiveTimeoutMs;
	protected $socket;
	protected $transport;
	protected $protocol;
	protected $client;
	protected $isOpen;
	
	public function __construct(
		$host = '127.0.0.1',
		$port = 9160,
		$useFramedTransport = true,
		$sendTimeoutMs = null,
		$receiveTimeoutMs = null
	) {
		$this->host = $host;
		$this->port = $port;
		$this->useFramedTransport = $useFramedTransport;
		$this->sendTimeoutMs = $sendTimeoutMs;
		$this->receiveTimeoutMs = $receiveTimeoutMs;
		$this->isOpen = false;
		
		$this->socket = $this->createSocket(
			$host,
			$port,
			$sendTimeoutMs,
			$receiveTimeoutMs
		);
		
		if ($useFramedTransport) {
			$this->transport = $this->createFramedTransport($this->socket);
		} else {
			$this->transport = $this->createBufferedTransport($this->socket);
		}
		
		$this->transport->open();
		$this->isOpen = true;
		
		$this->protocol = new TBinaryProtocolAccelerated($this->transport);
		$this->client = new CassandraClient($this->protocol);
	}
	
	public function __destruct() {
		$this->close();
	}
	
	public function close() {
		if ($this->isOpen) {
			$this->transport->flush();
			$this->transport->close();
			
			$this->isOpen = false;
		}
	}
	
	public function isOpen() {
		return $this->isOpen;
	}
	
	public function getClient() {
		if (!$this->isOpen) {
			throw new CassandraConnectionClosedException(
				'The connection has been closed'
			);
		}
		
		return $this->client;
	}
	
	public function getTransport() {
		return $this->transport;
	}
	
	public function useKeyspace(
		$keyspace,
		$username = null,
		$password = null
	) {
		$attempts = 3;
		$success = false;
		
		while($attempts-- > 0 && !$success) {
			try {
				$this->client->set_keyspace($keyspace);
				$success = true;
			} catch (cassandra_InvalidRequestException $e) {
				continue;
			}
		}
		
		if (!$success) {
			throw new CassandraSettingKeyspaceFailedException(
				'Using keyspace "'.$keyspace.'" failed after several attempts'
			);
		}
		
		if ($username !== null) {
			$request = new cassandra_AuthenticationRequest(
				array('credentials' => array($username, $password))
			);

			$this->client->login($request);
		}
	}
	
	protected function createSocket(
		$host,
		$port,
		$sendTimeoutMs,
		$receiveTimeoutMs
	) {
		$socket = new TSocket($host, $port);
		
		if ($sendTimeoutMs !== null) {
			$socket->setSendTimeout($sendTimeoutMs);
		}
		
		if ($receiveTimeoutMs !== null) {
			$socket->setRecvTimeout($receiveTimeoutMs);
		}
		
		return $socket;
	}
	
	protected function createFramedTransport(TSocket $socket) {
		require_once THRIFT_PATH.'/transport/TFramedTransport.php';
			
		return new TFramedTransport($socket, true, true);
	}
	
	protected function createBufferedTransport(TSocket $socket) {
		require_once THRIFT_PATH.'/transport/TBufferedTransport.php';
			
		return new TBufferedTransport($socket, 512, 512);
	}
}

class CassandraCluster {
	
	protected $keyspace;
	protected $username;
	protected $password;
	protected $servers = array();
	protected $connections = array();
	
	public function __construct(array $servers = array()) {
		foreach ($servers as $server) {
			$this->registerServer(
				isset($server['host']) ? $server['host'] : '127.0.0.1',
				isset($server['port']) ? $server['port'] : 9160,
				isset($server['use-framed-transport']) ? $server['use-framed-transport'] : true,
				isset($server['send-timeout-ms']) ? $server['send-timeout-ms'] : null,
				isset($server['receive-timeout-ms']) ? $server['receive-timeout-ms'] : null
			);
		}
	}
	
	public function __destruct() {
		$this->closeConnections();
	}
	
	public function registerServer(
		$host = '127.0.0.1',
		$port = 9160,
		$useFramedTransport = true,
		$sendTimeoutMs = null,
		$receiveTimeoutMs = null
	) {
		$this->servers[] = array(
			'host' => $host,
			'port' => $port,
			'use-framed-transport' => $useFramedTransport,
			'send-timeout-ms' => $sendTimeoutMs,
			'receive-timeout-ms' => $receiveTimeoutMs
		);
		
		return $this;
	}
	
	public function useKeyspace($keyspace, $username = null, $password = null) {
		$this->keyspace = $keyspace;
		$this->username = $username;
		$this->password = $password;
		
		foreach ($this->connections as $connection) {
			$connection->useKeyspace(
				$keyspace,
				$username,
				$password
			);
		}
		
		return $this;
	}
	
	public function getCurrentKeyspace() {
		return $this->keyspace;
	}
	
	public function getServers() {
		return $this->servers;
	}
	
	public function getConnection() {
		if (empty($this->servers)) {
			throw new CassandraConnectionFailedException(
				'Unable to create connection, the cluster server pool is empty'
			);
		}
			
		$serverCount = count($this->servers);
		$attemptsLeft = $serverCount * 2;
		
		while ($attemptsLeft-- > 0) {
			$randomServerIndex = mt_rand(0, $serverCount - 1);
			
			if (isset($this->connections[$randomServerIndex])) {
				if (!$this->connections[$randomServerIndex]->isOpen()) {
					unset($this->connections[$randomServerIndex]);
					
					continue;
				}

				return $this->connections[$randomServerIndex];
			} else {
				$server = $this->servers[$randomServerIndex];
				
				try {
					$this->connections[$randomServerIndex] = new CassandraConnection(
						$server['host'],
						$server['port'],
						$server['use-framed-transport'],
						$server['send-timeout-ms'],
						$server['receive-timeout-ms']
					);
					
					$this->connections[$randomServerIndex]->useKeyspace(
						$this->keyspace,
						$this->username,
						$this->password
					);
					
					return $this->connections[$randomServerIndex];
				} catch (TException $e) {
					continue;
				}
			}
		}
		
		throw new CassandraConnectionFailedException(
			'Connecting to any of the '.$serverCount.' nodes failed'
		);
	}
	
	public function closeConnections() {
		foreach ($this->connections as $connection) {
			$connection->close();
		}
		
		$this->connections = array();
		
		return $this;
	}
}

class Cassandra {
	
	protected static $instances = array();
	protected static $keyspaceRequiredMethods;
	protected static $requestKeyTokens = array('.', ':', ',', '-', '|');
	
	protected $cluster;
	protected $maxCallRetries = 5;
	protected $defaultColumnCount = 100;
	protected $columnFamilies = array();
	protected $keyspaceAuthentication = array();
	protected $autopack = true;
	
	const CONSISTENCY_ONE = cassandra_ConsistencyLevel::ONE;
	const CONSISTENCY_QUORUM = cassandra_ConsistencyLevel::QUORUM;
	const CONSISTENCY_ANY = cassandra_ConsistencyLevel::ANY;
	const CONSISTENCY_ALL = cassandra_ConsistencyLevel::ALL;
	
	const COLUMN_STANDARD = 'Standard';
	const COLUMN_SUPER = 'Super';
	
	const TYPE_ASCII = 'AsciiType';
	const TYPE_BYTES = 'BytesType';
	const TYPE_LEXICAL_UUID = 'LexicalUUIDType';
	const TYPE_LONG = 'LongType';
	const TYPE_INTEGER = 'IntegerType';
	const TYPE_TIME_UUID = 'TimeUUIDType';
	const TYPE_UTF8 = 'UTF8Type';
	
	const OP_EQ = cassandra_IndexOperator::EQ;
	const OP_LT = cassandra_IndexOperator::LT;
	const OP_GT = cassandra_IndexOperator::GT;
	const OP_LTE = cassandra_IndexOperator::LTE;
	const OP_GTE = cassandra_IndexOperator::GTE;
	
	const PLACEMENT_SIMPLE = 'org.apache.cassandra.locator.SimpleStrategy';
	const PLACEMENT_NETWORK
		= 'org.apache.cassandra.locator.NetworkTopologyStrategy';
	const PLACEMENT_OLD_NETWORK
		= 'org.apache.cassandra.locator.OldNetworkTopologyStrategy';
	
	const INDEX_KEYS = 0;
	
	public function __construct(array $servers = array(), $autopack = true) {
		$this->cluster = new CassandraCluster($servers);
		$this->autopack = $autopack;
	}
	
	public static function createInstance(array $servers, $name = 'main') {
		self::$instances[$name] = new self($servers);
		
		return self::$instances[$name];
	}
	
	public static function getInstance($name = 'main') {
		if (!isset(self::$instances[$name])) {
			throw new CassandraInvalidRequestException(
				'Instance called "'.$name.'" does not exist'
			);
		}
		
		return self::$instances[$name];
	}
	
	public function registerKeyspace(
		$keyspace,
		$username = null,
		$password = null
	) {
		$this->keyspaceAuthentication[$keyspace] = array(
			'username' => $username,
			'password' => $password
		);
	}
	
	public function useKeyspace($keyspace, $username = null, $password = null) {
		if (!empty($username)) {
			$this->registerKeyspace($keyspace, $username, $password);
		} else if (isset($this->keyspaceAuthentication[$keyspace])) {
			$username = $this->keyspaceAuthentication[$keyspace]['username'];
			$password = $this->keyspaceAuthentication[$keyspace]['password'];
		}
		
		$this->cluster->useKeyspace($keyspace, $username, $password);
		
		return $this;
	}
	
	/**
	 * Returns the Cassandra cluster of servers.
	 * 
	 * @return CassandraCluster
	 */
	public function getCluster() {
		return $this->cluster;
	}
	
	/**
	 * Returns random connection to a node.
	 * 
	 * @return CassandraConnection
	 */
	public function getConnection() {
		return $this->cluster->getConnection();
	}
	
	public function closeConnections() {
		$this->cluster->closeConnections();
	}
	
	/**
	 * Return the thrift client.
	 * 
	 * @return CassandraClient
	 */
	public function getClient() {
		return $this->cluster->getConnection()->getClient();
	}
	
	public function setMaxCallRetries($retryCount) {
		$this->maxCallRetries = $retryCount;
		
		return $this;
	}
	
	public function setDefaultColumnCount($columnCountLimit) {
		$this->defaultColumnCount = $columnCountLimit;
	}
	
	public function call(/*$methodName, $arg1, $arg2 */) {
		$args = func_get_args();
		$methodName = array_shift($args);
		
		$tries = $this->maxCallRetries;
		$lastException = null;
		
		$keyspaceRequiredMethods = self::getKeyspaceRequiredMethods();
		
		if (
			in_array($methodName, $keyspaceRequiredMethods)
			&& $this->cluster->getCurrentKeyspace() === null
		) {
			throw new CassandraInvalidRequestException(
				'Unable to call "'.$methodName.'", no keyspace has been set'
			);
		}
		
		while($tries-- > 0) {
			$client = $this->getClient();
			
			try {
                return call_user_func_array(array($client, $methodName), $args);
            } catch (Exception $e) {
				$lastException = $e;
			}
		}

		throw new CassandraMaxRetriesException(
			'Failed calling "'.$methodName.'" the maximum of '.
			$this->maxCallRetries.' times',
			$lastException->getCode(),
			$lastException
		);
	}
	
	public function describeKeyspace($keyspace = null) {
		if ($keyspace === null) {
			$keyspace = $this->cluster->getCurrentKeyspace();
		}
		
		return $this->call('describe_keyspace', $keyspace);
	}
	
	public function getKeyspaceSchema($keyspace = null, $useCache = true) {
		if ($keyspace === null) {
			$keyspace = $this->cluster->getCurrentKeyspace();
		}
		
		$cacheKey = 'cassandra.schema|'.$keyspace;

		$schema = $useCache ? apc_fetch($cacheKey) : false;

		if ($schema !== false) {
			return $schema;
		}

		$info = $this->describeKeyspace($keyspace);
		
		$schema = array(
			'name' => $info->name,
			'placement-strategy' => $info->strategy_class,
			'placement-strategy-options' => $info->strategy_options,
			'replication-factor' => $info->replication_factor,
			'column-families' => array()
		);
		
		foreach ($info->cf_defs as $columnFamily) {
			$isSuper = $columnFamily->column_type == 'Super' ? true : false;
			
			$schema['column-families'][$columnFamily->name] = array(
				'name' => $columnFamily->name,
				'super' => $isSuper,
				'column-type' =>
					$isSuper
						? CassandraUtil::extractType(
							$columnFamily->subcomparator_type
						)
						: CassandraUtil::extractType(
							$columnFamily->comparator_type
						),
				'super-type' =>
					$isSuper
						? CassandraUtil::extractType(
							$columnFamily->comparator_type
						)
						: null,
				'data-type' => CassandraUtil::extractType(
					$columnFamily->default_validation_class
				),
				'column-data-types' => array()
			);

			if (
				is_array($columnFamily->column_metadata)
				&& !empty($columnFamily->column_metadata)
			) {
				foreach ($columnFamily->column_metadata as $column) {
					$schema['column-families'][$columnFamily->name]['column-data-types'][$column->name]
						= CassandraUtil::extractType($column->validation_class);
				}
			}
		}
		
		if ($useCache) {
			apc_store($cacheKey, $schema, 3600);
		}
		
		return $schema;
	}
	
	public function getVersion() {
		return $this->call('describe_version');
	}
	
	public function cf($name) {
		if (!isset($this->columnFamilies[$name])) {
			$this->columnFamilies[$name] = new CassandraColumnFamily(
				$this,
				$name,
				Cassandra::CONSISTENCY_ONE,
				Cassandra::CONSISTENCY_ONE,
				$this->autopack
			);
		}
		
		return $this->columnFamilies[$name];
	}
	
	/**
	 * Requests some data as a single request string.
	 * 
	 * Supported patterns:
	 * - family.key
	 * - family.key:col1,col2,coln
	 * - family.key:col1-col2
	 * - family.key:col1-col2|100
	 * - family\.name:key\:name:col\.1,col\|2|100
	 * 
	 * In all of the parts, the following characters shoudl be escaped (\. etc)
	 * '.', ':', ',', '-', '|'
	 * 
	 * @param type $request
	 * @param type $consistency 
	 */
	public function get($request, $consistency = null) {
		$details = $this->parseRequest($request);

		return $this->cf($details['column-family'])->get(
			$details['key'],
			$details['columns'],
			$details['start-column'],
			$details['end-column'],
			$details['reversed'],
			$details['column-count'],
			$details['super-column'],
			$consistency
		);
	}
	
	public function set($key, array $columns, $consistency = null) {
		$dotPosition = mb_strpos($key, '.');
		
		if ($dotPosition === false) {
			throw new CassandraInvalidPatternException(
				'Unable to set "'.$key.'", expected the column family name and'.
				'key name seperated by a fullstop'
			);
		}
		
		$columnFamilyName = mb_substr($key, 0, $dotPosition);
		$keyName = mb_substr($key, $dotPosition + 1);
		
		return $this->cf($columnFamilyName)->set(
			$keyName,
			$columns,
			$consistency
		);
	}
	
	public function createKeyspace(
		$name,
		$replicationFactor = 1,
		$placementStrategyClass = self::PLACEMENT_SIMPLE,
		$placementStrategyOptions = null
	) {
		$def = new cassandra_KsDef();
		
		$def->name = $name;
		$def->strategy_class = $placementStrategyClass;
		$def->strategy_options = $placementStrategyOptions;
		$def->cf_defs = array();
		$def->replication_factor = $replicationFactor;
		
		return $this->call('system_add_keyspace', $def);
	}
	
	public function updateKeyspace(
		$name,
		$replicationFactor = 1,
		$placementStrategyClass = self::PLACEMENT_SIMPLE,
		$placementStrategyOptions = null
	) {
		$def = new cassandra_KsDef();
		
		$def->name = $name;
		$def->strategy_class = $placementStrategyClass;
		$def->strategy_options = $placementStrategyOptions;
		$def->cf_defs = array();
		$def->replication_factor = $replicationFactor;
		
		return $this->call('system_update_keyspace', $def);
	}
	
	public function dropKeyspace($name) {
		return $this->call('system_drop_keyspace', $name);
	}
	
	public function createStandardColumnFamily(
		$keyspace,
		$name,
		$columns = array(),
		$comparatorType = Cassandra::TYPE_UTF8,
		$defaultValidationClass = Cassandra::TYPE_UTF8,
		$comment = null,
		$rowCacheSize = null,
		$keyCacheSize = null,
		$readRepairChance = null,
		$cgGraceSeconds = null,
		$minCompactionThreshold = null,
		$maxCompactionThreshold = null,
		$rowCacheSavePeriodSeconds = null,
		$keyCacheSavePeriodSeconds = null,
		$memtableFlushAfterMins = null,
		$memtableFlushAfterThroughputMb = null,
		$memtableFlushAfterOpsMillions = null
	) {
		return $this->createColumnFamily(
			$keyspace,
			$name,
			Cassandra::COLUMN_STANDARD,
			$columns,
			$comparatorType,
			null,
			$defaultValidationClass,
			$comment,
			$rowCacheSize,
			$keyCacheSize,
			$readRepairChance,
			$cgGraceSeconds,
			$minCompactionThreshold,
			$maxCompactionThreshold,
			$rowCacheSavePeriodSeconds,
			$keyCacheSavePeriodSeconds,
			$memtableFlushAfterMins,
			$memtableFlushAfterThroughputMb,
			$memtableFlushAfterOpsMillions
		);
	}
	
	public function createSuperColumnFamily(
		$keyspace,
		$name,
		$columns = array(),
		$comparatorType = Cassandra::TYPE_UTF8,
		$subcomparatorType = Cassandra::TYPE_UTF8,
		$defaultValidationClass = Cassandra::TYPE_UTF8,
		$comment = null,
		$rowCacheSize = null,
		$keyCacheSize = null,
		$readRepairChance = null,
		$cgGraceSeconds = null,
		$minCompactionThreshold = null,
		$maxCompactionThreshold = null,
		$rowCacheSavePeriodSeconds = null,
		$keyCacheSavePeriodSeconds = null,
		$memtableFlushAfterMins = null,
		$memtableFlushAfterThroughputMb = null,
		$memtableFlushAfterOpsMillions = null
	) {
		return $this->createColumnFamily(
			$keyspace,
			$name,
			Cassandra::COLUMN_SUPER,
			$columns,
			$comparatorType,
			$subcomparatorType,
			$defaultValidationClass,
			$comment,
			$rowCacheSize,
			$keyCacheSize,
			$readRepairChance,
			$cgGraceSeconds,
			$minCompactionThreshold,
			$maxCompactionThreshold,
			$rowCacheSavePeriodSeconds,
			$keyCacheSavePeriodSeconds,
			$memtableFlushAfterMins,
			$memtableFlushAfterThroughputMb,
			$memtableFlushAfterOpsMillions
		);
	}
	
	public function createColumnFamily(
		$keyspace,
		$name,
		$columnType = Cassandra::COLUMN_STANDARD,
		$columns = array(),
		$comparatorType = Cassandra::TYPE_UTF8,
		$subcomparatorType = null,
		$defaultValidationClass = Cassandra::TYPE_UTF8,
		$comment = null,
		$rowCacheSize = null,
		$keyCacheSize = null,
		$readRepairChance = null,
		$cgGraceSeconds = null,
		$minCompactionThreshold = null,
		$maxCompactionThreshold = null,
		$rowCacheSavePeriodSeconds = null,
		$keyCacheSavePeriodSeconds = null,
		$memtableFlushAfterMins = null,
		$memtableFlushAfterThroughputMb = null,
		$memtableFlushAfterOpsMillions = null
	) {
		$columnMetadata = null;
		
		if (!empty($columns)) {
			$columnMetadata = array();
			
			foreach ($columns as $column) {
				$columnMetadata[] = $this->createColumnDefinition($column);
			}
		}
		
		$def = new cassandra_CfDef();
		
		$def->keyspace = $keyspace;
		$def->name = $name;
		$def->column_type = $columnType;
		$def->comparator_type = $comparatorType;
		$def->subcomparator_type = $subcomparatorType;
		$def->comment = $comment;
		$def->row_cache_size = $rowCacheSize;
		
		if ($keyCacheSize !== null) {
			$def->key_cache_size = $keyCacheSize;
		}
		
		$def->read_repair_chance = $readRepairChance;
		$def->column_metadata = $columnMetadata;
		$def->gc_grace_seconds = $cgGraceSeconds;
		$def->default_validation_class = $defaultValidationClass;
		$def->min_compaction_threshold = $minCompactionThreshold;
		$def->max_compaction_threshold = $maxCompactionThreshold;
		$def->row_cache_save_period_in_seconds = $rowCacheSavePeriodSeconds;
		$def->key_cache_save_period_in_seconds = $keyCacheSavePeriodSeconds;
		$def->memtable_flush_after_mins = $memtableFlushAfterMins;
		$def->memtable_throughput_in_mb = $memtableFlushAfterThroughputMb;
		$def->memtable_operations_in_millions = $memtableFlushAfterOpsMillions;

		return $this->call('system_add_column_family', $def);
	}
	
	public static function getKeyspaceRequiredMethods() {
		if (self::$keyspaceRequiredMethods === null) {
			self::$keyspaceRequiredMethods = array(
				'login',
				'get',
				'get_slice',
				'get_count',
				'multiget_slice',
				'multiget_count',
				'get_indexed_slices',
				'insert',
				'remove',
				'batch_mutate',
				'truncate',
				'describe_splits'
			);
		}
		
		return self::$keyspaceRequiredMethods;
	}

	protected function parseRequest($request) {
		foreach(self::$requestKeyTokens as $tokenKey => $keyToken) {
			$request = str_replace(
				'\\'.$keyToken,
				'[/'.$tokenKey.'\\]',
				$request
			);
		}
		
		$components = array();
		
		$matchSuccessful = preg_match(
			'/^(.+)\.(.+)((\.(.*))|\[\])?(:(.*))?(\|(\d*)(R)?)?$/U',
			// 1     2   34  5           6 7     8  9    10        
			$request,
			$components
		);

		if (!$matchSuccessful || empty($components)) {
			throw new CassandraInvalidPatternException(
				'Invalid get request "'.$request.'" provided'
			);
		}

		$result = array(
			'column-family' => $components[1],
			'key' => $components[2],
			'columns' => null,
			'start-column' => null,
			'end-column' => null,
			'reversed' => false,
			'column-count' => $this->defaultColumnCount,
			'super-column' => null
		);
		
		$componentCount = count($components);
		
		// @codeCoverageIgnoreStart
		if ($componentCount < 3 || $componentCount > 11) {
			throw new CassandraInvalidPatternException(
				'Invalid pattern, expected between 3 and 11 components, got '.
				$componentCount.', this should not happen'
			);
		}
		// @codeCoverageIgnoreEnd
		
		if ($componentCount >= 6) {
			if (
				!empty($components[5])
				&& $components[5] != '[]'
			) {
				$result['super-column'] = $components[5];
			}
			
			if ($componentCount >= 8) {
				if (!empty($components[7])) {
					if (strpos($components[7], ',') !== false) {
						$columns = explode(',', $components[7]);

						foreach ($columns as $columnIndex => $columnName) {
							$columns[$columnIndex] = trim($columnName);
						}

						$result['columns'] = $columns;
					} else if (strpos($components[7], '-') !== false) {
						$rangeColumns = explode('-', $components[7]);

						if (count($rangeColumns) > 2) {
							throw new CassandraInvalidPatternException(
								'Expected no more than 2 columns '.
								'to define a range'
							);
						}

						$result['start-column'] = trim($rangeColumns[0]);
						$result['end-column'] = trim($rangeColumns[1]);
					} else {
						$result['columns'] = array(trim($components[7]));
					}
				}

				if ($componentCount >= 10 && !empty($components[9])) {
					$columnCount = (int)$components[9];

					$result['column-count'] = $columnCount;
				}

				if (
					$componentCount == 11
					&& strtoupper($components[10]) == 'R'
				) {
					$result['reversed'] = true;
				}
			}
		}
		
		foreach ($result as $key => $item) {
			$result[$key] = self::unescape($item);
		}
	
		return $result;
	}
	
	public static function escape($value) {
		foreach(self::$requestKeyTokens as $keyToken) {
			$value = str_replace(
				$keyToken,
				'\\'.$keyToken,
				$value
			);
		}
		
		return $value;
	}
	
	public static function unescape($value) {
		if (empty($value)) {
			return $value;
		}
		else if (is_array($value)) {
			foreach ($value as $key => $item) {
				$value[$key] = self::unescape($item);
			}
		} else if (is_string($value)) {
			foreach(self::$requestKeyTokens as $tokenKey => $keyToken) {
				$value = str_replace(
					'[/'.$tokenKey.'\\]',
					$keyToken,
					$value
				);
			}
		}
		
		return $value;
	}
	
	protected function createColumnDefinition(array $info) {
		$def = new cassandra_ColumnDef();

		$def->name = $info['name'];
		
		if (!empty($info['type'])) {
			$def->validation_class = $info['type'];
		}
		
		if (isset($info['index-type'])) {
			$def->index_type = $info['index-type'];
		}
		
		if (!empty($info['index-name'])) {
			$def->index_type = $info['index-name'];
		}
		
		return $def;
	}
}

class CassandraColumnFamily {
	
	protected $connection;
	protected $name;
	protected $defaultReadConsistency;
	protected $defaultWriteConsistency;
	protected $autopack;
	protected $schema;
	
	public function __construct(
		Cassandra $connection,
		$name,
		$defaultReadConsistency = Cassandra::CONSISTENCY_ONE,
		$defaultWriteConsistency = Cassandra::CONSISTENCY_ONE,
		$autopack = true
	) {
		$this->connection = $connection;
		$this->name = $name;
		$this->defaultReadConsistency = $defaultReadConsistency;
		$this->defaultWriteConsistency = $defaultWriteConsistency;
		$this->autopack = $autopack;
		$this->schema = null;
	}
	
	public function getSchema($useCache = true) {
		if ($this->schema === null) {
			$keyspaceSchema = $this->connection->getKeyspaceSchema(
				null,
				$useCache
			);

			if (!isset($keyspaceSchema['column-families'][$this->name])) {
				throw new CassandraColumnFamilyNotFoundException(
					'Schema for column family "'.$this->name.'" not found'
				);
			}
			
			$this->schema = $keyspaceSchema['column-families'][$this->name];
		}
		
		return $this->schema;
	}
	
	public function getColumnNameType($useCache = true) {
		$schema = $this->getSchema($useCache);
		
		if ($schema['super']) {
			return $schema['super-type'];
		} else {
			return $schema['column-type'];
		}
	}
	
	public function getColumnValueType($columnName, $useCache = true) {
		$schema = $this->getSchema($useCache);
			
		if (isset($schema['column-data-types'][$columnName])) {
			return $schema['column-data-types'][$columnName];
		}
		
		return 'BytesType';
	}
	
	public function getAll($key, $superColumn = null, $consistency = null) {
		return $this->get(
			$key,
			null,
			null,
			null,
			false,
			100,
			$superColumn,
			$consistency
		);
	}
	
	public function getColumns(
		$key,
		array $columns,
		$superColumn = null,
		$consistency = null
	) {
		return $this->get(
			$key,
			$columns,
			null,
			null,
			false,
			100,
			$superColumn,
			$consistency
		);
	}
	
	public function getColumnRange(
		$key,
		$startColumn,
		$endColumn,
		$superColumn = null,
		$consistency = null
	) {
		return $this->get(
			$key,
			null,
			$startColumn,
			$endColumn,
			false,
			100,
			$superColumn,
			$consistency
		);
	}
	
	public function get(
		$key,
		$columns = null,
		$startColumn = null,
		$endColumn = null,
		$columnsReversed = false,
		$columnCount = 100,
		$superColumn = null,
		$consistency = null
	) {
		if ($columns !== null && $startColumn !== null) {
			throw new CassandraInvalidRequestException(
				'You can define either a list of columns or the start and end '.
				'columns for a range but not both at the same time'
			);
		}
		
		if ($consistency === null) {
			$consistency = $this->defaultReadConsistency;
		}
		
		$columnParent = $this->createColumnParent($superColumn);

		$slicePredicate = $this->createSlicePredicate(
			$columns,
			$startColumn,
			$endColumn,
			$columnsReversed,
			$columnCount
		);
		
		$result = $this->connection->call(
			'get_slice',
			$key,
			$columnParent,
			$slicePredicate,
			$consistency
		);
				
		if (count($result) == 0) {
			return null;
		}
		
		return $this->parseSliceResponse($result);
	}
	
	public function getWhere(
		array $where,
		$columns = null,
		$startColumn = null,
		$endColumn = null,
		$columnsReversed = false,
		$columnCount = 100,
		$superColumn = null,
		$consistency = null
	) {
		if ($columns !== null && $startColumn !== null) {
			throw new CassandraInvalidRequestException(
				'You can define either a list of columns or the start and end '.
				'columns for a range but not both at the same time'
			);
		}
		
		if ($consistency === null) {
			$consistency = $this->defaultReadConsistency;
		}
		
		$columnParent = $this->createColumnParent($superColumn);

		$slicePredicate = $this->createSlicePredicate(
			$columns,
			$startColumn,
			$endColumn,
			$columnsReversed,
			$columnCount
		);
		
		
		$indexClause = $this->createIndexClause(
			$where,
			$startColumn,
			$columnCount
		);

		/*
		$result = $this->connection->call(
			'get_indexed_slices',
			$columnParent,
			$indexClause,
			$slicePredicate,
			$consistency
		);
		*/

		$result = $this->connection->getClient()->get_indexed_slices(
			$columnParent,
			$indexClause,
			$slicePredicate,
			$consistency
		);
		
		if (count($result) == 0) {
			return array();
		}
		
		return $this->parseSlicesResponse($result);
	}
	
	public function set(
		$key,
		array $columns,
		$consistency = null,
		$timestamp = null,
		$timeToLiveSeconds = null
	) {
		if ($timestamp === null) {
			$timestamp = CassandraUtil::getTimestamp();
		}
		
		if ($consistency === null) {
			$consistency = $this->defaultWriteConsistency;
		}
		
		$mutationMap = array(
			$key => array(
				$this->name => $this->createColumnMutations(
					$columns,
					$timestamp,
					$timeToLiveSeconds
				)
			)
		);
		
		$this->connection->call('batch_mutate', $mutationMap, $consistency);
	}
	
	public function createColumnParent($superColumnName = null) {
		$schema = $this->getSchema();
		
		$columnParent = new cassandra_ColumnParent();
		$columnParent->column_family = $this->name;
		
		if ($superColumnName !== null) {
			if ($this->autopack) {
				$columnParent->super_column = CassandraUtil::pack(
					$superColumnName,
					$schema['super-type']
				);
			} else {
				$columnParent->super_column = $superColumnName;
			}
		}
		
		return $columnParent;
	}
	
	public function createSlicePredicate(
		$columns,
		$startColumn,
		$endColumn,
		$columnsReversed,
		$columnCount
	) {
		$predicate = new cassandra_SlicePredicate();
		$schema = $this->getSchema();
		
		if (is_array($columns)) {
			if ($this->autopack) {
				$packedColumns = array();

				foreach ($columns as $columnName) {
					$columnType = $this->getColumnNameType();
					
					$packedColumns[] = CassandraUtil::pack(
						$columnName,
						$columnType
					);
				}
				
				$predicate->column_names = $packedColumns;
			} else {
				$predicate->column_names = $columns;
			}
		} else {
			if ($this->autopack) {
				if ($startColumn === null) {
					$startColumn = '';
				}
				
				if ($endColumn === null) {
					$endColumn = '';
				}
				
				if (!empty($startColumn)) {
					$columnType = $this->getColumnNameType();
					
					$startColumn = CassandraUtil::pack(
						$startColumn,
						$columnType
					);
				}
				
				if (!empty($endColumn)) {
					$columnType = $this->getColumnNameType();
					
					$endColumn = CassandraUtil::pack(
						$endColumn,
						$columnType
					);
				}
			}
			
			$sliceRange = new cassandra_SliceRange();
			$sliceRange->count = $columnCount;
			$sliceRange->reversed = $columnsReversed;
			$sliceRange->start = $startColumn;
			$sliceRange->finish = $endColumn;
			
			$predicate->slice_range = $sliceRange;
		}
		
		return $predicate;
	}
	
	public function createIndexClause(
		array $where,
		$startKey = null,
		$columnCount = 100
	) {
		$indexClause = new cassandra_IndexClause();
		$expressions = array();
		
		foreach ($where as $columnName => $value) {
			$indexExpression = new cassandra_IndexExpression();
			
			if (is_array($value)) {
				$supportedOperators = array(
					Cassandra::OP_EQ,
					Cassandra::OP_LT,
					Cassandra::OP_GT,
					Cassandra::OP_LTE,
					Cassandra::OP_GTE
				);
				
				if (
					count($value) != 3
					|| !in_array($value[1], $supportedOperators)
				) {
					throw new CassandraInvalidRequestException(
						'Invalid where clause: '.serialize($value)
					);
				}
				
				$indexExpression->column_name = CassandraUtil::pack(
					$value[0],
					$this->getColumnNameType()
				);
				
				$indexExpression->op = $value[1];
				
				$indexExpression->value = CassandraUtil::pack(
					$value[2],
					$this->getColumnValueType($value[0])
				);
			} else {
				$indexExpression->column_name = CassandraUtil::pack(
					$columnName,
					$this->getColumnNameType()
				);
				
				$indexExpression->op = Cassandra::OP_EQ;
				
				$indexExpression->value = CassandraUtil::pack(
					$value,
					$this->getColumnValueType($columnName)
				);
			}
			
			$expressions[] = $indexExpression;
		}
		
		$indexClause->expressions = $expressions;
        $indexClause->start_key = $startKey !== null ? $startKey : '';
        $indexClause->count = $columnCount;
		
		return $indexClause;
	}
	
	public function createColumnMutations(
		array $columns,
		$timestamp = null,
		$timeToLiveSeconds = null
	) {
		if ($timestamp === null) {
			$timestamp = CassandraUtil::getTimestamp();
		}
		
		$columnsOrSuperColumns = $this->createColumnsOrSuperColumns(
			$columns,
			$timestamp,
			$timeToLiveSeconds
		);
		
		$mutations = array();
		
		foreach ($columnsOrSuperColumns as $columnOrSuperColumn) {
			$mutation = new cassandra_Mutation();
			$mutation->column_or_supercolumn = $columnOrSuperColumn;
			
			$mutations[] = $mutation;
		}
		
		return $mutations;
	}
	
	public function createColumnsOrSuperColumns(
		array $columns,
		$timestamp = null,
		$timeToLiveSeconds = null
	) {
		if ($timestamp === null) {
			$timestamp = CassandraUtil::getTimestamp();
		}
		
		$results = array();
		
		foreach ($columns as $columnName => $columnValue) {
			$column = new cassandra_ColumnOrSuperColumn();
			
			if (is_array($columnValue)) {
				$column->super_column = new cassandra_SuperColumn();
				$column->super_column->name = CassandraUtil::pack(
					$columnName,
					$this->getColumnNameType()
				);
				$column->super_column->columns = $this->createColumns(
					$columnValue,
					$timestamp,
					$timeToLiveSeconds
				);
				$column->super_column->timestamp = $timestamp;
			} else {
				$column->column = new cassandra_Column();
				$column->column->name = CassandraUtil::pack(
					$columnName,
					$this->getColumnNameType()
				);
				$column->column->value = CassandraUtil::pack(
					$columnValue,
					$this->getColumnValueType($columnName)
				);
				$column->column->timestamp = $timestamp;
				$column->column->ttl = $timeToLiveSeconds;
			}
			
			$results[] = $column;
		}
		
		return $results;
	}
	
	public function createColumns(
		array $columns,
		$timestamp = null,
		$timeToLiveSeconds = null
	) {
		if ($timestamp === null) {
			$timestamp = CassandraUtil::getTimestamp();
		}
		
		$results = array();
		
		foreach ($columns as $name => $value) {
			$column = new cassandra_Column();
			$column->name = CassandraUtil::pack(
				$name,
				$this->getColumnNameType()
			);
			$column->value = CassandraUtil::pack(
				$value,
				$this->getColumnValueType($name)
			);
			$column->timestamp = $timestamp;
			$column->ttl = $timeToLiveSeconds;
			
			$results[] = $column;
		}
		
		return $results;
	}

	protected function parseSliceResponse(array $response) {
		$results = array();
		
		foreach ($response as $row) {
			$results = array_merge(
				$results,
				$this->parseSliceRow($row)
			);
		}
		
		return $results;
	}
	
	protected function parseSlicesResponse(array $response) {
		$results = array();
		
		foreach ($response as $response) {
			$key = $response->key;
			$results[$key] = array();
			
			foreach ($response->columns as $row) {
				$results[$key] = array_merge(
					$results[$key],
					$this->parseSliceRow($row)
				);
			}
		}
		
		return $results;
	}
	
	protected function parseSliceRow(cassandra_ColumnOrSuperColumn $row) {
		$result = array();
		
		if ($row->column !== null) {
			$nameType = $this->getColumnNameType();
			$valueType = $this->getColumnValueType($row->column->name);

			$name = CassandraUtil::unpack($row->column->name, $nameType);
			$value = CassandraUtil::unpack($row->column->value, $valueType);

			$result[$name] = $value;
		} else if($row->super_column !== null) {
			$superNameType = null;

			$superName = CassandraUtil::unpack(
				$row->super_column->name,
				$superNameType
			);

			$values = array();

			foreach ($row->super_column->columns as $column) {
				$nameType = $this->getColumnNameType();
				$valueType = $this->getColumnValueType($column->name);

				$name = CassandraUtil::unpack($column->name, $nameType);
				$value = CassandraUtil::unpack($column->value, $valueType);

				$values[$name] = $value;
			}

			$result[$superName] = $values;
		} else {
			// @codeCoverageIgnoreStart
			throw new Exception('Expected either normal or super column');
			// @codeCoverageIgnoreEnd
		}
		
		return $result;
	}
}

class CassandraUtil {
	
	public static function extractType($definition) {
		if ($definition === null or $definition == '') {
			return Cassandra::TYPE_BYTES;
		}

		$index = strrpos($definition, '.');

		if ($index === false) {
			return Cassandra::TYPE_BYTES;
		}

		return substr($definition, $index + 1);
	}
	
	public static function pack($value, $type) {
		switch ($type) {
			case Cassandra::TYPE_LONG:
				return self::packLong($value);
			
			case Cassandra::TYPE_INTEGER:
				return self::packInteger($value);
			
			case Cassandra::TYPE_ASCII:
				return self::packString($value, strlen($value));
			
			case Cassandra::TYPE_UTF8:
				if (mb_detect_encoding($value, 'UTF-8') != 'UTF-8') {
					$value = utf8_encode($value);
				}
				
				return self::packString($value, strlen($value));
				
			case Cassandra::TYPE_TIME_UUID:
				return self::packString($value, 16);
			
			case Cassandra::TYPE_LEXICAL_UUID:
				return self::packString($value, 16);
				
			default:
				return $value;
		}
	}
	
	public static function packLong($value) {
        // If we are on a 32bit architecture we have to explicitly deal with
		// 64-bit twos-complement arithmetic since PHP wants to treat all ints
		// as signed and any int over 2^31 - 1 as a float
		if (PHP_INT_SIZE == 4) {
			$neg = $value < 0;

			if ($neg) {
				$value *= - 1;
			}

			$hi = (int) ($value / 4294967296);
			$lo = (int) $value;

			if ($neg) {
				$hi = ~$hi;
				$lo = ~$lo;
				
				if (($lo & (int)0xffffffff) == (int)0xffffffff) {
					$lo = 0;
					$hi++;
				} else {
					$lo++;
				}
			}
			
			$data = pack('N2', $hi, $lo);
		} else {
			$hi = $value >> 32;
			$lo = $value & 0xFFFFFFFF;
			$data = pack('N2', $hi, $lo);
		}
		
		return $data;
	}
	
	public static function packInteger($value) {
		return pack('N', $value);
	}
	
	public static function packString($string, $length) {       
        $result = '';
		
        for($i = 0; $i < $length; $i++) {
            $result .= pack('c', ord(substr($string, $i, 1)));
		}
		
        return $result;
    }
	
	public static function unpack($value, $type) {
		switch ($type) {
			case Cassandra::TYPE_LONG:
				return self::unpackLong($value);
			
			case Cassandra::TYPE_INTEGER:
				return self::unpackInteger($value);
			
			case Cassandra::TYPE_ASCII:
				return self::unpackString($value, strlen($value));
			
			case Cassandra::TYPE_UTF8:
				return self::unpackString($value, strlen($value));
				
			case Cassandra::TYPE_TIME_UUID:
				return $value;
			
			case Cassandra::TYPE_LEXICAL_UUID:
				return $value;
				
			default:
				return $value;
		}
	}
	
	public static function unpackLong($data) {
		$arr = unpack('N2', $data);

		// If we are on a 32bit architecture we have to explicitly deal with
		// 64-bit twos-complement arithmetic since PHP wants to treat all ints
		// as signed and any int over 2^31 - 1 as a float
		if (PHP_INT_SIZE == 4) {

			$hi = $arr[1];
			$lo = $arr[2];
			$isNeg = $hi < 0;

			// Check for a negative
			if ($isNeg) {
				$hi = ~$hi & (int)0xffffffff;
				$lo = ~$lo & (int)0xffffffff;

				if ($lo == (int)0xffffffff) {
					$hi++;
					$lo = 0;
				} else {
					$lo++;
				}
			}

			// Force 32bit words in excess of 2G to pe positive - we deal wigh sign
			// explicitly below

			if ($hi & (int)0x80000000) {
				$hi &= (int)0x7fffffff;
				$hi += 0x80000000;
			}

			if ($lo & (int)0x80000000) {
				$lo &= (int)0x7fffffff;
				$lo += 0x80000000;
			}

			$value = $hi * 4294967296 + $lo;

			if ($isNeg) {
				$value = 0 - $value;
			}
		} else {
			// Upcast negatives in LSB bit
			if ($arr[2] & 0x80000000)
				$arr[2] = $arr[2] & 0xffffffff;

			// Check for a negative
			if ($arr[1] & 0x80000000) {
				$arr[1] = $arr[1] & 0xffffffff;
				$arr[1] = $arr[1] ^ 0xffffffff;
				$arr[2] = $arr[2] ^ 0xffffffff;
				
				$value = 0 - $arr[1] * 4294967296 - $arr[2] - 1;
			} else {
				$value = $arr[1] * 4294967296 + $arr[2];
			}
		}
		return 
		$value;
	}
	
	public static function unpackInteger($value) {
		return unpack('N', $value);
	}
	
	public static function unpackString($value, $length) {
		$unpacked = unpack('c'.$length.'chars', $value);
        $out = '';
		
        foreach($unpacked as $element) {
            if($element > 0) {
				$out .= chr($element);
			}
		}
		
        return $out;
	}
	
	public function getTimestamp() {
		// By Zach Buller (zachbuller@gmail.com)
        $time1 = microtime();
        settype($time1, 'string'); //convert to string to keep trailing zeroes
        $time2 = explode(" ", $time1);
        $sub_secs = preg_replace('/0./', '', $time2[0], 1);
        $time3 = ($time2[1].$sub_secs) / 100;
		
        return $time3;
	}
}

class CassandraMaxRetriesException extends Exception {};
class CassandraConnectionClosedException extends Exception {};
class CassandraConnectionFailedException extends Exception {};
class CassandraSettingKeyspaceFailedException extends Exception {};
class CassandraColumnFamilyNotFoundException extends Exception {};
class CassandraInvalidRequestException extends Exception {};
class CassandraInvalidPatternException extends Exception {};